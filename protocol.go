package signalg

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"unicode/utf8"

	"github.com/vmihailenco/msgpack/v5"
	"google.golang.org/protobuf/proto"
)

const (
	// HeaderSize is the byte length of one SignalG protocol frame header.
	HeaderSize = 8

	// MaxMethodNameLen is the max byte length of a SignalG method name.
	MaxMethodNameLen = 255

	// DefaultMaxPayloadSize is the default max body size for one SignalG message.
	DefaultMaxPayloadSize int64 = 1 << 20

	protocolMagic   byte = 'S'
	protocolVersion byte = 1
)

var (
	ErrInvalidFrame         = errors.New("signalg: invalid protocol frame")
	ErrUnsupportedCodec     = errors.New("signalg: unsupported serialization codec")
	ErrUnexpectedCodec      = errors.New("signalg: unexpected serialization codec")
	ErrPayloadTooLarge      = errors.New("signalg: protocol payload too large")
	ErrInvalidMessageType   = errors.New("signalg: websocket message must be binary")
	ErrUnsupportedBodyValue = errors.New("signalg: unsupported body value")
	ErrInvalidMethodName    = errors.New("signalg: invalid method name")
)

// Serialization identifies the body codec used by SignalG protocol frames.
type Serialization uint8

const (
	// SerializationMessagePack uses MessagePack for the frame body. It is the default.
	SerializationMessagePack Serialization = iota
	// SerializationProtobuf uses protobuf wire format for the frame body.
	SerializationProtobuf
	// SerializationJSON uses JSON for the frame body.
	SerializationJSON
)

func (s Serialization) String() string {
	switch s {
	case SerializationMessagePack:
		return "messagepack"
	case SerializationProtobuf:
		return "protobuf"
	case SerializationJSON:
		return "json"
	default:
		return fmt.Sprintf("unknown(%d)", s)
	}
}

// FrameHeader is the fixed SignalG protocol frame header.
type FrameHeader struct {
	Magic     byte
	Version   uint8
	Codec     Serialization
	MethodLen uint8
	BodyLen   uint32
}

// Message is a validated SignalG protocol frame.
type Message struct {
	Header  FrameHeader
	Method  string
	Payload []byte

	codec BodyCodec
}

// Decode decodes the message payload into dst with the connection codec.
func (m Message) Decode(dst any) error {
	if m.codec == nil {
		return ErrUnsupportedCodec
	}
	return m.codec.Unmarshal(m.Payload, dst)
}

// BodyCodec marshals and unmarshals protocol frame bodies.
type BodyCodec interface {
	Serialization() Serialization
	MarshalAppend(dst []byte, v any) ([]byte, error)
	Unmarshal(data []byte, v any) error
}

type protocolConfig struct {
	codec          BodyCodec
	maxPayloadSize int64
}

func newProtocolConfig(serialization Serialization, maxPayloadSize int64) (*protocolConfig, error) {
	codec, err := newBodyCodec(serialization)
	if err != nil {
		return nil, err
	}
	return &protocolConfig{
		codec:          codec,
		maxPayloadSize: normalizeMaxPayloadSize(maxPayloadSize),
	}, nil
}

func (p *protocolConfig) serialization() Serialization {
	if p == nil || p.codec == nil {
		return SerializationMessagePack
	}
	return p.codec.Serialization()
}

func (p *protocolConfig) marshalAppend(dst []byte, v any) ([]byte, error) {
	if p == nil || p.codec == nil {
		return dst, ErrUnsupportedCodec
	}
	return p.codec.MarshalAppend(dst, v)
}

func (p *protocolConfig) decodeFrame(frame []byte) (Message, error) {
	if p == nil {
		return Message{}, ErrUnsupportedCodec
	}
	return decodeFrame(frame, p.codec, p.maxPayloadSize)
}

func (p *protocolConfig) ensurePayloadSize(n int) error {
	if p == nil {
		return ErrUnsupportedCodec
	}
	return ensurePayloadSize(n, p.maxPayloadSize)
}

func newBodyCodec(s Serialization) (BodyCodec, error) {
	switch s {
	case SerializationMessagePack:
		return messagePackCodec{}, nil
	case SerializationProtobuf:
		return protobufCodec{}, nil
	case SerializationJSON:
		return jsonCodec{}, nil
	default:
		return nil, fmt.Errorf("%w: %s", ErrUnsupportedCodec, s)
	}
}

func normalizeMaxPayloadSize(n int64) int64 {
	if n <= 0 {
		return DefaultMaxPayloadSize
	}
	return n
}

func encodeFrameHeader(dst []byte, header FrameHeader) {
	dst[0] = protocolMagic
	dst[1] = protocolVersion
	dst[2] = byte(header.Codec)
	dst[3] = header.MethodLen
	binary.BigEndian.PutUint32(dst[4:8], header.BodyLen)
}

func decodeFrame(frame []byte, codec BodyCodec, maxPayloadSize int64) (Message, error) {
	maxPayloadSize = normalizeMaxPayloadSize(maxPayloadSize)
	if len(frame) < HeaderSize {
		return Message{}, fmt.Errorf("%w: frame shorter than header", ErrInvalidFrame)
	}
	if frame[0] != protocolMagic {
		return Message{}, fmt.Errorf("%w: bad magic", ErrInvalidFrame)
	}
	if frame[1] != protocolVersion {
		return Message{}, fmt.Errorf("%w: unsupported version %d", ErrInvalidFrame, frame[1])
	}
	if codec == nil {
		return Message{}, ErrUnsupportedCodec
	}

	header := FrameHeader{
		Magic:     frame[0],
		Version:   frame[1],
		Codec:     Serialization(frame[2]),
		MethodLen: frame[3],
		BodyLen:   binary.BigEndian.Uint32(frame[4:8]),
	}
	if header.Codec != codec.Serialization() {
		return Message{}, fmt.Errorf("%w: got %s, want %s", ErrUnexpectedCodec, header.Codec, codec.Serialization())
	}
	if header.MethodLen == 0 {
		return Message{}, fmt.Errorf("%w: method name is empty", ErrInvalidMethodName)
	}
	if int64(header.BodyLen) > maxPayloadSize {
		return Message{}, fmt.Errorf("%w: %d > %d", ErrPayloadTooLarge, header.BodyLen, maxPayloadSize)
	}
	wantLen := HeaderSize + int(header.MethodLen) + int(header.BodyLen)
	if len(frame) != wantLen {
		return Message{}, fmt.Errorf("%w: length mismatch got %d want %d", ErrInvalidFrame, len(frame), wantLen)
	}
	methodStart := HeaderSize
	methodEnd := methodStart + int(header.MethodLen)
	methodBytes := frame[methodStart:methodEnd]
	if !utf8.Valid(methodBytes) {
		return Message{}, fmt.Errorf("%w: method name must be utf-8", ErrInvalidMethodName)
	}

	return Message{
		Header:  header,
		Method:  string(methodBytes),
		Payload: frame[methodEnd:],
		codec:   codec,
	}, nil
}

func validateMethodName(method string) error {
	if method == "" {
		return fmt.Errorf("%w: method name is empty", ErrInvalidMethodName)
	}
	if len(method) > MaxMethodNameLen {
		return fmt.Errorf("%w: method name length %d > %d", ErrInvalidMethodName, len(method), MaxMethodNameLen)
	}
	if !utf8.ValidString(method) {
		return fmt.Errorf("%w: method name must be utf-8", ErrInvalidMethodName)
	}
	return nil
}

type messagePackCodec struct{}

func (messagePackCodec) Serialization() Serialization {
	return SerializationMessagePack
}

func (messagePackCodec) MarshalAppend(dst []byte, v any) ([]byte, error) {
	if v == nil {
		return dst, nil
	}
	body, err := msgpack.Marshal(v)
	if err != nil {
		return dst, err
	}
	return append(dst, body...), nil
}

func (messagePackCodec) Unmarshal(data []byte, v any) error {
	if len(data) == 0 {
		return nil
	}
	return msgpack.Unmarshal(data, v)
}

type protobufCodec struct{}

func (protobufCodec) Serialization() Serialization {
	return SerializationProtobuf
}

func (protobufCodec) MarshalAppend(dst []byte, v any) ([]byte, error) {
	if v == nil {
		return dst, nil
	}
	msg, ok := v.(proto.Message)
	if !ok {
		return dst, fmt.Errorf("%w: protobuf body must implement proto.Message", ErrUnsupportedBodyValue)
	}
	return proto.MarshalOptions{}.MarshalAppend(dst, msg)
}

func (protobufCodec) Unmarshal(data []byte, v any) error {
	msg, ok := v.(proto.Message)
	if !ok {
		return fmt.Errorf("%w: protobuf target must implement proto.Message", ErrUnsupportedBodyValue)
	}
	if len(data) == 0 {
		return nil
	}
	return proto.UnmarshalOptions{}.Unmarshal(data, msg)
}

type jsonCodec struct{}

func (jsonCodec) Serialization() Serialization {
	return SerializationJSON
}

func (jsonCodec) MarshalAppend(dst []byte, v any) ([]byte, error) {
	if v == nil {
		return dst, nil
	}
	body, err := json.Marshal(v)
	if err != nil {
		return dst, err
	}
	return append(dst, body...), nil
}

func (jsonCodec) Unmarshal(data []byte, v any) error {
	if len(data) == 0 {
		return nil
	}
	return json.Unmarshal(data, v)
}

func ensurePayloadSize(n int, maxPayloadSize int64) error {
	if n < 0 || int64(n) > maxPayloadSize || uint64(n) > math.MaxUint32 {
		return fmt.Errorf("%w: %d > %d", ErrPayloadTooLarge, n, maxPayloadSize)
	}
	return nil
}
