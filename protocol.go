package signalg

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"math"

	"github.com/vmihailenco/msgpack/v5"
	"google.golang.org/protobuf/proto"
)

const (
	// HeaderSize is the byte length of one SignalG protocol frame header.
	HeaderSize = 16

	// DefaultMaxPayloadSize is the default max body size for one SignalG message.
	DefaultMaxPayloadSize int64 = 1 << 20

	protocolMagic0  byte = 'S'
	protocolMagic1  byte = 'G'
	protocolVersion byte = 1
)

var (
	ErrInvalidFrame         = errors.New("signalg: invalid protocol frame")
	ErrUnsupportedCodec     = errors.New("signalg: unsupported serialization codec")
	ErrUnexpectedCodec      = errors.New("signalg: unexpected serialization codec")
	ErrPayloadTooLarge      = errors.New("signalg: protocol payload too large")
	ErrInvalidMessageType   = errors.New("signalg: websocket message must be binary")
	ErrUnsupportedBodyValue = errors.New("signalg: unsupported body value")
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

// MessageType is an application-defined message kind carried in the fixed header.
type MessageType uint16

// FrameHeader is the fixed SignalG protocol frame header.
type FrameHeader struct {
	Version     uint8
	Codec       Serialization
	MessageType MessageType
	Flags       uint16
	BodyLen     uint32
	Reserved    uint32
}

// Message is a validated SignalG protocol frame.
type Message struct {
	Header  FrameHeader
	Type    MessageType
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
	dst[0] = protocolMagic0
	dst[1] = protocolMagic1
	dst[2] = protocolVersion
	dst[3] = byte(header.Codec)
	binary.BigEndian.PutUint16(dst[4:6], uint16(header.MessageType))
	binary.BigEndian.PutUint16(dst[6:8], header.Flags)
	binary.BigEndian.PutUint32(dst[8:12], header.BodyLen)
	binary.BigEndian.PutUint32(dst[12:16], header.Reserved)
}

func decodeFrame(frame []byte, codec BodyCodec, maxPayloadSize int64) (Message, error) {
	maxPayloadSize = normalizeMaxPayloadSize(maxPayloadSize)
	if len(frame) < HeaderSize {
		return Message{}, fmt.Errorf("%w: frame shorter than header", ErrInvalidFrame)
	}
	if frame[0] != protocolMagic0 || frame[1] != protocolMagic1 {
		return Message{}, fmt.Errorf("%w: bad magic", ErrInvalidFrame)
	}
	if frame[2] != protocolVersion {
		return Message{}, fmt.Errorf("%w: unsupported version %d", ErrInvalidFrame, frame[2])
	}
	if codec == nil {
		return Message{}, ErrUnsupportedCodec
	}

	header := FrameHeader{
		Version:     frame[2],
		Codec:       Serialization(frame[3]),
		MessageType: MessageType(binary.BigEndian.Uint16(frame[4:6])),
		Flags:       binary.BigEndian.Uint16(frame[6:8]),
		BodyLen:     binary.BigEndian.Uint32(frame[8:12]),
		Reserved:    binary.BigEndian.Uint32(frame[12:16]),
	}
	if header.Codec != codec.Serialization() {
		return Message{}, fmt.Errorf("%w: got %s, want %s", ErrUnexpectedCodec, header.Codec, codec.Serialization())
	}
	if header.Flags != 0 {
		return Message{}, fmt.Errorf("%w: flags must be zero", ErrInvalidFrame)
	}
	if header.Reserved != 0 {
		return Message{}, fmt.Errorf("%w: reserved must be zero", ErrInvalidFrame)
	}
	if int64(header.BodyLen) > maxPayloadSize {
		return Message{}, fmt.Errorf("%w: %d > %d", ErrPayloadTooLarge, header.BodyLen, maxPayloadSize)
	}
	wantLen := HeaderSize + int(header.BodyLen)
	if len(frame) != wantLen {
		return Message{}, fmt.Errorf("%w: length mismatch got %d want %d", ErrInvalidFrame, len(frame), wantLen)
	}

	return Message{
		Header:  header,
		Type:    header.MessageType,
		Payload: frame[HeaderSize:],
		codec:   codec,
	}, nil
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
