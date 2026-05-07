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

	// MaxInvocationIDLen is the max byte length of a SignalG invocation id.
	MaxInvocationIDLen = 255

	// DefaultMaxPayloadSize is the default max body size for one SignalG message.
	DefaultMaxPayloadSize int64 = 1 << 20

	protocolMagic   byte = 0x5
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
	ErrInvalidInvocationID  = errors.New("signalg: invalid invocation id")
	ErrInvalidFrameKind     = errors.New("signalg: invalid frame kind")
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

// FrameKind identifies the semantic kind of a SignalG protocol frame.
type FrameKind uint8

const (
	// FrameKindMessage is a fire-and-forget message.
	FrameKindMessage FrameKind = iota
	// FrameKindInvoke is a client-to-server invocation that expects a completion.
	FrameKindInvoke
	// FrameKindCompletion is a successful invocation result.
	FrameKindCompletion
	// FrameKindError is a failed invocation result.
	FrameKindError
)

func (k FrameKind) String() string {
	switch k {
	case FrameKindMessage:
		return "message"
	case FrameKindInvoke:
		return "invoke"
	case FrameKindCompletion:
		return "completion"
	case FrameKindError:
		return "error"
	default:
		return fmt.Sprintf("unknown(%d)", k)
	}
}

// FrameHeader is the fixed SignalG protocol frame header.
type FrameHeader struct {
	Magic           byte
	Version         uint8
	Codec           Serialization
	Kind            FrameKind
	MethodLen       uint8
	InvocationIDLen uint8
	BodyLen         uint32
}

// Message is a validated SignalG protocol frame.
type Message struct {
	Header       FrameHeader
	Kind         FrameKind
	Method       string
	InvocationID string
	Payload      []byte

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
	dst[0] = protocolMagic<<4 | (protocolVersion & 0x0f)
	dst[1] = byte(header.Codec)<<4 | (byte(header.Kind) & 0x0f)
	dst[2] = header.MethodLen
	dst[3] = header.InvocationIDLen
	binary.BigEndian.PutUint32(dst[4:8], header.BodyLen)
}

func decodeFrame(frame []byte, codec BodyCodec, maxPayloadSize int64) (Message, error) {
	maxPayloadSize = normalizeMaxPayloadSize(maxPayloadSize)
	if len(frame) < HeaderSize {
		return Message{}, fmt.Errorf("%w: frame shorter than header", ErrInvalidFrame)
	}
	magic := frame[0] >> 4
	version := frame[0] & 0x0f
	if magic != protocolMagic {
		return Message{}, fmt.Errorf("%w: bad magic", ErrInvalidFrame)
	}
	if version != protocolVersion {
		return Message{}, fmt.Errorf("%w: unsupported version %d", ErrInvalidFrame, version)
	}
	if codec == nil {
		return Message{}, ErrUnsupportedCodec
	}

	header := FrameHeader{
		Magic:           magic,
		Version:         version,
		Codec:           Serialization(frame[1] >> 4),
		Kind:            FrameKind(frame[1] & 0x0f),
		MethodLen:       frame[2],
		InvocationIDLen: frame[3],
		BodyLen:         binary.BigEndian.Uint32(frame[4:8]),
	}
	if header.Codec != codec.Serialization() {
		return Message{}, fmt.Errorf("%w: got %s, want %s", ErrUnexpectedCodec, header.Codec, codec.Serialization())
	}
	if err := validateFrameKind(header.Kind); err != nil {
		return Message{}, err
	}
	if (header.Kind == FrameKindMessage || header.Kind == FrameKindInvoke) && header.MethodLen == 0 {
		return Message{}, fmt.Errorf("%w: method name is empty", ErrInvalidMethodName)
	}
	if header.Kind != FrameKindMessage && header.InvocationIDLen == 0 {
		return Message{}, fmt.Errorf("%w: invocation id is empty", ErrInvalidInvocationID)
	}
	if int64(header.BodyLen) > maxPayloadSize {
		return Message{}, fmt.Errorf("%w: %d > %d", ErrPayloadTooLarge, header.BodyLen, maxPayloadSize)
	}

	wantLen := HeaderSize + int(header.MethodLen) + int(header.InvocationIDLen) + int(header.BodyLen)
	if len(frame) != wantLen {
		return Message{}, fmt.Errorf("%w: length mismatch got %d want %d", ErrInvalidFrame, len(frame), wantLen)
	}

	methodStart := HeaderSize
	methodEnd := methodStart + int(header.MethodLen)
	methodBytes := frame[methodStart:methodEnd]
	if !utf8.Valid(methodBytes) {
		return Message{}, fmt.Errorf("%w: method name must be utf-8", ErrInvalidMethodName)
	}

	invocationIDStart := methodEnd
	invocationIDEnd := invocationIDStart + int(header.InvocationIDLen)
	invocationIDBytes := frame[invocationIDStart:invocationIDEnd]
	if !utf8.Valid(invocationIDBytes) {
		return Message{}, fmt.Errorf("%w: invocation id must be utf-8", ErrInvalidInvocationID)
	}

	payload := make([]byte, int(header.BodyLen))
	copy(payload, frame[invocationIDEnd:])

	return Message{
		Header:       header,
		Kind:         header.Kind,
		Method:       string(methodBytes),
		InvocationID: string(invocationIDBytes),
		Payload:      payload,
		codec:        codec,
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

func validateInvocationID(invocationID string) error {
	if invocationID == "" {
		return fmt.Errorf("%w: invocation id is empty", ErrInvalidInvocationID)
	}
	if len(invocationID) > MaxInvocationIDLen {
		return fmt.Errorf("%w: invocation id length %d > %d", ErrInvalidInvocationID, len(invocationID), MaxInvocationIDLen)
	}
	if !utf8.ValidString(invocationID) {
		return fmt.Errorf("%w: invocation id must be utf-8", ErrInvalidInvocationID)
	}
	return nil
}

func validateFrameKind(kind FrameKind) error {
	switch kind {
	case FrameKindMessage, FrameKindInvoke, FrameKindCompletion, FrameKindError:
		return nil
	default:
		return fmt.Errorf("%w: %s", ErrInvalidFrameKind, kind)
	}
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
