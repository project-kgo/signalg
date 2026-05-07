package signalg

import (
	"encoding/binary"
	"errors"
	"strings"
	"testing"

	"google.golang.org/protobuf/types/known/wrapperspb"
)

type protocolTestBody struct {
	Name string `json:"name" msgpack:"name"`
	Seq  int    `json:"seq" msgpack:"seq"`
}

func TestProtocolFrameRoundTrip(t *testing.T) {
	t.Run("messagepack default", func(t *testing.T) {
		codec := mustCodec(t, SerializationMessagePack)
		frame := encodeTestFrame(t, codec, "chat.send", protocolTestBody{Name: "msgpack", Seq: 1})

		msg, err := decodeFrame(frame, codec, 0)
		if err != nil {
			t.Fatalf("decodeFrame returned error: %v", err)
		}
		if msg.Method != "chat.send" {
			t.Fatalf("expected method chat.send, got %q", msg.Method)
		}
		if msg.Kind != FrameKindMessage {
			t.Fatalf("expected message frame, got %s", msg.Kind)
		}
		if msg.InvocationID != "" {
			t.Fatalf("expected empty invocation id, got %q", msg.InvocationID)
		}
		var got protocolTestBody
		if err = msg.Decode(&got); err != nil {
			t.Fatalf("Decode returned error: %v", err)
		}
		if got.Name != "msgpack" || got.Seq != 1 {
			t.Fatalf("unexpected decoded body: %#v", got)
		}
	})

	t.Run("protobuf", func(t *testing.T) {
		codec := mustCodec(t, SerializationProtobuf)
		frame := encodeTestFrame(t, codec, "user.rename", wrapperspb.String("protobuf"))

		msg, err := decodeFrame(frame, codec, DefaultMaxPayloadSize)
		if err != nil {
			t.Fatalf("decodeFrame returned error: %v", err)
		}
		if msg.Method != "user.rename" {
			t.Fatalf("expected method user.rename, got %q", msg.Method)
		}
		var got wrapperspb.StringValue
		if err = msg.Decode(&got); err != nil {
			t.Fatalf("Decode returned error: %v", err)
		}
		if got.Value != "protobuf" {
			t.Fatalf("expected protobuf, got %q", got.Value)
		}
	})

	t.Run("json", func(t *testing.T) {
		codec := mustCodec(t, SerializationJSON)
		frame := encodeTestFrame(t, codec, "json.echo", protocolTestBody{Name: "json", Seq: 2})

		msg, err := decodeFrame(frame, codec, DefaultMaxPayloadSize)
		if err != nil {
			t.Fatalf("decodeFrame returned error: %v", err)
		}
		if msg.Method != "json.echo" {
			t.Fatalf("expected method json.echo, got %q", msg.Method)
		}
		var got protocolTestBody
		if err = msg.Decode(&got); err != nil {
			t.Fatalf("Decode returned error: %v", err)
		}
		if got.Name != "json" || got.Seq != 2 {
			t.Fatalf("unexpected decoded body: %#v", got)
		}
	})
}

func TestInvokeProtocolFrameRoundTrip(t *testing.T) {
	t.Run("invoke", func(t *testing.T) {
		codec := mustCodec(t, SerializationMessagePack)
		frame := encodeInvokeTestFrame(t, codec, FrameKindInvoke, "Echo", "inv-1", protocolTestBody{Name: "invoke", Seq: 5})

		msg, err := decodeFrame(frame, codec, DefaultMaxPayloadSize)
		if err != nil {
			t.Fatalf("decodeFrame returned error: %v", err)
		}
		if msg.Kind != FrameKindInvoke {
			t.Fatalf("expected invoke frame, got %s", msg.Kind)
		}
		if msg.Method != "Echo" {
			t.Fatalf("expected method Echo, got %q", msg.Method)
		}
		if msg.InvocationID != "inv-1" {
			t.Fatalf("expected invocation id inv-1, got %q", msg.InvocationID)
		}
		var got protocolTestBody
		if err = msg.Decode(&got); err != nil {
			t.Fatalf("Decode returned error: %v", err)
		}
		if got.Name != "invoke" || got.Seq != 5 {
			t.Fatalf("unexpected decoded body: %#v", got)
		}
	})

	t.Run("completion", func(t *testing.T) {
		codec := mustCodec(t, SerializationJSON)
		frame := encodeInvokeTestFrame(t, codec, FrameKindCompletion, "", "inv-2", protocolTestBody{Name: "done", Seq: 6})

		msg, err := decodeFrame(frame, codec, DefaultMaxPayloadSize)
		if err != nil {
			t.Fatalf("decodeFrame returned error: %v", err)
		}
		if msg.Kind != FrameKindCompletion {
			t.Fatalf("expected completion frame, got %s", msg.Kind)
		}
		if msg.Method != "" {
			t.Fatalf("expected empty completion method, got %q", msg.Method)
		}
		if msg.InvocationID != "inv-2" {
			t.Fatalf("expected invocation id inv-2, got %q", msg.InvocationID)
		}
		var got protocolTestBody
		if err = msg.Decode(&got); err != nil {
			t.Fatalf("Decode returned error: %v", err)
		}
		if got.Name != "done" || got.Seq != 6 {
			t.Fatalf("unexpected decoded body: %#v", got)
		}
	})

	t.Run("error", func(t *testing.T) {
		codec := mustCodec(t, SerializationMessagePack)
		frame := encodeInvokeTestFrameRaw(t, codec, FrameKindError, "", "inv-3", []byte("boom"))

		msg, err := decodeFrame(frame, codec, DefaultMaxPayloadSize)
		if err != nil {
			t.Fatalf("decodeFrame returned error: %v", err)
		}
		if msg.Kind != FrameKindError {
			t.Fatalf("expected error frame, got %s", msg.Kind)
		}
		if msg.InvocationID != "inv-3" {
			t.Fatalf("expected invocation id inv-3, got %q", msg.InvocationID)
		}
		if string(msg.Payload) != "boom" {
			t.Fatalf("expected error payload boom, got %q", msg.Payload)
		}
	})
}

func TestProtocolFrameValidation(t *testing.T) {
	codec := mustCodec(t, SerializationMessagePack)
	frame := encodeTestFrame(t, codec, "ok", protocolTestBody{Name: "ok"})

	t.Run("bad magic", func(t *testing.T) {
		bad := append([]byte(nil), frame...)
		bad[0] = 0xf0 | (bad[0] & 0x0f)
		_, err := decodeFrame(bad, codec, DefaultMaxPayloadSize)
		if !errors.Is(err, ErrInvalidFrame) {
			t.Fatalf("expected ErrInvalidFrame, got %v", err)
		}
	})

	t.Run("bad version", func(t *testing.T) {
		bad := append([]byte(nil), frame...)
		bad[0] = (bad[0] & 0xf0) | 0x0f
		_, err := decodeFrame(bad, codec, DefaultMaxPayloadSize)
		if !errors.Is(err, ErrInvalidFrame) {
			t.Fatalf("expected ErrInvalidFrame, got %v", err)
		}
	})

	t.Run("unexpected codec", func(t *testing.T) {
		bad := append([]byte(nil), frame...)
		bad[1] = byte(SerializationJSON)<<4 | (bad[1] & 0x0f)
		_, err := decodeFrame(bad, codec, DefaultMaxPayloadSize)
		if !errors.Is(err, ErrUnexpectedCodec) {
			t.Fatalf("expected ErrUnexpectedCodec, got %v", err)
		}
	})

	t.Run("empty method", func(t *testing.T) {
		bad := append([]byte(nil), frame...)
		bad[2] = 0
		_, err := decodeFrame(bad, codec, DefaultMaxPayloadSize)
		if !errors.Is(err, ErrInvalidMethodName) {
			t.Fatalf("expected ErrInvalidMethodName, got %v", err)
		}
	})

	t.Run("invalid utf8 method", func(t *testing.T) {
		bad := encodeTestFrame(t, codec, "ok", protocolTestBody{Name: "ok"})
		bad[HeaderSize] = 0xff
		_, err := decodeFrame(bad, codec, DefaultMaxPayloadSize)
		if !errors.Is(err, ErrInvalidMethodName) {
			t.Fatalf("expected ErrInvalidMethodName, got %v", err)
		}
	})

	t.Run("length mismatch", func(t *testing.T) {
		bad := append([]byte(nil), frame...)
		binary.BigEndian.PutUint32(bad[4:8], uint32(len(bad)))
		_, err := decodeFrame(bad, codec, DefaultMaxPayloadSize)
		if !errors.Is(err, ErrInvalidFrame) {
			t.Fatalf("expected ErrInvalidFrame, got %v", err)
		}
	})

	t.Run("payload too large", func(t *testing.T) {
		_, err := decodeFrame(frame, codec, 1)
		if !errors.Is(err, ErrPayloadTooLarge) {
			t.Fatalf("expected ErrPayloadTooLarge, got %v", err)
		}
	})
}

func TestValidateMethodName(t *testing.T) {
	if err := validateMethodName(""); !errors.Is(err, ErrInvalidMethodName) {
		t.Fatalf("expected empty method error, got %v", err)
	}
	if err := validateMethodName(strings.Repeat("a", MaxMethodNameLen+1)); !errors.Is(err, ErrInvalidMethodName) {
		t.Fatalf("expected too long method error, got %v", err)
	}
	if err := validateMethodName(string([]byte{0xff})); !errors.Is(err, ErrInvalidMethodName) {
		t.Fatalf("expected invalid utf8 method error, got %v", err)
	}
	if err := validateMethodName(strings.Repeat("a", MaxMethodNameLen)); err != nil {
		t.Fatalf("expected max length method to pass, got %v", err)
	}
}

func mustCodec(t *testing.T, serialization Serialization) BodyCodec {
	t.Helper()

	codec, err := newBodyCodec(serialization)
	if err != nil {
		t.Fatalf("newBodyCodec returned error: %v", err)
	}
	return codec
}

func encodeTestFrame(t *testing.T, codec BodyCodec, method string, body any) []byte {
	t.Helper()

	return encodeProtocolTestFrame(t, codec, FrameKindMessage, method, "", body)
}

func encodeInvokeTestFrame(t *testing.T, codec BodyCodec, kind FrameKind, method, invocationID string, body any) []byte {
	t.Helper()

	return encodeProtocolTestFrame(t, codec, kind, method, invocationID, body)
}

func encodeProtocolTestFrame(t *testing.T, codec BodyCodec, kind FrameKind, method, invocationID string, body any) []byte {
	t.Helper()

	payload := make([]byte, 0, 32)
	var err error
	payload, err = codec.MarshalAppend(payload, body)
	if err != nil {
		t.Fatalf("MarshalAppend returned error: %v", err)
	}
	return encodeInvokeTestFrameRaw(t, codec, kind, method, invocationID, payload)
}

func encodeInvokeTestFrameRaw(t *testing.T, codec BodyCodec, kind FrameKind, method, invocationID string, payload []byte) []byte {
	t.Helper()

	if err := validateFrameKind(kind); err != nil {
		t.Fatalf("validateFrameKind returned error: %v", err)
	}
	if method != "" {
		if err := validateMethodName(method); err != nil {
			t.Fatalf("validateMethodName returned error: %v", err)
		}
	}
	if kind != FrameKindMessage || invocationID != "" {
		if err := validateInvocationID(invocationID); err != nil {
			t.Fatalf("validateInvocationID returned error: %v", err)
		}
	}

	frame := make([]byte, HeaderSize, HeaderSize+len(method)+len(invocationID)+len(payload))
	frame = append(frame, method...)
	frame = append(frame, invocationID...)
	frame = append(frame, payload...)
	encodeFrameHeader(frame[:HeaderSize], FrameHeader{
		Version:         protocolVersion,
		Codec:           codec.Serialization(),
		Kind:            kind,
		MethodLen:       uint8(len(method)),
		InvocationIDLen: uint8(len(invocationID)),
		BodyLen:         uint32(len(payload)),
	})
	return frame
}
