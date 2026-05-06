package signalg

import (
	"encoding/binary"
	"errors"
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
		frame := encodeTestFrame(t, codec, 7, protocolTestBody{Name: "msgpack", Seq: 1})

		msg, err := decodeFrame(frame, codec, 0)
		if err != nil {
			t.Fatalf("decodeFrame returned error: %v", err)
		}
		if msg.Type != 7 {
			t.Fatalf("expected message type 7, got %d", msg.Type)
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
		frame := encodeTestFrame(t, codec, 8, wrapperspb.String("protobuf"))

		msg, err := decodeFrame(frame, codec, DefaultMaxPayloadSize)
		if err != nil {
			t.Fatalf("decodeFrame returned error: %v", err)
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
		frame := encodeTestFrame(t, codec, 9, protocolTestBody{Name: "json", Seq: 2})

		msg, err := decodeFrame(frame, codec, DefaultMaxPayloadSize)
		if err != nil {
			t.Fatalf("decodeFrame returned error: %v", err)
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

func TestProtocolFrameValidation(t *testing.T) {
	codec := mustCodec(t, SerializationMessagePack)
	frame := encodeTestFrame(t, codec, 1, protocolTestBody{Name: "ok"})

	t.Run("bad magic", func(t *testing.T) {
		bad := append([]byte(nil), frame...)
		bad[0] = 'X'
		_, err := decodeFrame(bad, codec, DefaultMaxPayloadSize)
		if !errors.Is(err, ErrInvalidFrame) {
			t.Fatalf("expected ErrInvalidFrame, got %v", err)
		}
	})

	t.Run("bad version", func(t *testing.T) {
		bad := append([]byte(nil), frame...)
		bad[2] = 99
		_, err := decodeFrame(bad, codec, DefaultMaxPayloadSize)
		if !errors.Is(err, ErrInvalidFrame) {
			t.Fatalf("expected ErrInvalidFrame, got %v", err)
		}
	})

	t.Run("unexpected codec", func(t *testing.T) {
		bad := append([]byte(nil), frame...)
		bad[3] = byte(SerializationJSON)
		_, err := decodeFrame(bad, codec, DefaultMaxPayloadSize)
		if !errors.Is(err, ErrUnexpectedCodec) {
			t.Fatalf("expected ErrUnexpectedCodec, got %v", err)
		}
	})

	t.Run("length mismatch", func(t *testing.T) {
		bad := append([]byte(nil), frame...)
		binary.BigEndian.PutUint32(bad[8:12], uint32(len(bad)))
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

func mustCodec(t *testing.T, serialization Serialization) BodyCodec {
	t.Helper()

	codec, err := newBodyCodec(serialization)
	if err != nil {
		t.Fatalf("newBodyCodec returned error: %v", err)
	}
	return codec
}

func encodeTestFrame(t *testing.T, codec BodyCodec, msgType MessageType, body any) []byte {
	t.Helper()

	frame := make([]byte, HeaderSize)
	var err error
	frame, err = codec.MarshalAppend(frame, body)
	if err != nil {
		t.Fatalf("MarshalAppend returned error: %v", err)
	}
	encodeFrameHeader(frame[:HeaderSize], FrameHeader{
		Version:     protocolVersion,
		Codec:       codec.Serialization(),
		MessageType: msgType,
		BodyLen:     uint32(len(frame) - HeaderSize),
	})
	return frame
}
