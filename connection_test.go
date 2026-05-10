package signalg

import "testing"

func TestConnectionEncodedFrameBufferSizeUsesAdaptivePayloadHint(t *testing.T) {
	protocol, err := newProtocolConfig(SerializationMessagePack, 0)
	if err != nil {
		t.Fatalf("newProtocolConfig returned error: %v", err)
	}
	conn := &Connection{protocol: protocol}
	prefixLen := HeaderSize + len("server.update")

	if got, want := conn.encodedFrameBufferSize(prefixLen), prefixLen+576; got != want {
		t.Fatalf("expected default frame size %d, got %d", want, got)
	}

	conn.observeEncodedPayloadSize(1024)
	if got, want := conn.encodedFrameBufferSize(prefixLen), prefixLen+1152; got != want {
		t.Fatalf("expected grown frame size %d, got %d", want, got)
	}

	conn.observeEncodedPayloadSize(128)
	if got, want := conn.encodedPayloadHint.Load(), int64(960); got != want {
		t.Fatalf("expected decayed payload hint %d, got %d", want, got)
	}
}

func TestConnectionEncodedFrameBufferSizeClampsToMaxPayloadSize(t *testing.T) {
	protocol, err := newProtocolConfig(SerializationMessagePack, 600)
	if err != nil {
		t.Fatalf("newProtocolConfig returned error: %v", err)
	}
	conn := &Connection{protocol: protocol}
	prefixLen := HeaderSize + len("server.update")

	conn.observeEncodedPayloadSize(1024)
	if got, want := conn.encodedFrameBufferSize(prefixLen), prefixLen+600; got != want {
		t.Fatalf("expected clamped frame size %d, got %d", want, got)
	}
}
