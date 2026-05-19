package signalg

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
	signalgv1 "github.com/project-kgo/signalg/proto/signalg/v1"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

func TestNewHandlerValidationAndDefaults(t *testing.T) {
	_, err := NewHandler(Config{}, nil)
	if !errors.Is(err, ErrNilHubFactory) {
		t.Fatalf("expected ErrNilHubFactory, got %v", err)
	}

	handler, err := NewHandler(Config{}, func(*Connection) (Hub, error) {
		return &recordingHub{}, nil
	})
	if err != nil {
		t.Fatalf("NewHandler returned error: %v", err)
	}
	if handler.cfg.Path != DefaultPath {
		t.Fatalf("expected default path %q, got %q", DefaultPath, handler.cfg.Path)
	}
	if handler.logger == nil {
		t.Fatal("expected default logger")
	}
	if handler.cfg.Serialization != SerializationMessagePack {
		t.Fatalf("expected default serialization messagepack, got %s", handler.cfg.Serialization)
	}
	if handler.protocol.maxPayloadSize != DefaultMaxPayloadSize {
		t.Fatalf("expected default max payload %d, got %d", DefaultMaxPayloadSize, handler.protocol.maxPayloadSize)
	}
	if handler.cfg.PingInterval != 0 {
		t.Fatalf("expected default ping interval 0, got %s", handler.cfg.PingInterval)
	}
	if handler.cfg.PingTimeout != 0 {
		t.Fatalf("expected default ping timeout 0, got %s", handler.cfg.PingTimeout)
	}
	wantReadLimit := HeaderSize + MaxMethodNameLen + MaxInvocationIDLen + DefaultMaxPayloadSize
	if handler.cfg.ReadLimit != wantReadLimit {
		t.Fatalf("expected default read limit %d, got %d", wantReadLimit, handler.cfg.ReadLimit)
	}
	if handler.cfg.SendQueueSize != DefaultSendQueueSize {
		t.Fatalf("expected default send queue size %d, got %d", DefaultSendQueueSize, handler.cfg.SendQueueSize)
	}
	if handler.cfg.WriteTimeout != DefaultWriteTimeout {
		t.Fatalf("expected default write timeout %s, got %s", DefaultWriteTimeout, handler.cfg.WriteTimeout)
	}

	handler, err = NewHandler(Config{Path: "custom"}, func(*Connection) (Hub, error) {
		return &recordingHub{}, nil
	})
	if err != nil {
		t.Fatalf("NewHandler returned error: %v", err)
	}
	if handler.cfg.Path != "/custom" {
		t.Fatalf("expected normalized path /custom, got %q", handler.cfg.Path)
	}

	handler, err = NewHandler(Config{PingInterval: -time.Second}, func(*Connection) (Hub, error) {
		return &recordingHub{}, nil
	})
	if err != nil {
		t.Fatalf("NewHandler returned error: %v", err)
	}
	if handler.cfg.PingInterval != DefaultPingInterval {
		t.Fatalf("expected negative ping interval to normalize to %s, got %s", DefaultPingInterval, handler.cfg.PingInterval)
	}
	if handler.cfg.PingTimeout != 5*DefaultPingInterval {
		t.Fatalf("expected default ping timeout to normalize to %s, got %s", 5*DefaultPingInterval, handler.cfg.PingTimeout)
	}

	_, err = NewHandler(Config{Serialization: Serialization(99)}, func(*Connection) (Hub, error) {
		return &recordingHub{}, nil
	})
	if !errors.Is(err, ErrUnsupportedCodec) {
		t.Fatalf("expected ErrUnsupportedCodec, got %v", err)
	}
}

func TestNewServerValidationAndDefaults(t *testing.T) {
	_, err := NewServer(ServerConfig{}, func(*Connection) (Hub, error) {
		return &recordingHub{}, nil
	})
	if !errors.Is(err, ErrMissingAddr) {
		t.Fatalf("expected ErrMissingAddr, got %v", err)
	}

	_, err = NewServer(ServerConfig{Addr: "127.0.0.1:1"}, nil)
	if !errors.Is(err, ErrNilHubFactory) {
		t.Fatalf("expected ErrNilHubFactory, got %v", err)
	}

	server, err := NewServer(ServerConfig{Addr: "127.0.0.1:1"}, func(*Connection) (Hub, error) {
		return &recordingHub{}, nil
	})
	if err != nil {
		t.Fatalf("NewServer returned error: %v", err)
	}
	if server.Handler() == nil {
		t.Fatal("expected handler")
	}
	if server.handler.cfg.Path != DefaultPath {
		t.Fatalf("expected default path %q, got %q", DefaultPath, server.handler.cfg.Path)
	}
}

func TestConnectionWithoutUnderlyingWebsocket(t *testing.T) {
	conn := &Connection{ID: "test"}
	if conn.RemoteAddr() != nil {
		t.Fatal("expected nil remote addr")
	}
	if conn.Subprotocol() != "" {
		t.Fatal("expected empty subprotocol")
	}
	if err := conn.Close(); err != nil {
		t.Fatalf("Close returned error: %v", err)
	}
	if err := conn.CloseWithStatus(websocket.StatusNormalClosure, "done"); err != nil {
		t.Fatalf("CloseWithStatus returned error: %v", err)
	}
}

func TestHandlerConnectionLifecycle(t *testing.T) {
	connected := make(chan *Connection, 1)
	disconnected := make(chan error, 1)
	handler := newTestHandler(t, Config{}, func(*Connection) (Hub, error) {
		return &recordingHub{
			connected:    connected,
			disconnected: disconnected,
		}, nil
	})
	server := httptest.NewServer(handler)
	defer server.Close()

	client := dialWebSocket(t, httpToWS(server.URL)+DefaultPath, nil)
	defer client.CloseNow()

	conn := receiveConnection(t, connected)
	if conn.ID == "" {
		t.Fatal("expected connection id")
	}
	if conn.UserID != "" {
		t.Fatalf("expected empty user id, got %q", conn.UserID)
	}
	if conn.Request == nil {
		t.Fatal("expected request")
	}
	if conn.Request.URL.Path != DefaultPath {
		t.Fatalf("expected request path %q, got %q", DefaultPath, conn.Request.URL.Path)
	}
	if conn.RemoteAddr() == nil {
		t.Fatal("expected remote addr")
	}
	if conn.Subprotocol() != "" {
		t.Fatalf("expected empty subprotocol, got %q", conn.Subprotocol())
	}
	if handler.Online() <= 0 {
		t.Fatal("expected online count to be positive")
	}

	if err := client.Close(websocket.StatusNormalClosure, "done"); err != nil {
		t.Fatalf("close client: %v", err)
	}
	receiveDisconnect(t, disconnected)
}

func TestHandlerSendsConnectedMessageWithConfigForSerializations(t *testing.T) {
	for _, serialization := range []Serialization{
		SerializationMessagePack,
		SerializationJSON,
		SerializationProtobuf,
	} {
		t.Run(serialization.String(), func(t *testing.T) {
			handler := newTestHandler(t, Config{
				Serialization: serialization,
				PingInterval:  7500 * time.Millisecond,
				UserProvider: UserProviderFunc(func(*http.Request) (string, error) {
					return "user-1", nil
				}),
			}, func(*Connection) (Hub, error) {
				return &recordingHub{}, nil
			})
			server := httptest.NewServer(handler)
			defer server.Close()

			client := dialWebSocket(t, httpToWS(server.URL)+DefaultPath, nil)
			defer client.CloseNow()

			msg := readRawProtocolMessage(t, handler, client)
			if msg.Kind != FrameKindMessage {
				t.Fatalf("expected message frame, got %s", msg.Kind)
			}
			if msg.Method != ConnectedMethod {
				t.Fatalf("expected method %s, got %q", ConnectedMethod, msg.Method)
			}
			if msg.InvocationID != "" {
				t.Fatalf("expected empty invocation id, got %q", msg.InvocationID)
			}
			assertConnectedPayload(t, msg, serialization, "user-1", 7500)
		})
	}
}

func TestConnectionSendWritesProtocolFrame(t *testing.T) {
	connected := make(chan *Connection, 1)
	handler := newTestHandler(t, Config{}, func(*Connection) (Hub, error) {
		return &recordingHub{connected: connected}, nil
	})
	server := httptest.NewServer(handler)
	defer server.Close()

	client := dialWebSocket(t, httpToWS(server.URL)+DefaultPath, nil)
	defer client.CloseNow()

	conn := receiveConnection(t, connected)
	if err := conn.Send(context.Background(), "server.send", protocolTestBody{Name: "server", Seq: 3}); err != nil {
		t.Fatalf("Send returned error: %v", err)
	}

	msg := readProtocolMessage(t, handler, client)
	if msg.Method != "server.send" {
		t.Fatalf("expected method server.send, got %q", msg.Method)
	}
	var got protocolTestBody
	if err := msg.Decode(&got); err != nil {
		t.Fatalf("Decode returned error: %v", err)
	}
	if got.Name != "server" || got.Seq != 3 {
		t.Fatalf("unexpected decoded body: %#v", got)
	}
}

func TestMessageHubReceivesProtocolMessage(t *testing.T) {
	messages := make(chan Message, 1)
	handler := newTestHandler(t, Config{Serialization: SerializationJSON}, func(*Connection) (Hub, error) {
		return &recordingMessageHub{messages: messages}, nil
	})
	server := httptest.NewServer(handler)
	defer server.Close()

	client := dialWebSocket(t, httpToWS(server.URL)+DefaultPath, nil)
	defer client.CloseNow()

	frame := encodeTestFrame(t, handler.protocol.codec, "client.send", protocolTestBody{Name: "client", Seq: 4})
	if err := client.Write(context.Background(), websocket.MessageBinary, frame); err != nil {
		t.Fatalf("client write: %v", err)
	}

	select {
	case msg := <-messages:
		if msg.Method != "client.send" {
			t.Fatalf("expected method client.send, got %q", msg.Method)
		}
		var got protocolTestBody
		if err := msg.Decode(&got); err != nil {
			t.Fatalf("Decode returned error: %v", err)
		}
		if got.Name != "client" || got.Seq != 4 {
			t.Fatalf("unexpected decoded body: %#v", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for protocol message")
	}
}

func TestHandlerRespondsToProtocolPingWithOptionalNotificationWithoutDispatching(t *testing.T) {
	messages := make(chan Message, 1)
	pings := make(chan *Connection, 1)
	pingStarted := make(chan struct{}, 1)
	releasePing := make(chan struct{})
	handler := newTestHandler(t, Config{}, func(*Connection) (Hub, error) {
		return &recordingPingHub{
			recordingMessageHub: recordingMessageHub{messages: messages},
			pings:               pings,
			onPing: func(_ context.Context, conn *Connection) {
				pingStarted <- struct{}{}
				pings <- conn
				<-releasePing
			},
		}, nil
	})
	server := httptest.NewServer(handler)
	defer server.Close()
	defer close(releasePing)

	client := dialWebSocket(t, httpToWS(server.URL)+DefaultPath, nil)
	defer client.CloseNow()

	frame := encodeControlTestFrame(t, handler.protocol.codec, FrameKindPing)
	if err := client.Write(context.Background(), websocket.MessageBinary, frame); err != nil {
		t.Fatalf("client write: %v", err)
	}

	select {
	case <-pingStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for ping callback to start")
	}

	msg := readProtocolMessage(t, handler, client)
	if msg.Kind != FrameKindPong {
		t.Fatalf("expected pong frame, got %s", msg.Kind)
	}
	if msg.Method != "" || msg.InvocationID != "" || len(msg.Payload) != 0 {
		t.Fatalf("unexpected pong frame: %#v", msg)
	}

	select {
	case conn := <-pings:
		if conn == nil {
			t.Fatal("expected ping callback connection")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for ping callback")
	}

	select {
	case msg := <-messages:
		t.Fatalf("expected ping to stay in framework layer, got message %#v", msg)
	case <-time.After(50 * time.Millisecond):
	}
}

func TestHandlerHeartbeatTimeoutClosesIdleConnection(t *testing.T) {
	disconnected := make(chan error, 1)
	handler := newTestHandler(t, Config{
		PingInterval: 20 * time.Millisecond,
		PingTimeout:  50 * time.Millisecond,
	}, func(*Connection) (Hub, error) {
		return &recordingHub{disconnected: disconnected}, nil
	})
	server := httptest.NewServer(handler)
	defer server.Close()

	client := dialWebSocket(t, httpToWS(server.URL)+DefaultPath, nil)
	defer client.CloseNow()

	assertConnectedMessage(t, handler, client)
	receiveDisconnect(t, disconnected)
	if got := handler.Online(); got != 0 {
		t.Fatalf("expected zero online connections, got %d", got)
	}
}

func TestDefaultDispatcherDispatchesMatchingMethod(t *testing.T) {
	codec := mustCodec(t, SerializationMessagePack)
	hub := &reflectDispatchHub{}
	dispatcher, err := dispatcherFor(hub)
	if err != nil {
		t.Fatalf("dispatcherFor returned error: %v", err)
	}

	frame := encodeTestFrame(t, codec, "Echo", protocolTestBody{Name: "client", Seq: 4})
	msg, err := decodeFrame(frame, codec, DefaultMaxPayloadSize)
	if err != nil {
		t.Fatalf("decodeFrame returned error: %v", err)
	}

	res, err := dispatcher.dispatch(context.Background(), hub, msg)
	if err != nil {
		t.Fatalf("dispatch returned error: %v", err)
	}
	got, ok := res.(*protocolTestBody)
	if !ok {
		t.Fatalf("expected *protocolTestBody response, got %T", res)
	}
	if got.Name != "client:echo" || got.Seq != 5 {
		t.Fatalf("unexpected response: %#v", got)
	}
}

func TestDefaultDispatcherIgnoresInvalidSignatures(t *testing.T) {
	dispatcher, err := dispatcherFor(&invalidSignatureHub{})
	if err != nil {
		t.Fatalf("dispatcherFor returned error: %v", err)
	}
	if len(dispatcher.routes) != 0 {
		t.Fatalf("expected no registered routes, got %d", len(dispatcher.routes))
	}
}

func TestMessageHubTakesPriorityOverDefaultDispatcher(t *testing.T) {
	messages := make(chan Message, 1)
	handler := newTestHandler(t, Config{}, func(*Connection) (Hub, error) {
		return &priorityMessageHub{messages: messages}, nil
	})
	server := httptest.NewServer(handler)
	defer server.Close()

	client := dialWebSocket(t, httpToWS(server.URL)+DefaultPath, nil)
	defer client.CloseNow()

	frame := encodeInvokeTestFrame(t, handler.protocol.codec, FrameKindInvoke, "Echo", "priority-1", protocolTestBody{Name: "client", Seq: 4})
	if err := client.Write(context.Background(), websocket.MessageBinary, frame); err != nil {
		t.Fatalf("client write: %v", err)
	}

	select {
	case msg := <-messages:
		if msg.Method != "Echo" || msg.InvocationID != "priority-1" {
			t.Fatalf("unexpected message: %#v", msg)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for protocol message")
	}

	assertConnectedMessage(t, handler, client)

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if _, _, err := client.Read(ctx); err == nil {
		t.Fatal("expected no default completion when MessageHub handles the message")
	}
}

func TestDefaultDispatcherInvokeMessagePackCompletion(t *testing.T) {
	connected := make(chan *Connection, 1)
	handler := newTestHandler(t, Config{}, func(*Connection) (Hub, error) {
		return &reflectDispatchHub{
			recordingHub: recordingHub{connected: connected},
		}, nil
	})
	server := httptest.NewServer(handler)
	defer server.Close()

	client := dialWebSocket(t, httpToWS(server.URL)+DefaultPath, nil)
	defer client.CloseNow()
	receiveConnection(t, connected)

	frame := encodeInvokeTestFrame(t, handler.protocol.codec, FrameKindInvoke, "Echo", "invoke-1", protocolTestBody{Name: "client", Seq: 7})
	if err := client.Write(context.Background(), websocket.MessageBinary, frame); err != nil {
		t.Fatalf("client write: %v", err)
	}

	msg := readProtocolMessage(t, handler, client)
	if msg.Kind != FrameKindCompletion {
		t.Fatalf("expected completion frame, got %s", msg.Kind)
	}
	if msg.InvocationID != "invoke-1" {
		t.Fatalf("expected invocation id invoke-1, got %q", msg.InvocationID)
	}
	var got protocolTestBody
	if err := msg.Decode(&got); err != nil {
		t.Fatalf("Decode returned error: %v", err)
	}
	if got.Name != "client:echo" || got.Seq != 8 {
		t.Fatalf("unexpected completion body: %#v", got)
	}
}

func TestDefaultDispatcherInvokeJSONAndProtobuf(t *testing.T) {
	t.Run("json", func(t *testing.T) {
		handler := newTestHandler(t, Config{Serialization: SerializationJSON}, func(*Connection) (Hub, error) {
			return &reflectDispatchHub{}, nil
		})
		server := httptest.NewServer(handler)
		defer server.Close()

		client := dialWebSocket(t, httpToWS(server.URL)+DefaultPath, nil)
		defer client.CloseNow()

		frame := encodeInvokeTestFrame(t, handler.protocol.codec, FrameKindInvoke, "Echo", "json-1", protocolTestBody{Name: "json", Seq: 1})
		if err := client.Write(context.Background(), websocket.MessageBinary, frame); err != nil {
			t.Fatalf("client write: %v", err)
		}

		msg := readProtocolMessage(t, handler, client)
		if msg.Kind != FrameKindCompletion {
			t.Fatalf("expected completion frame, got %s", msg.Kind)
		}
		var got protocolTestBody
		if err := msg.Decode(&got); err != nil {
			t.Fatalf("Decode returned error: %v", err)
		}
		if got.Name != "json:echo" || got.Seq != 2 {
			t.Fatalf("unexpected completion body: %#v", got)
		}
	})

	t.Run("protobuf", func(t *testing.T) {
		handler := newTestHandler(t, Config{Serialization: SerializationProtobuf}, func(*Connection) (Hub, error) {
			return &protobufDispatchHub{}, nil
		})
		server := httptest.NewServer(handler)
		defer server.Close()

		client := dialWebSocket(t, httpToWS(server.URL)+DefaultPath, nil)
		defer client.CloseNow()

		frame := encodeInvokeTestFrame(t, handler.protocol.codec, FrameKindInvoke, "EchoProto", "proto-1", wrapperspb.String("protobuf"))
		if err := client.Write(context.Background(), websocket.MessageBinary, frame); err != nil {
			t.Fatalf("client write: %v", err)
		}

		msg := readProtocolMessage(t, handler, client)
		if msg.Kind != FrameKindCompletion {
			t.Fatalf("expected completion frame, got %s", msg.Kind)
		}
		var got wrapperspb.StringValue
		if err := msg.Decode(&got); err != nil {
			t.Fatalf("Decode returned error: %v", err)
		}
		if got.Value != "protobuf:echo" {
			t.Fatalf("unexpected completion body: %q", got.Value)
		}
	})
}

func TestDefaultDispatcherInvokeErrorKeepsConnectionOpen(t *testing.T) {
	handler := newTestHandler(t, Config{}, func(*Connection) (Hub, error) {
		return &reflectDispatchHub{}, nil
	})
	server := httptest.NewServer(handler)
	defer server.Close()

	client := dialWebSocket(t, httpToWS(server.URL)+DefaultPath, nil)
	defer client.CloseNow()

	frame := encodeInvokeTestFrame(t, handler.protocol.codec, FrameKindInvoke, "Fail", "fail-1", protocolTestBody{Name: "client", Seq: 1})
	if err := client.Write(context.Background(), websocket.MessageBinary, frame); err != nil {
		t.Fatalf("client write fail invoke: %v", err)
	}

	msg := readProtocolMessage(t, handler, client)
	if msg.Kind != FrameKindError {
		t.Fatalf("expected error frame, got %s", msg.Kind)
	}
	if msg.InvocationID != "fail-1" {
		t.Fatalf("expected invocation id fail-1, got %q", msg.InvocationID)
	}
	if string(msg.Payload) != "reflect failure" {
		t.Fatalf("expected error payload reflect failure, got %q", msg.Payload)
	}

	frame = encodeInvokeTestFrame(t, handler.protocol.codec, FrameKindInvoke, "Echo", "ok-1", protocolTestBody{Name: "after", Seq: 2})
	if err := client.Write(context.Background(), websocket.MessageBinary, frame); err != nil {
		t.Fatalf("client write echo invoke: %v", err)
	}
	msg = readProtocolMessage(t, handler, client)
	if msg.Kind != FrameKindCompletion {
		t.Fatalf("expected completion after error, got %s", msg.Kind)
	}
	if msg.InvocationID != "ok-1" {
		t.Fatalf("expected invocation id ok-1, got %q", msg.InvocationID)
	}
}

func TestDefaultDispatcherUnknownInvokeReturnsError(t *testing.T) {
	handler := newTestHandler(t, Config{}, func(*Connection) (Hub, error) {
		return &reflectDispatchHub{}, nil
	})
	server := httptest.NewServer(handler)
	defer server.Close()

	client := dialWebSocket(t, httpToWS(server.URL)+DefaultPath, nil)
	defer client.CloseNow()

	frame := encodeInvokeTestFrame(t, handler.protocol.codec, FrameKindInvoke, "Missing", "missing-1", protocolTestBody{Name: "client", Seq: 1})
	if err := client.Write(context.Background(), websocket.MessageBinary, frame); err != nil {
		t.Fatalf("client write: %v", err)
	}

	msg := readProtocolMessage(t, handler, client)
	if msg.Kind != FrameKindError {
		t.Fatalf("expected error frame, got %s", msg.Kind)
	}
	if msg.InvocationID != "missing-1" {
		t.Fatalf("expected invocation id missing-1, got %q", msg.InvocationID)
	}
	if !strings.Contains(string(msg.Payload), ErrMethodNotFound.Error()) {
		t.Fatalf("expected method not found payload, got %q", msg.Payload)
	}
}

func TestDefaultDispatcherProtobufRejectsNonProtoRequest(t *testing.T) {
	codec := mustCodec(t, SerializationProtobuf)
	hub := &reflectDispatchHub{}
	dispatcher, err := dispatcherFor(hub)
	if err != nil {
		t.Fatalf("dispatcherFor returned error: %v", err)
	}
	msg := Message{
		Kind:    FrameKindMessage,
		Method:  "Echo",
		Payload: []byte{0x0a, 0x01, 'x'},
		codec:   codec,
	}
	_, err = dispatcher.dispatch(context.Background(), hub, msg)
	if !errors.Is(err, ErrUnsupportedBodyValue) {
		t.Fatalf("expected ErrUnsupportedBodyValue, got %v", err)
	}
}

func TestConnectionSendConcurrent(t *testing.T) {
	connected := make(chan *Connection, 1)
	handler := newTestHandler(t, Config{}, func(*Connection) (Hub, error) {
		return &recordingHub{connected: connected}, nil
	})
	server := httptest.NewServer(handler)
	defer server.Close()

	client := dialWebSocket(t, httpToWS(server.URL)+DefaultPath, nil)
	defer client.CloseNow()

	conn := receiveConnection(t, connected)
	const sends = 16

	errs := make(chan error, sends)
	var wg sync.WaitGroup
	wg.Add(sends)
	for i := 0; i < sends; i++ {
		i := i
		go func() {
			defer wg.Done()
			errs <- conn.SendRaw(context.Background(), "method."+string(rune('a'+i)), []byte{byte(i)})
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("SendRaw returned error: %v", err)
		}
	}

	seen := make(map[string]struct{}, sends)
	for i := 0; i < sends; i++ {
		msg := readProtocolMessage(t, handler, client)
		seen[msg.Method] = struct{}{}
	}
	for i := 0; i < sends; i++ {
		method := "method." + string(rune('a'+i))
		if _, ok := seen[method]; !ok {
			t.Fatalf("missing sent method %s", method)
		}
	}
}

func TestConnectionSendQueueFullDisconnectsSlowConsumer(t *testing.T) {
	protocol, err := newProtocolConfig(SerializationMessagePack, 0)
	if err != nil {
		t.Fatalf("newProtocolConfig returned error: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	opsDone := make(chan struct{})
	close(opsDone)
	conn := &Connection{
		ID:        "slow",
		ctx:       ctx,
		cancel:    cancel,
		protocol:  protocol,
		sendQueue: make(chan []byte, 1),
		opsDone:   opsDone,
	}

	if !conn.beginSendOperation() {
		t.Fatal("expected first send operation to begin")
	}
	if err := conn.writeRawProtocolFrame(context.Background(), FrameKindMessage, "server.one", "", []byte("1")); err != nil {
		conn.endOperation()
		t.Fatalf("first enqueue returned error: %v", err)
	}

	if !conn.beginSendOperation() {
		t.Fatal("expected second send operation to begin")
	}
	err = conn.writeRawProtocolFrame(context.Background(), FrameKindMessage, "server.two", "", []byte("2"))
	if !errors.Is(err, ErrSlowConsumer) {
		conn.endOperation()
		t.Fatalf("expected ErrSlowConsumer, got %v", err)
	}
	conn.endOperation()

	select {
	case <-conn.ctx.Done():
	case <-time.After(time.Second):
		t.Fatal("expected queue-full slow consumer to cancel connection context")
	}

	done := conn.beginDrain()
	select {
	case <-done:
		t.Fatal("expected queued frame to keep drain open")
	default:
	}

	frame := <-conn.sendQueue
	putFrameBuffer(frame)
	conn.endOperation()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("expected drain to finish after queued frame is released")
	}
}

func TestConnectionRejectsSendAfterQueueClosed(t *testing.T) {
	protocol, err := newProtocolConfig(SerializationMessagePack, 0)
	if err != nil {
		t.Fatalf("newProtocolConfig returned error: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	opsDone := make(chan struct{})
	close(opsDone)
	conn := &Connection{
		ID:        "closed",
		ctx:       ctx,
		cancel:    cancel,
		protocol:  protocol,
		sendQueue: make(chan []byte, 1),
		opsDone:   opsDone,
	}
	conn.sendClosed = true

	if !conn.beginSendOperation() {
		t.Fatal("expected send operation to begin")
	}
	err = conn.writeRawProtocolFrame(context.Background(), FrameKindMessage, "server.late", "", []byte("late"))
	if !errors.Is(err, ErrHandlerShuttingDown) {
		conn.endOperation()
		t.Fatalf("expected ErrHandlerShuttingDown, got %v", err)
	}
	conn.endOperation()

	done := conn.beginDrain()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("expected closed queue send failure to release operation")
	}
}

func TestConnectionDrainAllowsSendFromActiveHandler(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	opsDone := make(chan struct{})
	close(opsDone)
	conn := &Connection{
		ID:      "handler-drain",
		ctx:     ctx,
		cancel:  cancel,
		opsDone: opsDone,
	}

	if !conn.beginHandlerOperation() {
		t.Fatal("expected handler operation to begin")
	}
	done := conn.beginDrain()
	select {
	case <-done:
		t.Fatal("expected active handler to keep drain open")
	default:
	}

	if !conn.beginSendOperation() {
		t.Fatal("expected active handler to allow send during drain")
	}
	conn.endHandlerOperation()
	select {
	case <-done:
		t.Fatal("expected queued send operation to keep drain open after handler returns")
	default:
	}

	conn.endOperation()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("expected drain to finish after handler and send operations end")
	}
}

func TestConnectionDrainRejectsSendWithoutActiveHandler(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	opsDone := make(chan struct{})
	close(opsDone)
	conn := &Connection{
		ID:      "no-handler-drain",
		ctx:     ctx,
		cancel:  cancel,
		opsDone: opsDone,
	}

	done := conn.beginDrain()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("expected idle connection drain to already be done")
	}
	if conn.beginSendOperation() {
		conn.endOperation()
		t.Fatal("expected send without active handler to be rejected during drain")
	}
}

func TestHandlerUserProvider(t *testing.T) {
	t.Run("resolves user id before hub factory", func(t *testing.T) {
		connected := make(chan *Connection, 1)
		handler := newTestHandler(t, Config{
			UserProvider: UserProviderFunc(func(r *http.Request) (string, error) {
				return r.Header.Get("X-User-ID"), nil
			}),
		}, func(conn *Connection) (Hub, error) {
			if conn.UserID != "user-1" {
				t.Fatalf("expected user id user-1, got %q", conn.UserID)
			}
			return &recordingHub{connected: connected}, nil
		})
		server := httptest.NewServer(handler)
		defer server.Close()

		client := dialWebSocket(t, httpToWS(server.URL)+DefaultPath, &websocket.DialOptions{
			HTTPHeader: http.Header{"X-User-ID": []string{"user-1"}},
		})
		defer client.CloseNow()

		conn := receiveConnection(t, connected)
		if conn.UserID != "user-1" {
			t.Fatalf("expected user id user-1, got %q", conn.UserID)
		}
	})

	t.Run("rejects when user provider fails", func(t *testing.T) {
		connected := make(chan *Connection, 1)
		handler := newTestHandler(t, Config{
			UserProvider: UserProviderFunc(func(*http.Request) (string, error) {
				return "", errors.New("missing user")
			}),
		}, func(*Connection) (Hub, error) {
			return &recordingHub{connected: connected}, nil
		})
		server := httptest.NewServer(handler)
		defer server.Close()

		client := dialWebSocket(t, httpToWS(server.URL)+DefaultPath, nil)
		defer client.CloseNow()

		select {
		case conn := <-connected:
			t.Fatalf("expected hub not to connect, got %s", conn.ID)
		case <-time.After(100 * time.Millisecond):
		}
	})
}

func TestHandlerUserConnectionIndex(t *testing.T) {
	connected := make(chan *Connection, 2)
	disconnected := make(chan error, 2)
	handler := newTestHandler(t, Config{
		UserProvider: UserProviderFunc(func(r *http.Request) (string, error) {
			return r.Header.Get("X-User-ID"), nil
		}),
	}, func(*Connection) (Hub, error) {
		return &recordingHub{
			connected:    connected,
			disconnected: disconnected,
		}, nil
	})
	server := httptest.NewServer(handler)
	defer server.Close()

	opts := &websocket.DialOptions{
		HTTPHeader: http.Header{"X-User-ID": []string{"user-1"}},
	}
	client1 := dialWebSocket(t, httpToWS(server.URL)+DefaultPath, opts)
	defer client1.CloseNow()
	client2 := dialWebSocket(t, httpToWS(server.URL)+DefaultPath, opts)
	defer client2.CloseNow()

	conn1 := receiveConnection(t, connected)
	conn2 := receiveConnection(t, connected)

	if got := handler.UserOnline("user-1"); got != 2 {
		t.Fatalf("expected user-1 online 2, got %d", got)
	}
	if got := handler.UserOnline(""); got != 0 {
		t.Fatalf("expected anonymous online 0, got %d", got)
	}
	if got := handler.UserOnline("missing"); got != 0 {
		t.Fatalf("expected missing user online 0, got %d", got)
	}

	userConnections := handler.UserConnections("user-1")
	if len(userConnections) != 2 {
		t.Fatalf("expected 2 user connections, got %d", len(userConnections))
	}
	assertContainsConnection(t, userConnections, conn1)
	assertContainsConnection(t, userConnections, conn2)

	if err := client1.Close(websocket.StatusNormalClosure, "done"); err != nil {
		t.Fatalf("close first client: %v", err)
	}
	receiveDisconnect(t, disconnected)
	if got := handler.UserOnline("user-1"); got != 1 {
		t.Fatalf("expected user-1 online 1, got %d", got)
	}

	if err := client2.Close(websocket.StatusNormalClosure, "done"); err != nil {
		t.Fatalf("close second client: %v", err)
	}
	receiveDisconnect(t, disconnected)
	if got := handler.UserOnline("user-1"); got != 0 {
		t.Fatalf("expected user-1 online 0, got %d", got)
	}
	if got := handler.UserConnections("user-1"); len(got) != 0 {
		t.Fatalf("expected no user connections, got %d", len(got))
	}
}

func TestHandlerSendAll(t *testing.T) {
	connected := make(chan *Connection, 2)
	handler := newTestHandler(t, Config{SendConcurrency: 1}, func(*Connection) (Hub, error) {
		return &recordingHub{connected: connected}, nil
	})
	server := httptest.NewServer(handler)
	defer server.Close()

	client1 := dialWebSocket(t, httpToWS(server.URL)+DefaultPath, nil)
	defer client1.CloseNow()
	client2 := dialWebSocket(t, httpToWS(server.URL)+DefaultPath, nil)
	defer client2.CloseNow()
	receiveConnection(t, connected)
	receiveConnection(t, connected)

	result := handler.SendAll(context.Background(), "server.broadcast", protocolTestBody{Name: "all", Seq: 1})
	assertSendResult(t, result, 2, 2, 0)
	assertProtocolMessage(t, handler, client1, "server.broadcast", protocolTestBody{Name: "all", Seq: 1})
	assertProtocolMessage(t, handler, client2, "server.broadcast", protocolTestBody{Name: "all", Seq: 1})
}

func TestHandlerSendAllRaw(t *testing.T) {
	connected := make(chan *Connection, 2)
	handler := newTestHandler(t, Config{SendConcurrency: 1}, func(*Connection) (Hub, error) {
		return &recordingHub{connected: connected}, nil
	})
	server := httptest.NewServer(handler)
	defer server.Close()

	client1 := dialWebSocket(t, httpToWS(server.URL)+DefaultPath, nil)
	defer client1.CloseNow()
	client2 := dialWebSocket(t, httpToWS(server.URL)+DefaultPath, nil)
	defer client2.CloseNow()
	receiveConnection(t, connected)
	receiveConnection(t, connected)

	payload := []byte{0x81, 0xa4, 'n', 'a', 'm', 'e', 0xa3, 'r', 'a', 'w'}
	result := handler.SendAllRaw(context.Background(), "server.broadcast.raw", payload)
	assertSendResult(t, result, 2, 2, 0)
	assertRawProtocolMessage(t, handler, client1, "server.broadcast.raw", payload)
	assertRawProtocolMessage(t, handler, client2, "server.broadcast.raw", payload)
}

func TestHandlerSendUsersDeduplicatesConnections(t *testing.T) {
	connected := make(chan *Connection, 3)
	handler := newTestHandler(t, Config{
		UserProvider: UserProviderFunc(func(r *http.Request) (string, error) {
			return r.Header.Get("X-User-ID"), nil
		}),
	}, func(*Connection) (Hub, error) {
		return &recordingHub{connected: connected}, nil
	})
	server := httptest.NewServer(handler)
	defer server.Close()

	client1 := dialWebSocket(t, httpToWS(server.URL)+DefaultPath, &websocket.DialOptions{
		HTTPHeader: http.Header{"X-User-ID": []string{"user-1"}},
	})
	defer client1.CloseNow()
	client2 := dialWebSocket(t, httpToWS(server.URL)+DefaultPath, &websocket.DialOptions{
		HTTPHeader: http.Header{"X-User-ID": []string{"user-1"}},
	})
	defer client2.CloseNow()
	client3 := dialWebSocket(t, httpToWS(server.URL)+DefaultPath, &websocket.DialOptions{
		HTTPHeader: http.Header{"X-User-ID": []string{"user-2"}},
	})
	defer client3.CloseNow()
	receiveConnection(t, connected)
	receiveConnection(t, connected)
	receiveConnection(t, connected)

	result := handler.SendUsers(context.Background(), []string{"user-1", "user-2", "user-1", ""}, "server.users", protocolTestBody{Name: "users", Seq: 2})
	assertSendResult(t, result, 3, 3, 0)
	assertProtocolMessage(t, handler, client1, "server.users", protocolTestBody{Name: "users", Seq: 2})
	assertProtocolMessage(t, handler, client2, "server.users", protocolTestBody{Name: "users", Seq: 2})
	assertProtocolMessage(t, handler, client3, "server.users", protocolTestBody{Name: "users", Seq: 2})
}

func TestHandlerSendUsersRawDeduplicatesConnections(t *testing.T) {
	connected := make(chan *Connection, 3)
	handler := newTestHandler(t, Config{
		UserProvider: UserProviderFunc(func(r *http.Request) (string, error) {
			return r.Header.Get("X-User-ID"), nil
		}),
	}, func(*Connection) (Hub, error) {
		return &recordingHub{connected: connected}, nil
	})
	server := httptest.NewServer(handler)
	defer server.Close()

	client1 := dialWebSocket(t, httpToWS(server.URL)+DefaultPath, &websocket.DialOptions{
		HTTPHeader: http.Header{"X-User-ID": []string{"user-1"}},
	})
	defer client1.CloseNow()
	client2 := dialWebSocket(t, httpToWS(server.URL)+DefaultPath, &websocket.DialOptions{
		HTTPHeader: http.Header{"X-User-ID": []string{"user-1"}},
	})
	defer client2.CloseNow()
	client3 := dialWebSocket(t, httpToWS(server.URL)+DefaultPath, &websocket.DialOptions{
		HTTPHeader: http.Header{"X-User-ID": []string{"user-2"}},
	})
	defer client3.CloseNow()
	receiveConnection(t, connected)
	receiveConnection(t, connected)
	receiveConnection(t, connected)

	payload := []byte{0x82, 0xa3, 's', 'e', 'q', 0x2a, 0xa3, 'r', 'a', 'w', 0xc3}
	result := handler.SendUsersRaw(context.Background(), []string{"user-1", "user-2", "user-1", ""}, "server.users.raw", payload)
	assertSendResult(t, result, 3, 3, 0)
	assertRawProtocolMessage(t, handler, client1, "server.users.raw", payload)
	assertRawProtocolMessage(t, handler, client2, "server.users.raw", payload)
	assertRawProtocolMessage(t, handler, client3, "server.users.raw", payload)
}

func TestHandlerSendConnectionsRawDeduplicatesConnections(t *testing.T) {
	connected := make(chan *Connection, 3)
	handler := newTestHandler(t, Config{SendConcurrency: 1}, func(*Connection) (Hub, error) {
		return &recordingHub{connected: connected}, nil
	})
	server := httptest.NewServer(handler)
	defer server.Close()

	client1 := dialWebSocket(t, httpToWS(server.URL)+DefaultPath, nil)
	defer client1.CloseNow()
	client2 := dialWebSocket(t, httpToWS(server.URL)+DefaultPath, nil)
	defer client2.CloseNow()
	client3 := dialWebSocket(t, httpToWS(server.URL)+DefaultPath, nil)
	defer client3.CloseNow()
	conn1 := receiveConnection(t, connected)
	conn2 := receiveConnection(t, connected)
	receiveConnection(t, connected)

	payload := []byte{0x82, 0xa3, 's', 'e', 'q', 0x2a, 0xa3, 'r', 'a', 'w', 0xc3}
	result := handler.SendConnectionsRaw(context.Background(), []string{conn1.ID, conn2.ID, conn1.ID, "", "missing"}, "server.connections.raw", payload)
	assertSendResult(t, result, 2, 2, 0)
	assertRawProtocolMessage(t, handler, client1, "server.connections.raw", payload)
	assertRawProtocolMessage(t, handler, client2, "server.connections.raw", payload)
	assertNoProtocolMessage(t, handler, client3)
}

func TestHandlerCloseUsersDeduplicatesConnections(t *testing.T) {
	connected := make(chan *Connection, 3)
	disconnected := make(chan error, 3)
	handler := newTestHandler(t, Config{
		UserProvider: UserProviderFunc(func(r *http.Request) (string, error) {
			return r.Header.Get("X-User-ID"), nil
		}),
		SendConcurrency: 1,
	}, func(*Connection) (Hub, error) {
		return &recordingHub{connected: connected, disconnected: disconnected}, nil
	})
	server := httptest.NewServer(handler)
	defer server.Close()

	client1 := dialWebSocket(t, httpToWS(server.URL)+DefaultPath, &websocket.DialOptions{
		HTTPHeader: http.Header{"X-User-ID": []string{"user-1"}},
	})
	defer client1.CloseNow()
	client2 := dialWebSocket(t, httpToWS(server.URL)+DefaultPath, &websocket.DialOptions{
		HTTPHeader: http.Header{"X-User-ID": []string{"user-1"}},
	})
	defer client2.CloseNow()
	client3 := dialWebSocket(t, httpToWS(server.URL)+DefaultPath, &websocket.DialOptions{
		HTTPHeader: http.Header{"X-User-ID": []string{"user-2"}},
	})
	defer client3.CloseNow()
	receiveConnection(t, connected)
	receiveConnection(t, connected)
	receiveConnection(t, connected)

	result := handler.CloseUsers(context.Background(), []string{"user-1", "user-1", "", "missing"})
	assertCloseResult(t, result, 2, 2, 0)
	assertClientDisconnected(t, handler, client1)
	assertClientDisconnected(t, handler, client2)
	assertNoProtocolMessage(t, handler, client3)
	receiveDisconnect(t, disconnected)
	receiveDisconnect(t, disconnected)
}

func TestHandlerCloseConnectionsDeduplicatesConnections(t *testing.T) {
	connected := make(chan *Connection, 3)
	disconnected := make(chan error, 3)
	handler := newTestHandler(t, Config{SendConcurrency: 1}, func(*Connection) (Hub, error) {
		return &recordingHub{connected: connected, disconnected: disconnected}, nil
	})
	server := httptest.NewServer(handler)
	defer server.Close()

	client1 := dialWebSocket(t, httpToWS(server.URL)+DefaultPath, nil)
	defer client1.CloseNow()
	client2 := dialWebSocket(t, httpToWS(server.URL)+DefaultPath, nil)
	defer client2.CloseNow()
	client3 := dialWebSocket(t, httpToWS(server.URL)+DefaultPath, nil)
	defer client3.CloseNow()
	conn1 := receiveConnection(t, connected)
	conn2 := receiveConnection(t, connected)
	receiveConnection(t, connected)

	result := handler.CloseConnections(context.Background(), []string{conn1.ID, conn2.ID, conn1.ID, "", "missing"})
	assertCloseResult(t, result, 2, 2, 0)
	assertClientDisconnected(t, handler, client1)
	assertClientDisconnected(t, handler, client2)
	assertNoProtocolMessage(t, handler, client3)
	receiveDisconnect(t, disconnected)
	receiveDisconnect(t, disconnected)
}

func TestHandlerGroupIndexAndSendGroup(t *testing.T) {
	connected := make(chan *Connection, 3)
	disconnected := make(chan error, 3)
	handler := newTestHandler(t, Config{}, func(*Connection) (Hub, error) {
		return &recordingHub{
			connected:    connected,
			disconnected: disconnected,
		}, nil
	})
	server := httptest.NewServer(handler)
	defer server.Close()

	client1 := dialWebSocket(t, httpToWS(server.URL)+DefaultPath, nil)
	defer client1.CloseNow()
	client2 := dialWebSocket(t, httpToWS(server.URL)+DefaultPath, nil)
	defer client2.CloseNow()
	client3 := dialWebSocket(t, httpToWS(server.URL)+DefaultPath, nil)
	defer client3.CloseNow()
	conn1 := receiveConnection(t, connected)
	conn2 := receiveConnection(t, connected)
	conn3 := receiveConnection(t, connected)

	if err := handler.AddToGroup(conn1, "room-1"); err != nil {
		t.Fatalf("AddToGroup conn1 returned error: %v", err)
	}
	if err := handler.AddToGroup(conn2, "room-1"); err != nil {
		t.Fatalf("AddToGroup conn2 returned error: %v", err)
	}
	if err := handler.AddToGroup(conn3, "room-2"); err != nil {
		t.Fatalf("AddToGroup conn3 returned error: %v", err)
	}
	if got := handler.GroupOnline("room-1"); got != 2 {
		t.Fatalf("expected room-1 online 2, got %d", got)
	}
	if got := handler.GroupOnline("missing"); got != 0 {
		t.Fatalf("expected missing group online 0, got %d", got)
	}

	groupConnections := handler.GroupConnections("room-1")
	if len(groupConnections) != 2 {
		t.Fatalf("expected 2 room-1 connections, got %d", len(groupConnections))
	}
	assertContainsConnection(t, groupConnections, conn1)
	assertContainsConnection(t, groupConnections, conn2)
	groupConnections[0] = nil
	if got := handler.GroupOnline("room-1"); got != 2 {
		t.Fatalf("expected snapshot mutation not to affect group, got %d", got)
	}

	result := handler.SendGroup(context.Background(), "room-1", "server.group", protocolTestBody{Name: "room", Seq: 3})
	assertSendResult(t, result, 2, 2, 0)
	assertProtocolMessage(t, handler, client1, "server.group", protocolTestBody{Name: "room", Seq: 3})
	assertProtocolMessage(t, handler, client2, "server.group", protocolTestBody{Name: "room", Seq: 3})

	if err := handler.RemoveFromGroup(conn2, "room-1"); err != nil {
		t.Fatalf("RemoveFromGroup returned error: %v", err)
	}
	if got := handler.GroupOnline("room-1"); got != 1 {
		t.Fatalf("expected room-1 online 1, got %d", got)
	}

	handler.RemoveFromAllGroups(conn1)
	if got := handler.GroupOnline("room-1"); got != 0 {
		t.Fatalf("expected room-1 online 0 after RemoveFromAllGroups, got %d", got)
	}

	if err := client3.Close(websocket.StatusNormalClosure, "done"); err != nil {
		t.Fatalf("close third client: %v", err)
	}
	receiveDisconnect(t, disconnected)
	if got := handler.GroupOnline("room-2"); got != 0 {
		t.Fatalf("expected room-2 online 0 after disconnect, got %d", got)
	}
}

func TestHandlerSendGroupRaw(t *testing.T) {
	connected := make(chan *Connection, 3)
	handler := newTestHandler(t, Config{}, func(*Connection) (Hub, error) {
		return &recordingHub{connected: connected}, nil
	})
	server := httptest.NewServer(handler)
	defer server.Close()

	client1 := dialWebSocket(t, httpToWS(server.URL)+DefaultPath, nil)
	defer client1.CloseNow()
	client2 := dialWebSocket(t, httpToWS(server.URL)+DefaultPath, nil)
	defer client2.CloseNow()
	conn1 := receiveConnection(t, connected)
	conn2 := receiveConnection(t, connected)

	if err := handler.AddToGroup(conn1, "room-raw"); err != nil {
		t.Fatalf("AddToGroup conn1 returned error: %v", err)
	}
	if err := handler.AddToGroup(conn2, "room-raw"); err != nil {
		t.Fatalf("AddToGroup conn2 returned error: %v", err)
	}

	payload := []byte{0xa9, 'r', 'a', 'w', '-', 'g', 'r', 'o', 'u', 'p'}
	result := handler.SendGroupRaw(context.Background(), "room-raw", "server.group.raw", payload)
	assertSendResult(t, result, 2, 2, 0)
	assertRawProtocolMessage(t, handler, client1, "server.group.raw", payload)
	assertRawProtocolMessage(t, handler, client2, "server.group.raw", payload)

	if got := handler.SendGroupRaw(context.Background(), "", "server.group.raw", payload); !errors.Is(got.Err, ErrInvalidGroup) {
		t.Fatalf("expected ErrInvalidGroup, got %v", got.Err)
	}
}

func TestHandlerGroupValidation(t *testing.T) {
	handler := newTestHandler(t, Config{}, func(*Connection) (Hub, error) {
		return &recordingHub{}, nil
	})

	if err := handler.AddToGroup(&Connection{ID: "missing"}, "room"); !errors.Is(err, ErrConnectionNotFound) {
		t.Fatalf("expected ErrConnectionNotFound, got %v", err)
	}
	if err := handler.AddToGroup(nil, "room"); !errors.Is(err, ErrConnectionNotFound) {
		t.Fatalf("expected ErrConnectionNotFound, got %v", err)
	}
	if err := handler.AddToGroup(&Connection{ID: "missing"}, " "); !errors.Is(err, ErrInvalidGroup) {
		t.Fatalf("expected ErrInvalidGroup, got %v", err)
	}
	if result := handler.SendGroup(context.Background(), "", "server.group", protocolTestBody{}); !errors.Is(result.Err, ErrInvalidGroup) {
		t.Fatalf("expected ErrInvalidGroup, got %v", result.Err)
	}
}

func TestHandlerBatchSendErrors(t *testing.T) {
	t.Run("invalid method", func(t *testing.T) {
		handler := newTestHandler(t, Config{}, func(*Connection) (Hub, error) {
			return &recordingHub{}, nil
		})
		result := handler.SendAll(context.Background(), "", protocolTestBody{})
		if !errors.Is(result.Err, ErrInvalidMethodName) {
			t.Fatalf("expected ErrInvalidMethodName, got %v", result.Err)
		}
	})

	t.Run("encoding failure", func(t *testing.T) {
		handler := newTestHandler(t, Config{Serialization: SerializationProtobuf}, func(*Connection) (Hub, error) {
			return &recordingHub{}, nil
		})
		result := handler.SendAll(context.Background(), "server.bad", protocolTestBody{})
		if !errors.Is(result.Err, ErrUnsupportedBodyValue) {
			t.Fatalf("expected ErrUnsupportedBodyValue, got %v", result.Err)
		}
	})

	t.Run("after shutdown", func(t *testing.T) {
		handler := newTestHandler(t, Config{}, func(*Connection) (Hub, error) {
			return &recordingHub{}, nil
		})
		if err := handler.Shutdown(context.Background()); err != nil {
			t.Fatalf("Shutdown returned error: %v", err)
		}
		result := handler.SendAll(context.Background(), "server.done", protocolTestBody{})
		if !errors.Is(result.Err, ErrHandlerShuttingDown) {
			t.Fatalf("expected ErrHandlerShuttingDown, got %v", result.Err)
		}
	})

	t.Run("partial failure", func(t *testing.T) {
		connected := make(chan *Connection, 1)
		handler := newTestHandler(t, Config{}, func(*Connection) (Hub, error) {
			return &recordingHub{connected: connected}, nil
		})
		server := httptest.NewServer(handler)
		defer server.Close()

		client := dialWebSocket(t, httpToWS(server.URL)+DefaultPath, nil)
		defer client.CloseNow()
		conn := receiveConnection(t, connected)

		payload, err := handler.prepareBatchPayload("server.partial", protocolTestBody{Name: "partial", Seq: 4})
		if err != nil {
			t.Fatalf("prepareBatchPayload returned error: %v", err)
		}
		connections := getConnectionSlice(2)
		connections = append(connections, conn, &Connection{ID: "broken"})
		result := handler.sendConnections(context.Background(), pooledConnections{connections: connections}, "server.partial", payload)
		assertSendResult(t, result, 2, 1, 1)
		if result.Err == nil {
			t.Fatal("expected aggregate send error")
		}
		assertProtocolMessage(t, handler, client, "server.partial", protocolTestBody{Name: "partial", Seq: 4})
	})
}

func TestHandlerRejectsUnexpectedPath(t *testing.T) {
	handler := newTestHandler(t, Config{}, func(*Connection) (Hub, error) {
		return &recordingHub{}, nil
	})
	server := httptest.NewServer(handler)
	defer server.Close()

	response, err := http.Get(server.URL + "/not-found")
	if err != nil {
		t.Fatalf("get unexpected path: %v", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", response.StatusCode)
	}
}

func TestHandlerOriginPolicy(t *testing.T) {
	t.Run("custom check origin rejects", func(t *testing.T) {
		handler := newTestHandler(t, Config{
			CheckOrigin: func(*http.Request) bool { return false },
		}, func(*Connection) (Hub, error) {
			return &recordingHub{}, nil
		})
		server := httptest.NewServer(handler)
		defer server.Close()

		_, response, err := websocket.Dial(context.Background(), httpToWS(server.URL)+DefaultPath, &websocket.DialOptions{
			HTTPHeader: http.Header{"Origin": []string{"https://evil.example"}},
		})
		if err == nil {
			t.Fatal("expected dial to fail")
		}
		if response == nil || response.StatusCode != http.StatusForbidden {
			t.Fatalf("expected 403 response, got %#v", response)
		}
	})

	t.Run("origin patterns allow cross origin", func(t *testing.T) {
		connected := make(chan *Connection, 1)
		handler := newTestHandler(t, Config{
			OriginPatterns: []string{"allowed.example"},
		}, func(*Connection) (Hub, error) {
			return &recordingHub{connected: connected}, nil
		})
		server := httptest.NewServer(handler)
		defer server.Close()

		client := dialWebSocket(t, httpToWS(server.URL)+DefaultPath, &websocket.DialOptions{
			HTTPHeader: http.Header{"Origin": []string{"https://allowed.example"}},
		})
		defer client.CloseNow()
		receiveConnection(t, connected)
	})
}

func TestHandlerClosesWhenHubConnectFails(t *testing.T) {
	disconnected := make(chan error, 1)
	connectErr := errors.New("connect failed")
	handler := newTestHandler(t, Config{}, func(*Connection) (Hub, error) {
		return &recordingHub{
			onConnected: func(context.Context, *Connection) error {
				return connectErr
			},
			disconnected: disconnected,
		}, nil
	})
	server := httptest.NewServer(handler)
	defer server.Close()

	client := dialWebSocket(t, httpToWS(server.URL)+DefaultPath, nil)
	defer client.CloseNow()

	if err := receiveDisconnect(t, disconnected); !errors.Is(err, connectErr) {
		t.Fatalf("expected connect error, got %v", err)
	}
}

func TestServerShutdownStopsServingAndClosesActiveConnections(t *testing.T) {
	connected := make(chan *Connection, 1)
	disconnected := make(chan error, 1)
	server := newTestServer(t, ServerConfig{}, func(*Connection) (Hub, error) {
		return &recordingHub{
			connected:    connected,
			disconnected: disconnected,
		}, nil
	})

	client := dialWebSocket(t, "ws://"+server.cfg.Addr+DefaultPath, nil)
	defer client.CloseNow()
	receiveConnection(t, connected)

	shutdownServer(t, server)
	receiveDisconnect(t, disconnected)

	_, _, err := websocket.Dial(context.Background(), "ws://"+server.cfg.Addr+DefaultPath, nil)
	if err == nil {
		t.Fatal("expected dial to fail after shutdown")
	}

	if err := server.Shutdown(context.Background()); err != nil {
		t.Fatalf("second Shutdown returned error: %v", err)
	}
}

func TestHandlerShutdownWaitsForInFlightMessageAndSend(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	handler := newTestHandler(t, Config{}, func(*Connection) (Hub, error) {
		return &drainMessageHub{
			started: started,
			release: release,
		}, nil
	})
	server := httptest.NewServer(handler)
	defer server.Close()

	client := dialWebSocket(t, httpToWS(server.URL)+DefaultPath, nil)
	defer client.CloseNow()

	frame := encodeTestFrame(t, handler.protocol.codec, "client.send", protocolTestBody{Name: "client", Seq: 1})
	if err := client.Write(context.Background(), websocket.MessageBinary, frame); err != nil {
		t.Fatalf("client write: %v", err)
	}

	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for message handler")
	}

	shutdownDone := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		shutdownDone <- handler.Shutdown(ctx)
	}()

	select {
	case err := <-shutdownDone:
		t.Fatalf("Shutdown returned before in-flight handler finished: %v", err)
	case <-time.After(100 * time.Millisecond):
	}

	close(release)

	msg := readProtocolMessage(t, handler, client)
	if msg.Method != "server.done" {
		t.Fatalf("expected method server.done, got %q", msg.Method)
	}

	select {
	case err := <-shutdownDone:
		if err != nil {
			t.Fatalf("Shutdown returned error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for shutdown")
	}
}

func TestHandlerShutdownOnlyRunsOnce(t *testing.T) {
	connected := make(chan *Connection, 1)
	disconnected := make(chan error, 1)
	handler := newTestHandler(t, Config{}, func(*Connection) (Hub, error) {
		return &recordingHub{
			connected:    connected,
			disconnected: disconnected,
		}, nil
	})
	server := httptest.NewServer(handler)
	defer server.Close()

	client := dialWebSocket(t, httpToWS(server.URL)+DefaultPath, nil)
	defer client.CloseNow()
	receiveConnection(t, connected)

	if err := handler.Shutdown(context.Background()); err != nil {
		t.Fatalf("first Shutdown returned error: %v", err)
	}
	receiveDisconnect(t, disconnected)

	if err := handler.Shutdown(context.Background()); err != nil {
		t.Fatalf("second Shutdown returned error: %v", err)
	}
	if got := handler.Online(); got != 0 {
		t.Fatalf("expected zero online connections, got %d", got)
	}
}

func TestServerShutdownClosesListenerWhenDrainTimesOut(t *testing.T) {
	started := make(chan struct{})
	cfg := ServerConfig{
		Addr:   freeAddr(t),
		Config: Config{Logger: slog.New(slog.NewTextHandler(io.Discard, nil))},
	}
	server, err := NewServer(cfg, func(*Connection) (Hub, error) {
		return &recordingMessageHub{
			onMessage: func(ctx context.Context, _ *Connection, _ Message) error {
				close(started)
				<-ctx.Done()
				return ctx.Err()
			},
		}, nil
	})
	if err != nil {
		t.Fatalf("NewServer returned error: %v", err)
	}
	if err := server.Start(); err != nil {
		t.Fatalf("Start returned error: %v", err)
	}

	client := dialWebSocket(t, "ws://"+server.cfg.Addr+DefaultPath, nil)
	defer client.CloseNow()

	frame := encodeTestFrame(t, server.handler.protocol.codec, "client.send", protocolTestBody{Name: "client", Seq: 1})
	if err := client.Write(context.Background(), websocket.MessageBinary, frame); err != nil {
		t.Fatalf("client write: %v", err)
	}

	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for message handler")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if err := server.Shutdown(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected deadline exceeded, got %v", err)
	}

	conn, err := net.DialTimeout("tcp", server.cfg.Addr, 100*time.Millisecond)
	if err == nil {
		_ = conn.Close()
		t.Fatal("expected listener to be closed after shutdown timeout")
	}
}

type recordingHub struct {
	onConnected  func(context.Context, *Connection) error
	connected    chan *Connection
	disconnected chan error
}

func (h *recordingHub) OnConnected(ctx context.Context, conn *Connection) error {
	if h.onConnected != nil {
		return h.onConnected(ctx, conn)
	}
	if h.connected != nil {
		h.connected <- conn
	}
	return nil
}

func (h *recordingHub) OnDisconnected(_ context.Context, _ *Connection, err error) {
	if h.disconnected != nil {
		h.disconnected <- err
	}
}

type reflectDispatchHub struct {
	recordingHub
}

func (h *reflectDispatchHub) Echo(_ context.Context, req *protocolTestBody) (*protocolTestBody, error) {
	return &protocolTestBody{
		Name: req.Name + ":echo",
		Seq:  req.Seq + 1,
	}, nil
}

func (h *reflectDispatchHub) Fail(context.Context, *protocolTestBody) (*protocolTestBody, error) {
	return nil, errors.New("reflect failure")
}

type protobufDispatchHub struct {
	recordingHub
}

func (h *protobufDispatchHub) EchoProto(_ context.Context, req *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
	return wrapperspb.String(req.Value + ":echo"), nil
}

type invalidSignatureHub struct {
	recordingHub
}

func (h *invalidSignatureHub) ValueReq(context.Context, protocolTestBody) (*protocolTestBody, error) {
	return nil, nil
}

func (h *invalidSignatureHub) MissingError(context.Context, *protocolTestBody) *protocolTestBody {
	return nil
}

func (h *invalidSignatureHub) WrongContext(string, *protocolTestBody) (*protocolTestBody, error) {
	return nil, nil
}

type recordingMessageHub struct {
	recordingHub
	messages  chan Message
	onMessage func(context.Context, *Connection, Message) error
}

func (h *recordingMessageHub) OnMessage(ctx context.Context, conn *Connection, msg Message) error {
	if h.onMessage != nil {
		return h.onMessage(ctx, conn, msg)
	}
	if h.messages != nil {
		h.messages <- msg
	}
	return nil
}

type recordingPingHub struct {
	recordingMessageHub
	pings  chan *Connection
	onPing func(context.Context, *Connection)
}

func (h *recordingPingHub) OnPing(ctx context.Context, conn *Connection) {
	if h.onPing != nil {
		h.onPing(ctx, conn)
		return
	}
	if h.pings != nil {
		h.pings <- conn
	}
}

type drainMessageHub struct {
	recordingHub
	started chan<- struct{}
	release <-chan struct{}
}

func (h *drainMessageHub) OnMessage(ctx context.Context, conn *Connection, _ Message) error {
	if h.started != nil {
		close(h.started)
	}
	if h.release != nil {
		<-h.release
	}
	return conn.Send(ctx, "server.done", protocolTestBody{Name: "server", Seq: 2})
}

type priorityMessageHub struct {
	reflectDispatchHub
	messages chan Message
}

func (h *priorityMessageHub) OnMessage(_ context.Context, _ *Connection, msg Message) error {
	h.messages <- msg
	return nil
}

func newTestHandler(t *testing.T, cfg Config, factory HubFactory) *Handler {
	t.Helper()

	cfg.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	handler, err := NewHandler(cfg, factory)
	if err != nil {
		t.Fatalf("NewHandler returned error: %v", err)
	}
	return handler
}

func newTestServer(t *testing.T, cfg ServerConfig, factory HubFactory) *Server {
	t.Helper()

	cfg.Addr = freeAddr(t)
	cfg.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))

	server, err := NewServer(cfg, factory)
	if err != nil {
		t.Fatalf("NewServer returned error: %v", err)
	}
	if err := server.Start(); err != nil {
		t.Fatalf("Start returned error: %v", err)
	}
	t.Cleanup(func() {
		shutdownServer(t, server)
	})
	return server
}

func readRawProtocolMessage(t *testing.T, handler *Handler, client *websocket.Conn) Message {
	t.Helper()

	typ, frame, err := client.Read(context.Background())
	if err != nil {
		t.Fatalf("client read: %v", err)
	}
	if typ != websocket.MessageBinary {
		t.Fatalf("expected binary message, got %s", typ)
	}
	msg, err := handler.protocol.decodeFrame(frame)
	if err != nil {
		t.Fatalf("decodeFrame returned error: %v", err)
	}
	return msg
}

func readProtocolMessage(t *testing.T, handler *Handler, client *websocket.Conn) Message {
	t.Helper()

	for {
		msg := readRawProtocolMessage(t, handler, client)
		if msg.Method != ConnectedMethod {
			return msg
		}
	}
}

func assertConnectedMessage(t *testing.T, handler *Handler, client *websocket.Conn) {
	t.Helper()

	msg := readRawProtocolMessage(t, handler, client)
	if msg.Method != ConnectedMethod {
		t.Fatalf("expected method %s, got %q", ConnectedMethod, msg.Method)
	}
}

func assertConnectedPayload(t *testing.T, msg Message, serialization Serialization, userID string, pingIntervalMilliseconds int64) {
	t.Helper()

	if serialization == SerializationProtobuf {
		var got signalgv1.ConnectedPayload
		if err := msg.Decode(&got); err != nil {
			t.Fatalf("Decode returned error: %v", err)
		}
		if got.GetConnectionId() == "" {
			t.Fatal("expected connection id")
		}
		if got.GetUserId() != userID {
			t.Fatalf("expected user id %q, got %q", userID, got.GetUserId())
		}
		if got.GetConfig().GetPingIntervalMs() != pingIntervalMilliseconds {
			t.Fatalf("expected ping interval %dms, got %dms", pingIntervalMilliseconds, got.GetConfig().GetPingIntervalMs())
		}
		return
	}

	var got ConnectedPayload
	if err := msg.Decode(&got); err != nil {
		t.Fatalf("Decode returned error: %v", err)
	}
	if got.ConnectionID == "" {
		t.Fatal("expected connection id")
	}
	if got.UserID != userID {
		t.Fatalf("expected user id %q, got %q", userID, got.UserID)
	}
	if got.Config.PingIntervalMilliseconds != pingIntervalMilliseconds {
		t.Fatalf("expected ping interval %dms, got %dms", pingIntervalMilliseconds, got.Config.PingIntervalMilliseconds)
	}
}

func assertProtocolMessage(t *testing.T, handler *Handler, client *websocket.Conn, method string, want protocolTestBody) {
	t.Helper()

	msg := readProtocolMessage(t, handler, client)
	if msg.Method != method {
		t.Fatalf("expected method %s, got %q", method, msg.Method)
	}
	var got protocolTestBody
	if err := msg.Decode(&got); err != nil {
		t.Fatalf("Decode returned error: %v", err)
	}
	if got != want {
		t.Fatalf("unexpected decoded body: got %#v want %#v", got, want)
	}
}

func assertRawProtocolMessage(t *testing.T, handler *Handler, client *websocket.Conn, method string, want []byte) {
	t.Helper()

	msg := readProtocolMessage(t, handler, client)
	if msg.Method != method {
		t.Fatalf("expected method %s, got %q", method, msg.Method)
	}
	if !bytes.Equal(msg.Payload, want) {
		t.Fatalf("unexpected raw payload: got %v want %v", msg.Payload, want)
	}
}

func assertNoProtocolMessage(t *testing.T, handler *Handler, client *websocket.Conn) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	for {
		typ, frame, err := client.Read(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			t.Fatalf("client read: %v", err)
		}
		if typ != websocket.MessageBinary {
			t.Fatalf("expected binary message, got %s", typ)
		}
		msg, err := handler.protocol.decodeFrame(frame)
		if err != nil {
			t.Fatalf("decodeFrame returned error: %v", err)
		}
		if msg.Method != ConnectedMethod {
			t.Fatalf("expected no protocol message, got %q", msg.Method)
		}
	}
}

func assertSendResult(t *testing.T, result SendResult, matched, sent, failed int) {
	t.Helper()

	if result.Matched != matched || result.Sent != sent || result.Failed != failed {
		t.Fatalf("unexpected send result: got matched=%d sent=%d failed=%d err=%v, want matched=%d sent=%d failed=%d",
			result.Matched, result.Sent, result.Failed, result.Err, matched, sent, failed)
	}
	if failed == 0 && result.Err != nil {
		t.Fatalf("expected nil send error, got %v", result.Err)
	}
	if failed > 0 && result.Err == nil {
		t.Fatal("expected send error")
	}
}

func assertCloseResult(t *testing.T, result CloseResult, matched, closed, failed int) {
	t.Helper()

	if result.Matched != matched || result.Closed != closed || result.Failed != failed {
		t.Fatalf("unexpected close result: got matched=%d closed=%d failed=%d err=%v, want matched=%d closed=%d failed=%d",
			result.Matched, result.Closed, result.Failed, result.Err, matched, closed, failed)
	}
	if failed == 0 && result.Err != nil {
		t.Fatalf("expected nil close error, got %v", result.Err)
	}
	if failed > 0 && result.Err == nil {
		t.Fatal("expected close error")
	}
}

func assertClientDisconnected(t *testing.T, handler *Handler, client *websocket.Conn) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	for {
		typ, frame, err := client.Read(ctx)
		if err != nil {
			return
		}
		if typ != websocket.MessageBinary {
			t.Fatalf("expected binary message or disconnect, got %s", typ)
		}
		msg, err := handler.protocol.decodeFrame(frame)
		if err != nil {
			t.Fatalf("decodeFrame returned error: %v", err)
		}
		if msg.Method != ConnectedMethod {
			t.Fatalf("expected disconnect after connected message, got %q", msg.Method)
		}
	}
}

func shutdownServer(t *testing.T, server *Server) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := server.Shutdown(ctx); err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("Shutdown returned error: %v", err)
	}
}

func freeAddr(t *testing.T) string {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen free addr: %v", err)
	}
	defer listener.Close()
	return listener.Addr().String()
}

func dialWebSocket(t *testing.T, url string, opts *websocket.DialOptions) *websocket.Conn {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	conn, response, err := websocket.Dial(ctx, url, opts)
	if err != nil {
		if response != nil && response.Body != nil {
			_, _ = io.ReadAll(response.Body)
			_ = response.Body.Close()
		}
		t.Fatalf("dial websocket: %v", err)
	}
	return conn
}

func receiveConnection(t *testing.T, ch <-chan *Connection) *Connection {
	t.Helper()

	select {
	case conn := <-ch:
		return conn
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for connection")
		return nil
	}
}

func receiveDisconnect(t *testing.T, ch <-chan error) error {
	t.Helper()

	select {
	case err := <-ch:
		return err
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for disconnect")
		return nil
	}
}

func assertContainsConnection(t *testing.T, connections []*Connection, want *Connection) {
	t.Helper()

	for _, conn := range connections {
		if conn == want {
			return
		}
	}
	t.Fatalf("expected connections to contain %s", want.ID)
}

func httpToWS(url string) string {
	return "ws" + strings.TrimPrefix(url, "http")
}

func TestConnectionRegistryExpires(t *testing.T) {
	registry := newConnectionRegistry()
	active := &Connection{ID: "active", UserID: "user-1"}
	idle := &Connection{ID: "idle", UserID: "user-1"}
	registry.add(active)
	registry.add(idle)

	now := time.Now()
	if entry, ok := registry.connections.Get(connectionKey(active)); ok {
		entry.lastSeen.Store(now.Add(-time.Minute).UnixNano())
	}
	if entry, ok := registry.connections.Get(connectionKey(idle)); ok {
		entry.lastSeen.Store(now.Add(-time.Minute).UnixNano())
	}
	registry.touch(active)

	expired := registry.expired(time.Now(), time.Second)
	if len(expired) != 1 || expired[0] != idle {
		t.Fatalf("expected only idle connection to expire, got %#v", expired)
	}
	if connections := registry.allConnections(); len(connections) != 1 || connections[0] != active {
		t.Fatalf("expected active connection to remain after touch, got %#v", connections)
	}
}

func TestPutConnectionSliceClearsReferences(t *testing.T) {
	conn := &Connection{ID: "pooled"}
	connections := getConnectionSlice(1)
	connections = append(connections, conn)
	backing := connections[:cap(connections)]

	putConnectionSlice(connections)

	for i, got := range backing {
		if got != nil {
			t.Fatalf("expected pooled connection slice slot %d to be nil, got %p", i, got)
		}
	}
}

func BenchmarkConnectionRegistrySnapshots50K(b *testing.B) {
	const total = 50000

	registry := newConnectionRegistry()
	for i := 0; i < total; i++ {
		conn := &Connection{
			ID:     "conn-" + strconv.Itoa(i),
			UserID: "user-" + strconv.Itoa(i%1000),
		}
		registry.add(conn)
		if i%10 == 0 {
			if err := registry.addToGroup(conn, "group-hot"); err != nil {
				b.Fatalf("addToGroup returned error: %v", err)
			}
		}
	}

	b.Run("users", func(b *testing.B) {
		connections := registry.userSnapshot([]string{"user-1", "user-2", "user-1"})
		if len(connections) != 100 {
			b.Fatalf("expected 100 user connections, got %d", len(connections))
		}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = registry.userSnapshot([]string{"user-1", "user-2", "user-1"})
		}
	})

	b.Run("group", func(b *testing.B) {
		connections := registry.groupConnections("group-hot")
		if len(connections) != 5000 {
			b.Fatalf("expected 5000 group connections, got %d", len(connections))
		}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = registry.groupConnections("group-hot")
		}
	})

	b.Run("all", func(b *testing.B) {
		connections := registry.allConnections()
		if len(connections) != total {
			b.Fatalf("expected %d connections, got %d", total, len(connections))
		}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = registry.allConnections()
		}
	})
}
