package signalg

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
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
	wantReadLimit := HeaderSize + MaxMethodNameLen + MaxInvocationIDLen + DefaultMaxPayloadSize
	if handler.cfg.ReadLimit != wantReadLimit {
		t.Fatalf("expected default read limit %d, got %d", wantReadLimit, handler.cfg.ReadLimit)
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
	if msg.Method != "server.send" {
		t.Fatalf("expected method server.send, got %q", msg.Method)
	}
	var got protocolTestBody
	if err = msg.Decode(&got); err != nil {
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
		typ, frame, err := client.Read(context.Background())
		if err != nil {
			t.Fatalf("client read %d: %v", i, err)
		}
		if typ != websocket.MessageBinary {
			t.Fatalf("expected binary message, got %s", typ)
		}
		msg, err := handler.protocol.decodeFrame(frame)
		if err != nil {
			t.Fatalf("decodeFrame returned error: %v", err)
		}
		seen[msg.Method] = struct{}{}
	}
	for i := 0; i < sends; i++ {
		method := "method." + string(rune('a'+i))
		if _, ok := seen[method]; !ok {
			t.Fatalf("missing sent method %s", method)
		}
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

func readProtocolMessage(t *testing.T, handler *Handler, client *websocket.Conn) Message {
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
