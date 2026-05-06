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
	"testing"
	"time"

	"github.com/coder/websocket"
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

	handler, err = NewHandler(Config{Path: "custom"}, func(*Connection) (Hub, error) {
		return &recordingHub{}, nil
	})
	if err != nil {
		t.Fatalf("NewHandler returned error: %v", err)
	}
	if handler.cfg.Path != "/custom" {
		t.Fatalf("expected normalized path /custom, got %q", handler.cfg.Path)
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

func httpToWS(url string) string {
	return "ws" + strings.TrimPrefix(url, "http")
}
