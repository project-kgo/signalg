package hertz

import (
	"context"
	"io"
	"log/slog"
	"net"
	"testing"
	"time"

	"github.com/cloudwego/hertz/pkg/app"
	hertzserver "github.com/cloudwego/hertz/pkg/app/server"
	"github.com/coder/websocket"
	"github.com/project-kgo/signalg"
)

func TestHandlerReturnsHertzHandler(t *testing.T) {
	handler, err := Handler(signalg.Config{
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}, func(*signalg.Connection) (signalg.Hub, error) {
		return &recordingHub{}, nil
	})
	if err != nil {
		t.Fatalf("Handler returned error: %v", err)
	}

	var _ app.HandlerFunc = handler
}

func TestHandlerWorksWithHertzRoute(t *testing.T) {
	connected := make(chan *signalg.Connection, 1)
	addr := freeAddr(t)

	handler, err := Handler(signalg.Config{
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}, func(*signalg.Connection) (signalg.Hub, error) {
		return &recordingHub{connected: connected}, nil
	})
	if err != nil {
		t.Fatalf("Handler returned error: %v", err)
	}

	h := hertzserver.New(hertzserver.WithHostPorts(addr))
	h.GET(signalg.DefaultPath, handler)

	go h.Spin()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = h.Shutdown(ctx)
	})

	waitTCP(t, addr)
	client := dialWebSocket(t, "ws://"+addr+signalg.DefaultPath)
	defer client.CloseNow()

	select {
	case conn := <-connected:
		if conn.ID == "" {
			t.Fatal("expected connection id")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for connection")
	}
}

type recordingHub struct {
	connected chan *signalg.Connection
}

func (h *recordingHub) OnConnected(_ context.Context, conn *signalg.Connection) error {
	if h.connected != nil {
		h.connected <- conn
	}
	return nil
}

func (h *recordingHub) OnDisconnected(context.Context, *signalg.Connection, error) {}

func freeAddr(t *testing.T) string {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen free addr: %v", err)
	}
	defer listener.Close()
	return listener.Addr().String()
}

func waitTCP(t *testing.T, addr string) {
	t.Helper()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", addr, 50*time.Millisecond)
		if err == nil {
			_ = conn.Close()
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", addr)
}

func dialWebSocket(t *testing.T, url string) *websocket.Conn {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	conn, response, err := websocket.Dial(ctx, url, nil)
	if err != nil {
		if response != nil && response.Body != nil {
			_, _ = io.ReadAll(response.Body)
			_ = response.Body.Close()
		}
		t.Fatalf("dial websocket: %v", err)
	}
	return conn
}
