package signalg

import (
	"context"
	"net"
	"net/http"
	"sync"

	"github.com/coder/websocket"
)

// Connection represents one websocket connection managed by SignalG.
type Connection struct {
	ID      string
	UserID  string
	Request *http.Request

	ctx        context.Context
	cancel     context.CancelFunc
	once       sync.Once
	ws         *websocket.Conn
	remoteAddr net.Addr
}

func newConnection(id, userID string, request *http.Request, ws *websocket.Conn) *Connection {
	ctx, cancel := context.WithCancel(context.Background())
	conn := &Connection{
		ID:     id,
		UserID: userID,
		ctx:    ctx,
		cancel: cancel,
		ws:     ws,
	}
	if request != nil {
		conn.Request = request.Clone(ctx)
		conn.remoteAddr = parseRemoteAddr(request.RemoteAddr)
	}
	return conn
}

// RemoteAddr returns the remote network address for the websocket connection.
func (c *Connection) RemoteAddr() net.Addr {
	if c == nil {
		return nil
	}
	return c.remoteAddr
}

// Close closes the websocket connection with a normal closure status.
func (c *Connection) Close() error {
	return c.CloseWithStatus(websocket.StatusNormalClosure, "")
}

// CloseWithStatus closes the websocket connection with a specific close status and reason.
func (c *Connection) CloseWithStatus(code websocket.StatusCode, reason string) error {
	if c == nil || c.ws == nil {
		return nil
	}
	return c.ws.Close(code, reason)
}

// Subprotocol returns the negotiated websocket subprotocol.
func (c *Connection) Subprotocol() string {
	if c == nil || c.ws == nil {
		return ""
	}
	return c.ws.Subprotocol()
}

func (c *Connection) closeContext() {
	if c == nil || c.cancel == nil {
		return
	}
	c.once.Do(c.cancel)
}

func (c *Connection) closeNow() error {
	if c == nil || c.ws == nil {
		return nil
	}
	return c.ws.CloseNow()
}

func parseRemoteAddr(addr string) net.Addr {
	if addr == "" {
		return nil
	}
	tcpAddr, err := net.ResolveTCPAddr("tcp", addr)
	if err == nil {
		return tcpAddr
	}
	return stringAddr(addr)
}

type stringAddr string

func (a stringAddr) Network() string {
	return "tcp"
}

func (a stringAddr) String() string {
	return string(a)
}
