package signalg

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"sync"

	"github.com/coder/websocket"
	"github.com/kanengo/ku/poolx/slicepool"
)

const maxPooledFrameSize = HeaderSize + 4<<20

var emptyFrameHeader [HeaderSize]byte

// var frameBufferPool = sync.Pool{
// 	New: func() any {
// 		return make([]byte, 0, HeaderSize+1024)
// 	},
// }

var frameBufferPool = &slicepool.Pool[byte]{}

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
	protocol   *protocolConfig
}

func newConnection(id, userID string, request *http.Request, ws *websocket.Conn, protocol *protocolConfig) *Connection {
	ctx, cancel := context.WithCancel(context.Background())
	conn := &Connection{
		ID:       id,
		UserID:   userID,
		ctx:      ctx,
		cancel:   cancel,
		ws:       ws,
		protocol: protocol,
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

// Serialization returns the body serialization selected for this connection.
func (c *Connection) Serialization() Serialization {
	if c == nil {
		return SerializationMessagePack
	}
	return c.protocol.serialization()
}

// Send encodes body with the connection codec and writes one SignalG binary frame.
func (c *Connection) Send(ctx context.Context, msgType MessageType, body any) error {
	if c == nil || c.ws == nil {
		return errors.New("signalg: nil websocket connection")
	}
	if c.protocol == nil {
		return ErrUnsupportedCodec
	}

	frame := getFrameBuffer(HeaderSize)
	defer putFrameBuffer(frame)

	frame = append(frame, emptyFrameHeader[:]...)
	var err error
	frame, err = c.protocol.marshalAppend(frame, body)
	if err != nil {
		return err
	}
	return c.writeFrame(ctx, msgType, frame)
}

// SendRaw writes one SignalG binary frame with an already-encoded payload.
func (c *Connection) SendRaw(ctx context.Context, msgType MessageType, payload []byte) error {
	if c == nil || c.ws == nil {
		return errors.New("signalg: nil websocket connection")
	}
	if c.protocol == nil {
		return ErrUnsupportedCodec
	}

	frame := getFrameBuffer(HeaderSize + len(payload))
	defer putFrameBuffer(frame)

	frame = append(frame, emptyFrameHeader[:]...)
	frame = append(frame, payload...)
	return c.writeFrame(ctx, msgType, frame)
}

func (c *Connection) writeFrame(ctx context.Context, msgType MessageType, frame []byte) error {
	if len(frame) < HeaderSize {
		return fmt.Errorf("%w: frame shorter than header", ErrInvalidFrame)
	}
	payloadLen := len(frame) - HeaderSize
	if err := c.protocol.ensurePayloadSize(payloadLen); err != nil {
		return err
	}
	encodeFrameHeader(frame[:HeaderSize], FrameHeader{
		Version:     protocolVersion,
		Codec:       c.protocol.serialization(),
		MessageType: msgType,
		BodyLen:     uint32(payloadLen),
	})
	return c.ws.Write(ctx, websocket.MessageBinary, frame)
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

func getFrameBuffer(size int) []byte {
	frame := frameBufferPool.Get(size)
	if cap(frame) < size {
		return make([]byte, 0, size)
	}
	return frame[:0]
}

func putFrameBuffer(frame []byte) {
	frameBufferPool.Put(frame[:0])
}
