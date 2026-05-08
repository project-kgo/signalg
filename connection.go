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

var emptyFrameHeader [HeaderSize]byte

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

	opsMu          sync.Mutex
	activeOps      int
	activeHandlers int
	draining       bool
	opsDone        chan struct{}
}

func newConnection(id, userID string, request *http.Request, ws *websocket.Conn, protocol *protocolConfig) *Connection {
	ctx, cancel := context.WithCancel(context.Background())
	opsDone := make(chan struct{})
	close(opsDone)
	conn := &Connection{
		ID:       id,
		UserID:   userID,
		ctx:      ctx,
		cancel:   cancel,
		ws:       ws,
		protocol: protocol,
		opsDone:  opsDone,
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
func (c *Connection) Send(ctx context.Context, method string, body any) error {
	if c == nil || c.ws == nil {
		return errors.New("signalg: nil websocket connection")
	}
	if c.protocol == nil {
		return ErrUnsupportedCodec
	}
	if !c.beginSendOperation() {
		return ErrHandlerShuttingDown
	}
	defer c.endOperation()
	return c.writeEncodedProtocolFrame(ctx, FrameKindMessage, method, "", body)
}

// SendRaw writes one SignalG binary frame with an already-encoded payload.
func (c *Connection) SendRaw(ctx context.Context, method string, payload []byte) error {
	if c == nil || c.ws == nil {
		return errors.New("signalg: nil websocket connection")
	}
	if c.protocol == nil {
		return ErrUnsupportedCodec
	}
	if !c.beginSendOperation() {
		return ErrHandlerShuttingDown
	}
	defer c.endOperation()
	return c.writeRawProtocolFrame(ctx, FrameKindMessage, method, "", payload)
}

// Complete writes one successful invocation completion frame.
func (c *Connection) Complete(ctx context.Context, invocationID string, body any) error {
	if c == nil || c.ws == nil {
		return errors.New("signalg: nil websocket connection")
	}
	if c.protocol == nil {
		return ErrUnsupportedCodec
	}
	if !c.beginSendOperation() {
		return ErrHandlerShuttingDown
	}
	defer c.endOperation()
	return c.writeEncodedProtocolFrame(ctx, FrameKindCompletion, "", invocationID, body)
}

// CompleteError writes one failed invocation completion frame.
func (c *Connection) CompleteError(ctx context.Context, invocationID string, err error) error {
	if c == nil || c.ws == nil {
		return errors.New("signalg: nil websocket connection")
	}
	if c.protocol == nil {
		return ErrUnsupportedCodec
	}
	if !c.beginSendOperation() {
		return ErrHandlerShuttingDown
	}
	defer c.endOperation()
	if err == nil {
		err = errors.New("signalg: invocation failed")
	}
	return c.writeRawProtocolFrame(ctx, FrameKindError, "", invocationID, []byte(err.Error()))
}

func (c *Connection) writeEncodedProtocolFrame(ctx context.Context, kind FrameKind, method, invocationID string, body any) error {
	if err := validateProtocolFrame(kind, method, invocationID); err != nil {
		return err
	}

	frame := getFrameBuffer(HeaderSize + len(method) + len(invocationID) + 512)
	defer putFrameBuffer(frame)

	frame = append(frame, emptyFrameHeader[:]...)
	frame = append(frame, method...)
	frame = append(frame, invocationID...)

	var err error
	frame, err = c.protocol.marshalAppend(frame, body)
	if err != nil {
		return err
	}
	return c.writePreparedProtocolFrame(ctx, kind, method, invocationID, frame)
}

func (c *Connection) writeRawProtocolFrame(ctx context.Context, kind FrameKind, method, invocationID string, payload []byte) error {
	if err := validateProtocolFrame(kind, method, invocationID); err != nil {
		return err
	}
	if err := c.protocol.ensurePayloadSize(len(payload)); err != nil {
		return err
	}

	frame := getFrameBuffer(HeaderSize + len(method) + len(invocationID) + len(payload))
	defer putFrameBuffer(frame)

	frame = append(frame, emptyFrameHeader[:]...)
	frame = append(frame, method...)
	frame = append(frame, invocationID...)
	frame = append(frame, payload...)
	return c.writePreparedProtocolFrame(ctx, kind, method, invocationID, frame)
}

func validateProtocolFrame(kind FrameKind, method, invocationID string) error {
	if err := validateFrameKind(kind); err != nil {
		return err
	}
	if kind == FrameKindMessage || kind == FrameKindInvoke {
		if err := validateMethodName(method); err != nil {
			return err
		}
	} else if method != "" {
		if err := validateMethodName(method); err != nil {
			return err
		}
	}
	if kind != FrameKindMessage {
		if err := validateInvocationID(invocationID); err != nil {
			return err
		}
	} else if invocationID != "" {
		if err := validateInvocationID(invocationID); err != nil {
			return err
		}
	}
	return nil
}

func (c *Connection) writePreparedProtocolFrame(ctx context.Context, kind FrameKind, method, invocationID string, frame []byte) error {
	prefixLen := HeaderSize + len(method) + len(invocationID)
	if len(frame) < prefixLen {
		return fmt.Errorf("%w: frame shorter than header", ErrInvalidFrame)
	}
	bodyLen := len(frame) - prefixLen
	if err := c.protocol.ensurePayloadSize(bodyLen); err != nil {
		return err
	}
	encodeFrameHeader(frame[:HeaderSize], FrameHeader{
		Version:         protocolVersion,
		Codec:           c.protocol.serialization(),
		Kind:            kind,
		MethodLen:       uint8(len(method)),
		InvocationIDLen: uint8(len(invocationID)),
		BodyLen:         uint32(bodyLen),
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

func (c *Connection) beginHandlerOperation() bool {
	if c == nil {
		return false
	}
	c.opsMu.Lock()
	defer c.opsMu.Unlock()
	if c.draining {
		return false
	}
	c.beginOperationLocked()
	c.activeHandlers++
	return true
}

func (c *Connection) endHandlerOperation() {
	if c == nil {
		return
	}
	c.opsMu.Lock()
	if c.activeHandlers > 0 {
		c.activeHandlers--
	}
	c.endOperationLocked()
	c.opsMu.Unlock()
}

func (c *Connection) beginSendOperation() bool {
	if c == nil {
		return false
	}
	c.opsMu.Lock()
	defer c.opsMu.Unlock()
	if c.draining && c.activeHandlers == 0 {
		return false
	}
	c.beginOperationLocked()
	return true
}

func (c *Connection) endOperation() {
	if c == nil {
		return
	}
	c.opsMu.Lock()
	c.endOperationLocked()
	c.opsMu.Unlock()
}

func (c *Connection) beginDrain() <-chan struct{} {
	if c == nil {
		done := make(chan struct{})
		close(done)
		return done
	}
	c.opsMu.Lock()
	c.draining = true
	done := c.opsDone
	c.opsMu.Unlock()

	return done
}

func (c *Connection) beginOperationLocked() {
	if c.activeOps == 0 {
		c.opsDone = make(chan struct{})
	}
	c.activeOps++
}

func (c *Connection) endOperationLocked() {
	if c.activeOps == 0 {
		return
	}
	c.activeOps--
	if c.activeOps == 0 {
		close(c.opsDone)
	}
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
