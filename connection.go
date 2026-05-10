package signalg

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coder/websocket"
	"github.com/kanengo/ku/poolx/slicepool"
)

var emptyFrameHeader [HeaderSize]byte

var frameBufferPool = &slicepool.Pool[byte]{}

const (
	defaultEncodedPayloadSizeHint = 128
	encodedPayloadHeadroomDivisor = 8
	encodedPayloadDecayDivisor    = 8
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
	protocol   *protocolConfig

	sendQueue          chan []byte
	sendMu             sync.Mutex
	sendClosed         bool
	writeTimeout       time.Duration
	slowConsumerPolicy SlowConsumerPolicy
	encodedPayloadHint atomic.Int64

	opsMu          sync.Mutex
	activeOps      int
	activeHandlers int
	draining       bool
	opsDone        chan struct{}
}

func newConnection(id, userID string, request *http.Request, ws *websocket.Conn, protocol *protocolConfig, sendQueueSize int, writeTimeout time.Duration, slowConsumerPolicy SlowConsumerPolicy) *Connection {
	ctx, cancel := context.WithCancel(context.Background())
	opsDone := make(chan struct{})
	close(opsDone)
	conn := &Connection{
		ID:                 id,
		UserID:             userID,
		ctx:                ctx,
		cancel:             cancel,
		ws:                 ws,
		protocol:           protocol,
		sendQueue:          make(chan []byte, sendQueueSize),
		writeTimeout:       writeTimeout,
		slowConsumerPolicy: slowConsumerPolicy,
		opsDone:            opsDone,
	}
	if request != nil {
		conn.Request = request.Clone(ctx)
		conn.remoteAddr = parseRemoteAddr(request.RemoteAddr)
	}
	go conn.writeLoop()
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
	err := c.ws.Close(code, reason)
	c.closeContext()
	return err
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

// Send encodes body with the connection codec and queues one SignalG binary frame.
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
	if err := c.writeEncodedProtocolFrame(ctx, FrameKindMessage, method, "", body); err != nil {
		c.endOperation()
		return err
	}
	return nil
}

// SendRaw queues one SignalG binary frame with an already-encoded payload.
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
	if err := c.writeRawProtocolFrame(ctx, FrameKindMessage, method, "", payload); err != nil {
		c.endOperation()
		return err
	}
	return nil
}

// Complete queues one successful invocation completion frame.
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
	if err := c.writeEncodedProtocolFrame(ctx, FrameKindCompletion, "", invocationID, body); err != nil {
		c.endOperation()
		return err
	}
	return nil
}

// CompleteError queues one failed invocation completion frame.
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
	if err == nil {
		err = errors.New("signalg: invocation failed")
	}
	if writeErr := c.writeRawProtocolFrame(ctx, FrameKindError, "", invocationID, []byte(err.Error())); writeErr != nil {
		c.endOperation()
		return writeErr
	}
	return nil
}

// Ping queues one SignalG protocol-level heartbeat ping frame.
func (c *Connection) Ping(ctx context.Context) error {
	if c == nil || c.ws == nil {
		return errors.New("signalg: nil websocket connection")
	}
	if c.protocol == nil {
		return ErrUnsupportedCodec
	}
	if !c.beginSendOperation() {
		return ErrHandlerShuttingDown
	}
	if err := c.writeRawProtocolFrame(ctx, FrameKindPing, "", "", nil); err != nil {
		c.endOperation()
		return err
	}
	return nil
}

// Pong queues one SignalG protocol-level heartbeat pong frame.
func (c *Connection) Pong(ctx context.Context) error {
	if c == nil || c.ws == nil {
		return errors.New("signalg: nil websocket connection")
	}
	if c.protocol == nil {
		return ErrUnsupportedCodec
	}
	if !c.beginSendOperation() {
		return ErrHandlerShuttingDown
	}
	if err := c.writeRawProtocolFrame(ctx, FrameKindPong, "", "", nil); err != nil {
		c.endOperation()
		return err
	}
	return nil
}

func (c *Connection) writeEncodedProtocolFrame(ctx context.Context, kind FrameKind, method, invocationID string, body any) error {
	if err := validateProtocolFrame(kind, method, invocationID); err != nil {
		return err
	}

	prefixLen := HeaderSize + len(method) + len(invocationID)
	frame := getFrameBuffer(c.encodedFrameBufferSize(prefixLen))

	frame = append(frame, emptyFrameHeader[:]...)
	frame = append(frame, method...)
	frame = append(frame, invocationID...)

	prefixFrame := frame
	var err error
	frame, err = c.protocol.marshalAppend(frame, body)
	if !sameFrameBuffer(prefixFrame, frame) {
		putFrameBuffer(prefixFrame)
	}
	if err != nil {
		putFrameBuffer(frame)
		return err
	}
	bodyLen := len(frame) - prefixLen
	if err := c.protocol.ensurePayloadSize(bodyLen); err != nil {
		putFrameBuffer(frame)
		return err
	}
	c.observeEncodedPayloadSize(bodyLen)
	return c.enqueuePreparedProtocolFrame(ctx, kind, method, invocationID, frame)
}

func (c *Connection) writeRawProtocolFrame(ctx context.Context, kind FrameKind, method, invocationID string, payload []byte) error {
	if err := validateProtocolFrame(kind, method, invocationID); err != nil {
		return err
	}
	if err := c.protocol.ensurePayloadSize(len(payload)); err != nil {
		return err
	}

	frame := getFrameBuffer(HeaderSize + len(method) + len(invocationID) + len(payload))

	frame = append(frame, emptyFrameHeader[:]...)
	frame = append(frame, method...)
	frame = append(frame, invocationID...)
	frame = append(frame, payload...)
	return c.enqueuePreparedProtocolFrame(ctx, kind, method, invocationID, frame)
}

func validateProtocolFrame(kind FrameKind, method, invocationID string) error {
	if err := validateFrameKind(kind); err != nil {
		return err
	}
	if isControlFrameKind(kind) {
		if method != "" {
			return fmt.Errorf("%w: control frame method must be empty", ErrInvalidMethodName)
		}
		if invocationID != "" {
			return fmt.Errorf("%w: control frame invocation id must be empty", ErrInvalidInvocationID)
		}
		return nil
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

func (c *Connection) enqueuePreparedProtocolFrame(ctx context.Context, kind FrameKind, method, invocationID string, frame []byte) error {
	prefixLen := HeaderSize + len(method) + len(invocationID)
	if len(frame) < prefixLen {
		putFrameBuffer(frame)
		return fmt.Errorf("%w: frame shorter than header", ErrInvalidFrame)
	}
	bodyLen := len(frame) - prefixLen
	if err := c.protocol.ensurePayloadSize(bodyLen); err != nil {
		putFrameBuffer(frame)
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
	return c.enqueueFrame(ctx, frame)
}

func (c *Connection) enqueueFrame(ctx context.Context, frame []byte) error {
	if ctx == nil {
		ctx = context.Background()
	}
	select {
	case <-ctx.Done():
		putFrameBuffer(frame)
		return ctx.Err()
	case <-c.ctx.Done():
		putFrameBuffer(frame)
		return c.ctx.Err()
	default:
	}

	c.sendMu.Lock()
	if c.sendClosed {
		c.sendMu.Unlock()
		putFrameBuffer(frame)
		return c.closedSendQueueError()
	}
	select {
	case c.sendQueue <- frame:
		c.sendMu.Unlock()
		return nil
	case <-c.ctx.Done():
		c.sendMu.Unlock()
		putFrameBuffer(frame)
		return c.ctx.Err()
	default:
		c.sendMu.Unlock()
		putFrameBuffer(frame)
		c.disconnectSlowConsumer()
		return ErrSlowConsumer
	}
}

func (c *Connection) closedSendQueueError() error {
	if err := c.ctx.Err(); err != nil {
		return err
	}
	return ErrHandlerShuttingDown
}

func (c *Connection) writeLoop() {
	for {
		select {
		case frame := <-c.sendQueue:
			if !c.writeQueuedFrame(frame) {
				c.discardQueuedFrames()
				return
			}
		case <-c.ctx.Done():
			c.discardQueuedFrames()
			return
		}
	}
}

func (c *Connection) writeQueuedFrame(frame []byte) bool {
	defer c.endOperation()
	defer putFrameBuffer(frame)

	ctx := c.ctx
	var cancel context.CancelFunc
	if c.writeTimeout > 0 {
		ctx, cancel = context.WithTimeout(c.ctx, c.writeTimeout)
		defer cancel()
	}

	if err := c.ws.Write(ctx, websocket.MessageBinary, frame); err != nil {
		c.closeContext()
		_ = c.closeNow()
		return false
	}
	return true
}

func (c *Connection) discardQueuedFrames() {
	c.sendMu.Lock()
	defer c.sendMu.Unlock()
	if c.sendClosed {
		return
	}
	c.sendClosed = true
	for {
		select {
		case frame := <-c.sendQueue:
			putFrameBuffer(frame)
			c.endOperation()
		default:
			return
		}
	}
}

func (c *Connection) disconnectSlowConsumer() {
	switch c.slowConsumerPolicy {
	case SlowConsumerPolicyDisconnect:
		c.closeContext()
		_ = c.closeNow()
	default:
		c.closeContext()
		_ = c.closeNow()
	}
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

func sameFrameBuffer(a, b []byte) bool {
	if len(a) == 0 || len(b) == 0 {
		return len(a) == len(b)
	}
	return &a[0] == &b[0]
}

func (c *Connection) encodedFrameBufferSize(prefixLen int) int {
	bodyHint := defaultEncodedPayloadSizeHint
	if c != nil {
		if hint := c.encodedPayloadHint.Load(); hint > 0 {
			bodyHint = int(hint)
		}
	}

	bodyHint += bodyHint / encodedPayloadHeadroomDivisor
	if c != nil && c.protocol != nil && c.protocol.maxPayloadSize > 0 && int64(bodyHint) > c.protocol.maxPayloadSize {
		bodyHint = int(c.protocol.maxPayloadSize)
	}
	return prefixLen + bodyHint
}

func (c *Connection) observeEncodedPayloadSize(size int) {
	if c == nil {
		return
	}
	if size < defaultEncodedPayloadSizeHint {
		size = defaultEncodedPayloadSizeHint
	}

	nextSample := int64(size)
	for {
		current := c.encodedPayloadHint.Load()
		next := nextSample
		// Grow immediately to avoid repeated reallocations, then decay gradually as payloads shrink.
		if current > 0 && nextSample < current {
			next = current - (current-nextSample)/encodedPayloadDecayDivisor
		}
		if c.encodedPayloadHint.CompareAndSwap(current, next) {
			return
		}
	}
}
