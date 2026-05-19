package signalg

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"path"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coder/websocket"
	gonanoid "github.com/matoous/go-nanoid/v2"
	"github.com/project-kgo/signalg/internal/bpool"
)

const (
	// DefaultPath is the default websocket endpoint path.
	DefaultPath = "/signalg"

	// DefaultSendConcurrency is the default max concurrent connection writes
	// for Handler batch send APIs.
	DefaultSendConcurrency = 256

	// DefaultSendQueueSize is the default bounded outbound queue size for one
	// websocket connection.
	DefaultSendQueueSize = 256

	DefaultPingInterval = 15 * time.Second

	// DefaultWriteTimeout bounds one websocket write from the per-connection
	// writer goroutine.
	DefaultWriteTimeout = 5 * time.Second
)

var (
	ErrMissingAddr         = errors.New("signalg: server addr is required")
	ErrNilHubFactory       = errors.New("signalg: hub factory is required")
	ErrNilHub              = errors.New("signalg: hub factory returned nil hub")
	ErrHandlerShuttingDown = errors.New("signalg: handler is shutting down")
	ErrConnectionNotFound  = errors.New("signalg: connection is not managed by handler")
	ErrInvalidGroup        = errors.New("signalg: invalid group")
	ErrSlowConsumer        = errors.New("signalg: slow consumer")
)

// SlowConsumerPolicy selects the action taken when a connection cannot accept
// more outbound frames.
type SlowConsumerPolicy uint8

const (
	// SlowConsumerPolicyDisconnect closes the websocket connection when its
	// bounded outbound queue is full.
	SlowConsumerPolicyDisconnect SlowConsumerPolicy = iota
)

// Hub handles lifecycle events for one websocket connection.
type Hub interface {
	OnConnected(ctx context.Context, conn *Connection) error
	OnDisconnected(ctx context.Context, conn *Connection, err error)
}

// MessageHub receives decoded SignalG protocol messages from a connection.
type MessageHub interface {
	OnMessage(ctx context.Context, conn *Connection, msg Message) error
}

// PingHub receives SignalG protocol-level heartbeat pings from a connection.
type PingHub interface {
	OnPing(ctx context.Context, conn *Connection)
}

// HubFactory creates one Hub instance for one websocket connection.
type HubFactory func(conn *Connection) (Hub, error)

// UserProvider resolves a business user id from the websocket handshake request.
type UserProvider interface {
	GetUserID(r *http.Request) (string, error)
}

// UserProviderFunc adapts a function to UserProvider.
type UserProviderFunc func(r *http.Request) (string, error)

// GetUserID resolves a business user id from the websocket handshake request.
func (f UserProviderFunc) GetUserID(r *http.Request) (string, error) {
	return f(r)
}

// Config configures the SignalG websocket handler.
type Config struct {
	Path   string
	Logger *slog.Logger

	UserProvider         UserProvider
	CheckOrigin          func(*http.Request) bool
	OriginPatterns       []string
	InsecureSkipVerify   bool
	Subprotocols         []string
	CompressionMode      websocket.CompressionMode
	CompressionThreshold int
	ReadLimit            int64
	Serialization        Serialization
	MaxPayloadSize       int64
	SendConcurrency      int
	SendQueueSize        int
	WriteTimeout         time.Duration
	SlowConsumerPolicy   SlowConsumerPolicy
	PingInterval         time.Duration
	PingTimeout          time.Duration
}

// Handler accepts websocket requests and dispatches lifecycle events to hubs.
type Handler struct {
	cfg      Config
	factory  HubFactory
	logger   *slog.Logger
	protocol *protocolConfig

	registry           *connectionRegistry
	sendConcurrency    int
	sendQueueSize      int
	writeTimeout       time.Duration
	slowConsumerPolicy SlowConsumerPolicy
	admissionMu        sync.Mutex
	active             sync.WaitGroup
	shuttingDown       atomic.Bool
	online             atomic.Int64
	shutdownOnce       sync.Once
	shutdownErr        error
	heartbeatWake      chan struct{}
	heartbeatStop      chan struct{}
	heartbeatStopOnce  sync.Once
}

// SendResult describes a batch send operation outcome. Sent counts frames
// accepted by per-connection outbound queues.
type SendResult struct {
	Matched int
	Sent    int
	Failed  int
	Err     error
}

type drainingConnection struct {
	conn *Connection
	done <-chan struct{}
}

// ServerConfig configures a standalone SignalG websocket server.
type ServerConfig struct {
	Config
	Addr              string
	ReadHeaderTimeout time.Duration
}

// Server wraps a standard HTTP server for convenience.
type Server struct {
	cfg     ServerConfig
	handler *Handler
	server  *http.Server

	mu           sync.Mutex
	listener     net.Listener
	shutdownOnce sync.Once
	shutdownErr  error
}

// NewHandler creates a SignalG websocket handler.
func NewHandler(cfg Config, factory HubFactory) (*Handler, error) {
	if factory == nil {
		return nil, ErrNilHubFactory
	}

	cfg.Path = normalizePath(cfg.Path)
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	protocol, err := newProtocolConfig(cfg.Serialization, cfg.MaxPayloadSize)
	if err != nil {
		return nil, err
	}
	if cfg.ReadLimit == 0 {
		cfg.ReadLimit = HeaderSize + MaxMethodNameLen + MaxInvocationIDLen + protocol.maxPayloadSize
	}
	cfg.PingInterval = normalizePingInterval(cfg.PingInterval)
	cfg.PingTimeout = normalizePingTimeout(cfg.PingTimeout, cfg.PingInterval)
	cfg.SendQueueSize = normalizeSendQueueSize(cfg.SendQueueSize)
	cfg.WriteTimeout = normalizeWriteTimeout(cfg.WriteTimeout)
	sendConcurrency := normalizeSendConcurrency(cfg.SendConcurrency)

	h := &Handler{
		cfg:                cfg,
		factory:            factory,
		logger:             cfg.Logger,
		protocol:           protocol,
		registry:           newConnectionRegistry(),
		sendConcurrency:    sendConcurrency,
		sendQueueSize:      cfg.SendQueueSize,
		writeTimeout:       cfg.WriteTimeout,
		slowConsumerPolicy: cfg.SlowConsumerPolicy,
		heartbeatWake:      make(chan struct{}, 1),
		heartbeatStop:      make(chan struct{}),
	}
	if h.heartbeatEnabled() {
		go h.heartbeatLoop()
	}
	return h, nil
}

// ServeHTTP handles websocket upgrade requests.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL == nil || r.URL.Path != h.cfg.Path {
		http.NotFound(w, r)
		return
	}
	if h.shuttingDown.Load() {
		http.Error(w, ErrHandlerShuttingDown.Error(), http.StatusServiceUnavailable)
		return
	}
	if h.cfg.CheckOrigin != nil && !h.cfg.CheckOrigin(r) {
		http.Error(w, http.StatusText(http.StatusForbidden), http.StatusForbidden)
		return
	}

	ws, err := websocket.Accept(w, r, h.acceptOptions())
	if err != nil {
		h.logger.Warn("failed to accept websocket request",
			slog.String("path", r.URL.Path),
			slog.String("remote_addr", r.RemoteAddr),
			slog.Any("error", err),
		)
		return
	}

	connectionID, err := nextConnectionID()
	if err != nil {
		h.logger.Error("failed to generate connection id",
			slog.String("remote_addr", r.RemoteAddr),
			slog.Any("error", err),
		)
		_ = ws.CloseNow()
		return
	}

	userID, err := h.resolveUserID(r)
	if err != nil {
		h.logger.Error("failed to resolve user id",
			slog.String("connection_id", connectionID),
			slog.String("remote_addr", r.RemoteAddr),
			slog.Any("error", err),
		)
		_ = ws.CloseNow()
		return
	}

	conn := newConnection(connectionID, userID, r, ws, h.protocol, h.sendQueueSize, h.writeTimeout, h.slowConsumerPolicy)
	if h.cfg.ReadLimit != 0 {
		ws.SetReadLimit(h.cfg.ReadLimit)
	}

	hub, err := h.factory(conn)
	if err != nil {
		h.logger.Error("failed to create hub",
			slog.String("connection_id", conn.ID),
			slog.String("remote_addr", remoteAddr(conn)),
			slog.Any("error", err),
		)
		_ = ws.CloseNow()
		conn.closeContext()
		return
	}
	if hub == nil {
		h.logger.Error("hub factory returned nil hub",
			slog.String("connection_id", conn.ID),
			slog.String("remote_addr", remoteAddr(conn)),
		)
		_ = ws.CloseNow()
		conn.closeContext()
		return
	}

	if !h.addConnection(conn) {
		_ = ws.CloseNow()
		conn.closeContext()
		return
	}
	removed := false
	removeConnection := func() {
		if removed {
			return
		}
		removed = true
		h.removeConnection(conn)
	}
	defer h.finishConnection()
	defer removeConnection()
	defer conn.closeContext()

	err = h.runHandlerOperation(conn, func() error {
		return hub.OnConnected(conn.ctx, conn)
	})
	if err != nil {
		if errors.Is(err, ErrHandlerShuttingDown) {
			_ = ws.CloseNow()
			return
		}
		h.logger.Error("hub connected callback failed",
			slog.String("connection_id", conn.ID),
			slog.String("remote_addr", remoteAddr(conn)),
			slog.Any("error", err),
		)
		removeConnection()
		hub.OnDisconnected(conn.ctx, conn, err)
		_ = ws.CloseNow()
		return
	}

	if err = h.sendConnected(conn); err != nil {
		h.logger.Error("failed to send connected message",
			slog.String("connection_id", conn.ID),
			slog.String("remote_addr", remoteAddr(conn)),
			slog.Any("error", err),
		)
		removeConnection()
		hub.OnDisconnected(conn.ctx, conn, err)
		_ = ws.CloseNow()
		return
	}

	h.logger.Info("websocket connection opened",
		slog.String("connection_id", conn.ID),
		slog.String("remote_addr", remoteAddr(conn)),
	)

	err = h.readLoop(conn, hub)
	removeConnection()
	hub.OnDisconnected(conn.ctx, conn, err)
	h.logger.Info("websocket connection closed",
		slog.String("connection_id", conn.ID),
		slog.String("remote_addr", remoteAddr(conn)),
		slog.Any("error", err),
	)
}

// Online returns the current active websocket connection count.
func (h *Handler) Online() int {
	return int(h.online.Load())
}

// UserOnline returns the current active websocket connection count for a user.
func (h *Handler) UserOnline(userID string) int {
	if h == nil || userID == "" {
		return 0
	}
	return h.registry.userOnline(userID)
}

// UserConnections returns a snapshot of active websocket connections for a user.
func (h *Handler) UserConnections(userID string) []*Connection {
	if h == nil || userID == "" {
		return nil
	}
	return h.registry.userConnections(userID)
}

// SendUsers sends one message to every active connection for the provided users.
func (h *Handler) SendUsers(ctx context.Context, userIDs []string, method string, body any) SendResult {
	if h == nil {
		return SendResult{}
	}
	payload, err := h.prepareBatchPayload(method, body)
	if err != nil {
		return SendResult{Err: err}
	}
	if h.shuttingDown.Load() {
		return SendResult{Err: ErrHandlerShuttingDown}
	}
	return h.sendConnections(ctx, h.registry.userSnapshotPooled(userIDs), method, payload)
}

// SendUsersRaw sends one message with an already-encoded payload to every active connection for the provided users.
func (h *Handler) SendUsersRaw(ctx context.Context, userIDs []string, method string, payload []byte) SendResult {
	if h == nil {
		return SendResult{}
	}
	if err := h.prepareBatchRawPayload(method, payload); err != nil {
		return SendResult{Err: err}
	}
	if h.shuttingDown.Load() {
		return SendResult{Err: ErrHandlerShuttingDown}
	}
	return h.sendConnections(ctx, h.registry.userSnapshotPooled(userIDs), method, payload)
}

// SendConnectionsRaw sends one message with an already-encoded payload to every active connection for the provided connection IDs.
func (h *Handler) SendConnectionsRaw(ctx context.Context, connectionIDs []string, method string, payload []byte) SendResult {
	if h == nil {
		return SendResult{}
	}
	return h.sendConnectionsRaw(ctx, connectionIDs, method, payload)
}

func (h *Handler) sendConnectionsRaw(ctx context.Context, connectionIDs []string, method string, payload []byte) SendResult {
	if err := h.prepareBatchRawPayload(method, payload); err != nil {
		return SendResult{Err: err}
	}
	if h.shuttingDown.Load() {
		return SendResult{Err: ErrHandlerShuttingDown}
	}
	return h.sendConnections(ctx, h.registry.connectionSnapshotPooled(connectionIDs), method, payload)
}

// SendGroup sends one message to every active connection in group.
func (h *Handler) SendGroup(ctx context.Context, group string, method string, body any) SendResult {
	if h == nil {
		return SendResult{}
	}
	group = normalizeGroup(group)
	if group == "" {
		return SendResult{Err: ErrInvalidGroup}
	}
	payload, err := h.prepareBatchPayload(method, body)
	if err != nil {
		return SendResult{Err: err}
	}
	if h.shuttingDown.Load() {
		return SendResult{Err: ErrHandlerShuttingDown}
	}
	return h.sendConnections(ctx, h.registry.groupConnectionsPooled(group), method, payload)
}

// SendGroupRaw sends one message with an already-encoded payload to every active connection in group.
func (h *Handler) SendGroupRaw(ctx context.Context, group string, method string, payload []byte) SendResult {
	if h == nil {
		return SendResult{}
	}
	group = normalizeGroup(group)
	if group == "" {
		return SendResult{Err: ErrInvalidGroup}
	}
	if err := h.prepareBatchRawPayload(method, payload); err != nil {
		return SendResult{Err: err}
	}
	if h.shuttingDown.Load() {
		return SendResult{Err: ErrHandlerShuttingDown}
	}
	return h.sendConnections(ctx, h.registry.groupConnectionsPooled(group), method, payload)
}

// SendAll sends one message to every active connection.
func (h *Handler) SendAll(ctx context.Context, method string, body any) SendResult {
	if h == nil {
		return SendResult{}
	}
	payload, err := h.prepareBatchPayload(method, body)
	if err != nil {
		return SendResult{Err: err}
	}
	if h.shuttingDown.Load() {
		return SendResult{Err: ErrHandlerShuttingDown}
	}
	return h.sendConnections(ctx, h.registry.allConnectionsPooled(), method, payload)
}

// SendAllRaw sends one message with an already-encoded payload to every active connection.
func (h *Handler) SendAllRaw(ctx context.Context, method string, payload []byte) SendResult {
	if h == nil {
		return SendResult{}
	}
	if err := h.prepareBatchRawPayload(method, payload); err != nil {
		return SendResult{Err: err}
	}
	if h.shuttingDown.Load() {
		return SendResult{Err: ErrHandlerShuttingDown}
	}
	return h.sendConnections(ctx, h.registry.allConnectionsPooled(), method, payload)
}

// AddToGroup adds conn to group.
func (h *Handler) AddToGroup(conn *Connection, group string) error {
	if h == nil {
		return ErrConnectionNotFound
	}
	return h.registry.addToGroup(conn, group)
}

// RemoveFromGroup removes conn from group.
func (h *Handler) RemoveFromGroup(conn *Connection, group string) error {
	if h == nil {
		return ErrConnectionNotFound
	}
	return h.registry.removeFromGroup(conn, group)
}

// RemoveFromAllGroups removes conn from every group.
func (h *Handler) RemoveFromAllGroups(conn *Connection) {
	if h == nil {
		return
	}
	h.registry.removeFromAllGroups(conn)
}

// GroupOnline returns the current active websocket connection count for a group.
func (h *Handler) GroupOnline(group string) int {
	if h == nil {
		return 0
	}
	group = normalizeGroup(group)
	if group == "" {
		return 0
	}
	return h.registry.groupOnline(group)
}

// GroupConnections returns a snapshot of active websocket connections in a group.
func (h *Handler) GroupConnections(group string) []*Connection {
	if h == nil {
		return nil
	}
	group = normalizeGroup(group)
	if group == "" {
		return nil
	}
	return h.registry.groupConnections(group)
}

// Shutdown drains this handler by rejecting new websocket upgrades, closing all
// active websocket connections, and waiting until their handlers return.
func (h *Handler) Shutdown(ctx context.Context) error {
	if h == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	h.shutdownOnce.Do(func() {
		h.shutdownErr = h.shutdown(ctx)
	})
	return h.shutdownErr
}

func (h *Handler) shutdown(ctx context.Context) error {
	h.stopHeartbeatLoop()
	drainingConnections := h.beginShutdown()
	if err := h.waitConnectionsDrained(ctx, drainingConnections); err != nil {
		h.closeDrainingConnections(drainingConnections)
		return err
	}
	h.closeDrainingConnections(drainingConnections)

	done := make(chan struct{})
	go func() {
		h.active.Wait()
		close(done)
	}()

	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (h *Handler) acceptOptions() *websocket.AcceptOptions {
	opts := &websocket.AcceptOptions{
		Subprotocols:         h.cfg.Subprotocols,
		InsecureSkipVerify:   h.cfg.InsecureSkipVerify,
		OriginPatterns:       h.cfg.OriginPatterns,
		CompressionMode:      h.cfg.CompressionMode,
		CompressionThreshold: h.cfg.CompressionThreshold,
	}
	if h.cfg.CheckOrigin != nil {
		opts.InsecureSkipVerify = true
		opts.OriginPatterns = nil
	}
	return opts
}

func (h *Handler) resolveUserID(r *http.Request) (string, error) {
	if h.cfg.UserProvider == nil {
		return "", nil
	}
	return h.cfg.UserProvider.GetUserID(r)
}

func (h *Handler) addConnection(conn *Connection) bool {
	h.admissionMu.Lock()
	defer h.admissionMu.Unlock()

	if h.shuttingDown.Load() {
		return false
	}
	h.active.Add(1)
	h.registry.add(conn)
	h.online.Add(1)
	h.wakeHeartbeatLoop()
	return true
}

func (h *Handler) removeConnection(conn *Connection) {
	if !h.registry.remove(conn) {
		return
	}
	h.online.Add(-1)
	h.wakeHeartbeatLoop()
}

func (h *Handler) finishConnection() {
	h.active.Done()
}

func (h *Handler) beginShutdown() []drainingConnection {
	h.admissionMu.Lock()
	defer h.admissionMu.Unlock()

	h.shuttingDown.Store(true)
	snapshot := h.registry.allConnections()

	connections := make([]drainingConnection, 0, len(snapshot))
	for _, conn := range snapshot {
		connections = append(connections, drainingConnection{
			conn: conn,
			done: conn.beginDrain(),
		})
	}
	return connections
}

func (h *Handler) waitConnectionsDrained(ctx context.Context, connections []drainingConnection) error {
	for _, draining := range connections {
		select {
		case <-draining.done:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

func (h *Handler) closeDrainingConnections(connections []drainingConnection) {
	for _, draining := range connections {
		draining.conn.closeContext()
		_ = draining.conn.closeNow()
	}
}

func (h *Handler) runHandlerOperation(conn *Connection, fn func() error) error {
	if !conn.beginHandlerOperation() {
		return ErrHandlerShuttingDown
	}
	defer conn.endHandlerOperation()
	return fn()
}

func (h *Handler) sendConnected(conn *Connection) error {
	payload := newConnectedPayload(conn, h.cfg.PingInterval)
	body, err := connectedBody(payload, h.protocol.serialization())
	if err != nil {
		return err
	}
	return conn.Send(conn.ctx, ConnectedMethod, body)
}

func (h *Handler) heartbeatEnabled() bool {
	return h != nil && h.cfg.PingInterval > 0 && h.cfg.PingTimeout > 0
}

func (h *Handler) heartbeatLoop() {
	for {
		wait, ok := h.registry.nextExpiration(time.Now(), h.cfg.PingTimeout)
		if !ok {
			select {
			case <-h.heartbeatWake:
				continue
			case <-h.heartbeatStop:
				return
			}
		}

		timer := time.NewTimer(wait)
		select {
		case <-timer.C:
			h.closeExpiredIdleConnections()
		case <-h.heartbeatWake:
			stopTimer(timer)
		case <-h.heartbeatStop:
			stopTimer(timer)
			return
		}
	}
}

func stopTimer(timer *time.Timer) {
	if timer == nil || timer.Stop() {
		return
	}
	select {
	case <-timer.C:
	default:
	}
}

func (h *Handler) closeExpiredIdleConnections() {
	expired := h.registry.expired(time.Now(), h.cfg.PingTimeout)
	if len(expired) == 0 {
		return
	}
	h.online.Add(-int64(len(expired)))
	for _, conn := range expired {
		h.logger.Warn("websocket connection heartbeat timed out",
			slog.String("connection_id", conn.ID),
			slog.String("remote_addr", remoteAddr(conn)),
			slog.Duration("ping_timeout", h.cfg.PingTimeout),
		)
		conn.closeContext()
		_ = conn.closeNow()
	}
	h.wakeHeartbeatLoop()
}

func (h *Handler) wakeHeartbeatLoop() {
	if !h.heartbeatEnabled() {
		return
	}
	select {
	case h.heartbeatWake <- struct{}{}:
	default:
	}
}

func (h *Handler) stopHeartbeatLoop() {
	h.heartbeatStopOnce.Do(func() {
		close(h.heartbeatStop)
	})
}

func (h *Handler) readLoop(conn *Connection, hub Hub) error {
	messageHub, receivesMessages := hub.(MessageHub)
	var dispatcher *hubDispatcher
	if !receivesMessages {
		var err error
		dispatcher, err = dispatcherFor(hub)
		if err != nil {
			return err
		}
	}
	for {
		typ, rd, err := conn.ws.Reader(conn.ctx)
		if err != nil {
			return err
		}

		if typ != websocket.MessageBinary {
			err = ErrInvalidMessageType
			_ = conn.CloseWithStatus(websocket.StatusUnsupportedData, err.Error())
			return err
		}

		err = func() error {
			b := bpool.Get()
			defer bpool.Put(b)

			_, err = b.ReadFrom(rd)
			if err != nil {
				return err
			}

			frame := b.Bytes()

			msg, err := h.protocol.decodeFrame(frame)
			if err != nil {
				_ = conn.CloseWithStatus(websocket.StatusProtocolError, "invalid signalg protocol frame")
				return err
			}
			h.registry.touch(conn)
			if msg.Kind == FrameKindPing {
				return h.handleProtocolPing(conn, hub)
			}
			if msg.Kind == FrameKindPong {
				return nil
			}
			if receivesMessages {
				return h.runHandlerOperation(conn, func() error {
					return messageHub.OnMessage(conn.ctx, conn, msg)
				})
			}
			return h.runHandlerOperation(conn, func() error {
				return h.dispatchMessage(conn, hub, dispatcher, msg)
			})
		}()
		if err != nil {
			return err
		}
	}
}

func (h *Handler) handleProtocolPing(conn *Connection, hub Hub) error {
	if err := conn.Pong(conn.ctx); err != nil {
		return err
	}
	if pingHub, ok := hub.(PingHub); ok {
		_ = h.runHandlerOperation(conn, func() error {
			pingHub.OnPing(conn.ctx, conn)
			return nil
		})
	}
	return nil
}

func (h *Handler) dispatchMessage(conn *Connection, hub Hub, dispatcher *hubDispatcher, msg Message) error {
	switch msg.Kind {
	case FrameKindMessage:
		_, err := dispatcher.dispatch(conn.ctx, hub, msg)
		return err
	case FrameKindInvoke:
		res, err := dispatcher.dispatch(conn.ctx, hub, msg)
		if err != nil {
			return conn.CompleteError(conn.ctx, msg.InvocationID, err)
		}
		return conn.Complete(conn.ctx, msg.InvocationID, res)
	default:
		return fmt.Errorf("%w: %s", ErrInvalidFrameKind, msg.Kind)
	}
}

// NewServer creates a standalone SignalG websocket server.
func NewServer(cfg ServerConfig, factory HubFactory) (*Server, error) {
	cfg.Addr = strings.TrimSpace(cfg.Addr)
	if cfg.Addr == "" {
		return nil, ErrMissingAddr
	}

	handler, err := NewHandler(cfg.Config, factory)
	if err != nil {
		return nil, err
	}

	s := &Server{
		cfg:     cfg,
		handler: handler,
	}
	s.server = &http.Server{
		Addr:              cfg.Addr,
		Handler:           handler,
		ReadHeaderTimeout: cfg.ReadHeaderTimeout,
	}
	return s, nil
}

// Handler returns the underlying HTTP handler.
func (s *Server) Handler() http.Handler {
	return s.handler
}

// Start starts the standalone HTTP server in the background.
func (s *Server) Start() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.listener != nil {
		return nil
	}

	listener, err := net.Listen("tcp", s.cfg.Addr)
	if err != nil {
		s.handler.logger.Error("failed to listen signalg websocket server",
			slog.String("addr", s.cfg.Addr),
			slog.Any("error", err),
		)
		return err
	}
	s.listener = listener

	s.handler.logger.Info("signalg websocket server started",
		slog.String("addr", listener.Addr().String()),
		slog.String("path", s.handler.cfg.Path),
	)

	go func() {
		if err := s.server.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			s.handler.logger.Error("signalg websocket server stopped with error",
				slog.String("addr", listener.Addr().String()),
				slog.Any("error", err),
			)
		}
	}()
	return nil
}

// Shutdown gracefully stops the HTTP server and active websocket connections.
func (s *Server) Shutdown(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	s.shutdownOnce.Do(func() {
		s.handler.logger.Info("shutting down signalg websocket server",
			slog.String("addr", s.cfg.Addr),
		)

		serverDone := make(chan error, 1)
		go func() {
			serverDone <- s.server.Shutdown(ctx)
		}()

		handlerErr := s.handler.Shutdown(ctx)
		serverErr := <-serverDone
		if handlerErr != nil || serverErr != nil {
			s.shutdownErr = errors.Join(handlerErr, serverErr)
			s.handler.logger.Error("failed to drain signalg websocket handler",
				slog.String("addr", s.cfg.Addr),
				slog.Any("handler_error", handlerErr),
				slog.Any("server_error", serverErr),
			)
			return
		}
		s.handler.logger.Info("signalg websocket server shut down",
			slog.String("addr", s.cfg.Addr),
		)
	})
	return s.shutdownErr
}

// Online returns the current active websocket connection count.
func (s *Server) Online() int {
	return s.handler.Online()
}

// UserOnline returns the current active websocket connection count for a user.
func (s *Server) UserOnline(userID string) int {
	return s.handler.UserOnline(userID)
}

// UserConnections returns a snapshot of active websocket connections for a user.
func (s *Server) UserConnections(userID string) []*Connection {
	return s.handler.UserConnections(userID)
}

// SendUsers sends one message to every active connection for the provided users.
func (s *Server) SendUsers(ctx context.Context, userIDs []string, method string, body any) SendResult {
	return s.handler.SendUsers(ctx, userIDs, method, body)
}

// SendUsersRaw sends one message with an already-encoded payload to every active connection for the provided users.
func (s *Server) SendUsersRaw(ctx context.Context, userIDs []string, method string, payload []byte) SendResult {
	return s.handler.SendUsersRaw(ctx, userIDs, method, payload)
}

// SendConnectionsRaw sends one message with an already-encoded payload to every active connection for the provided connection IDs.
func (s *Server) SendConnectionsRaw(ctx context.Context, connectionIDs []string, method string, payload []byte) SendResult {
	return s.handler.SendConnectionsRaw(ctx, connectionIDs, method, payload)
}

// CloseUsers immediately closes every active connection for the provided users.
func (s *Server) CloseUsers(ctx context.Context, userIDs []string) CloseResult {
	return s.handler.CloseUsers(ctx, userIDs)
}

// CloseConnections immediately closes active connections for the provided connection IDs.
func (s *Server) CloseConnections(ctx context.Context, connectionIDs []string) CloseResult {
	return s.handler.CloseConnections(ctx, connectionIDs)
}

// SendGroup sends one message to every active connection in group.
func (s *Server) SendGroup(ctx context.Context, group string, method string, body any) SendResult {
	return s.handler.SendGroup(ctx, group, method, body)
}

// SendGroupRaw sends one message with an already-encoded payload to every active connection in group.
func (s *Server) SendGroupRaw(ctx context.Context, group string, method string, payload []byte) SendResult {
	return s.handler.SendGroupRaw(ctx, group, method, payload)
}

// SendAll sends one message to every active connection.
func (s *Server) SendAll(ctx context.Context, method string, body any) SendResult {
	return s.handler.SendAll(ctx, method, body)
}

// SendAllRaw sends one message with an already-encoded payload to every active connection.
func (s *Server) SendAllRaw(ctx context.Context, method string, payload []byte) SendResult {
	return s.handler.SendAllRaw(ctx, method, payload)
}

// AddToGroup adds conn to group.
func (s *Server) AddToGroup(conn *Connection, group string) error {
	return s.handler.AddToGroup(conn, group)
}

// RemoveFromGroup removes conn from group.
func (s *Server) RemoveFromGroup(conn *Connection, group string) error {
	return s.handler.RemoveFromGroup(conn, group)
}

// RemoveFromAllGroups removes conn from every group.
func (s *Server) RemoveFromAllGroups(conn *Connection) {
	s.handler.RemoveFromAllGroups(conn)
}

// GroupOnline returns the current active websocket connection count for a group.
func (s *Server) GroupOnline(group string) int {
	return s.handler.GroupOnline(group)
}

// GroupConnections returns a snapshot of active websocket connections in a group.
func (s *Server) GroupConnections(group string) []*Connection {
	return s.handler.GroupConnections(group)
}

func normalizePath(p string) string {
	p = strings.TrimSpace(p)
	if p == "" {
		return DefaultPath
	}
	if !strings.HasPrefix(p, "/") {
		p = "/" + p
	}
	return path.Clean(p)
}

func normalizePingInterval(d time.Duration) time.Duration {
	if d < 0 {
		return DefaultPingInterval
	}
	return d
}

func normalizePingTimeout(timeout, interval time.Duration) time.Duration {
	if interval <= 0 {
		return 0
	}
	if timeout <= 0 {
		return 5 * interval
	}
	return timeout
}

func normalizeSendQueueSize(n int) int {
	if n <= 0 {
		return DefaultSendQueueSize
	}
	return n
}

func normalizeWriteTimeout(d time.Duration) time.Duration {
	if d <= 0 {
		return DefaultWriteTimeout
	}
	return d
}

func nextConnectionID() (string, error) {
	return gonanoid.New(16)
}

func remoteAddr(conn *Connection) string {
	addr := conn.RemoteAddr()
	if addr == nil {
		return ""
	}
	return addr.String()
}
