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
)

var (
	ErrMissingAddr         = errors.New("signalg: server addr is required")
	ErrNilHubFactory       = errors.New("signalg: hub factory is required")
	ErrNilHub              = errors.New("signalg: hub factory returned nil hub")
	ErrHandlerShuttingDown = errors.New("signalg: handler is shutting down")
	ErrConnectionNotFound  = errors.New("signalg: connection is not managed by handler")
	ErrInvalidGroup        = errors.New("signalg: invalid group")
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
}

// Handler accepts websocket requests and dispatches lifecycle events to hubs.
type Handler struct {
	cfg      Config
	factory  HubFactory
	logger   *slog.Logger
	protocol *protocolConfig

	registry        *connectionRegistry
	sendConcurrency int
	active          sync.WaitGroup
	shuttingDown    atomic.Bool
	online          atomic.Int64
	shutdownOnce    sync.Once
	shutdownErr     error
}

// SendResult describes a batch send operation outcome.
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
	sendConcurrency := normalizeSendConcurrency(cfg.SendConcurrency)

	return &Handler{
		cfg:             cfg,
		factory:         factory,
		logger:          cfg.Logger,
		protocol:        protocol,
		registry:        newConnectionRegistry(),
		sendConcurrency: sendConcurrency,
	}, nil
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

	conn := newConnection(connectionID, userID, r, ws, h.protocol)
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
	return h.sendConnections(ctx, h.registry.userSnapshot(userIDs), method, payload)
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
	return h.sendConnections(ctx, h.registry.groupConnections(group), method, payload)
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
	return h.sendConnections(ctx, h.registry.allConnections(), method, payload)
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
	h.registry.mu.Lock()
	if h.shuttingDown.Load() {
		h.registry.mu.Unlock()
		return false
	}
	h.active.Add(1)
	h.registry.addLocked(conn)
	h.registry.mu.Unlock()
	h.online.Add(1)
	return true
}

func (h *Handler) removeConnection(conn *Connection) {
	if !h.registry.remove(conn) {
		return
	}
	h.online.Add(-1)
}

func (h *Handler) finishConnection() {
	h.active.Done()
}

func (h *Handler) beginShutdown() []drainingConnection {
	h.registry.mu.Lock()
	h.shuttingDown.Store(true)
	snapshot := h.registry.allConnectionsLocked()
	h.registry.mu.Unlock()

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
	if len(connections) == 0 {
		return nil
	}
	var wg sync.WaitGroup
	wg.Add(len(connections))
	for _, draining := range connections {
		go func(done <-chan struct{}) {
			defer wg.Done()
			<-done
		}(draining.done)
	}

	drained := make(chan struct{})
	go func() {
		wg.Wait()
		close(drained)
	}()

	select {
	case <-drained:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
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

// SendGroup sends one message to every active connection in group.
func (s *Server) SendGroup(ctx context.Context, group string, method string, body any) SendResult {
	return s.handler.SendGroup(ctx, group, method, body)
}

// SendAll sends one message to every active connection.
func (s *Server) SendAll(ctx context.Context, method string, body any) SendResult {
	return s.handler.SendAll(ctx, method, body)
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
