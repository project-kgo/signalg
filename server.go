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
)

var (
	ErrMissingAddr   = errors.New("signalg: server addr is required")
	ErrNilHubFactory = errors.New("signalg: hub factory is required")
	ErrNilHub        = errors.New("signalg: hub factory returned nil hub")
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
}

// Handler accepts websocket requests and dispatches lifecycle events to hubs.
type Handler struct {
	cfg      Config
	factory  HubFactory
	logger   *slog.Logger
	protocol *protocolConfig

	mu            sync.RWMutex
	connections   map[*Connection]struct{}
	userConnIndex map[string]map[*Connection]struct{}
	online        atomic.Int64
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

	return &Handler{
		cfg:           cfg,
		factory:       factory,
		logger:        cfg.Logger,
		protocol:      protocol,
		connections:   make(map[*Connection]struct{}),
		userConnIndex: make(map[string]map[*Connection]struct{}),
	}, nil
}

// ServeHTTP handles websocket upgrade requests.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL == nil || r.URL.Path != h.cfg.Path {
		http.NotFound(w, r)
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

	h.addConnection(conn)
	defer h.removeConnection(conn)
	defer conn.closeContext()

	if err = hub.OnConnected(conn.ctx, conn); err != nil {
		h.logger.Error("hub connected callback failed",
			slog.String("connection_id", conn.ID),
			slog.String("remote_addr", remoteAddr(conn)),
			slog.Any("error", err),
		)
		hub.OnDisconnected(conn.ctx, conn, err)
		_ = ws.CloseNow()
		return
	}

	h.logger.Info("websocket connection opened",
		slog.String("connection_id", conn.ID),
		slog.String("remote_addr", remoteAddr(conn)),
	)

	err = h.readLoop(conn, hub)
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
	if userID == "" {
		return 0
	}
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.userConnIndex[userID])
}

// UserConnections returns a snapshot of active websocket connections for a user.
func (h *Handler) UserConnections(userID string) []*Connection {
	if userID == "" {
		return nil
	}

	h.mu.RLock()
	defer h.mu.RUnlock()

	userConnections := h.userConnIndex[userID]
	connections := make([]*Connection, 0, len(userConnections))
	for conn := range userConnections {
		connections = append(connections, conn)
	}
	return connections
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

func (h *Handler) addConnection(conn *Connection) {
	h.mu.Lock()
	h.connections[conn] = struct{}{}
	if conn.UserID != "" {
		userConnections := h.userConnIndex[conn.UserID]
		if userConnections == nil {
			userConnections = make(map[*Connection]struct{})
			h.userConnIndex[conn.UserID] = userConnections
		}
		userConnections[conn] = struct{}{}
	}
	h.mu.Unlock()
	h.online.Add(1)
}

func (h *Handler) removeConnection(conn *Connection) {
	h.mu.Lock()
	delete(h.connections, conn)
	if conn.UserID != "" {
		userConnections := h.userConnIndex[conn.UserID]
		delete(userConnections, conn)
		if len(userConnections) == 0 {
			delete(h.userConnIndex, conn.UserID)
		}
	}
	h.mu.Unlock()
	h.online.Add(-1)
}

func (h *Handler) closeActive(_ websocket.StatusCode, _ string) {
	h.mu.RLock()
	connections := make([]*Connection, 0, len(h.connections))
	for conn := range h.connections {
		connections = append(connections, conn)
	}
	h.mu.RUnlock()

	for _, conn := range connections {
		_ = conn.closeNow()
	}
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

			_, err := b.ReadFrom(rd)
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
				if err = messageHub.OnMessage(conn.ctx, conn, msg); err != nil {
					return err
				}
			} else if err = h.dispatchMessage(conn, hub, dispatcher, msg); err != nil {
				return err
			}
			return nil
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
	s.shutdownOnce.Do(func() {
		s.handler.logger.Info("shutting down signalg websocket server",
			slog.String("addr", s.cfg.Addr),
		)
		s.handler.closeActive(websocket.StatusGoingAway, "server shutdown")
		if err := s.server.Shutdown(ctx); err != nil {
			s.shutdownErr = err
			s.handler.logger.Error("failed to shut down signalg websocket server",
				slog.String("addr", s.cfg.Addr),
				slog.Any("error", err),
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
