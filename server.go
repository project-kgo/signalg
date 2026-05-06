package signalg

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"path"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coder/websocket"
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

var connectionID uint64

// Hub handles lifecycle events for one websocket connection.
type Hub interface {
	OnConnected(ctx context.Context, conn *Connection) error
	OnDisconnected(ctx context.Context, conn *Connection, err error)
}

// HubFactory creates one Hub instance for one websocket connection.
type HubFactory func(conn *Connection) (Hub, error)

// Config configures the SignalG websocket handler.
type Config struct {
	Path   string
	Logger *slog.Logger

	CheckOrigin          func(*http.Request) bool
	OriginPatterns       []string
	InsecureSkipVerify   bool
	Subprotocols         []string
	CompressionMode      websocket.CompressionMode
	CompressionThreshold int
	ReadLimit            int64
}

// Handler accepts websocket requests and dispatches lifecycle events to hubs.
type Handler struct {
	cfg     Config
	factory HubFactory
	logger  *slog.Logger

	mu          sync.Mutex
	connections map[*Connection]struct{}
	online      atomic.Int64
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

	return &Handler{
		cfg:         cfg,
		factory:     factory,
		logger:      cfg.Logger,
		connections: make(map[*Connection]struct{}),
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

	conn := newConnection(nextConnectionID(), r, ws)
	if h.cfg.ReadLimit > 0 {
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

	err = h.readLoop(conn)
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

func (h *Handler) addConnection(conn *Connection) {
	h.mu.Lock()
	h.connections[conn] = struct{}{}
	h.mu.Unlock()
	h.online.Add(1)
}

func (h *Handler) removeConnection(conn *Connection) {
	h.mu.Lock()
	delete(h.connections, conn)
	h.mu.Unlock()
	h.online.Add(-1)
}

func (h *Handler) closeActive(_ websocket.StatusCode, _ string) {
	h.mu.Lock()
	connections := make([]*Connection, 0, len(h.connections))
	for conn := range h.connections {
		connections = append(connections, conn)
	}
	h.mu.Unlock()

	for _, conn := range connections {
		_ = conn.closeNow()
	}
}

func (h *Handler) readLoop(conn *Connection) error {
	for {
		_, _, err := conn.ws.Read(conn.ctx)
		if err != nil {
			return err
		}
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

func nextConnectionID() string {
	return strconv.FormatUint(atomic.AddUint64(&connectionID, 1), 36)
}

func remoteAddr(conn *Connection) string {
	addr := conn.RemoteAddr()
	if addr == nil {
		return ""
	}
	return addr.String()
}
