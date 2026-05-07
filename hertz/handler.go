package hertz

import (
	"context"
	"net/http"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/common/adaptor"
	"github.com/project-kgo/signalg"
)

// ManagedHandler keeps the underlying SignalG handler available for lifecycle
// operations while exposing a Hertz-compatible handler function.
type ManagedHandler struct {
	handler     *signalg.Handler
	handlerFunc app.HandlerFunc
}

// NewHandler creates a Hertz handler with an explicit shutdown handle.
func NewHandler(cfg signalg.Config, factory signalg.HubFactory) (*ManagedHandler, error) {
	handler, err := signalg.NewHandler(cfg, factory)
	if err != nil {
		return nil, err
	}
	return newManagedHandler(handler), nil
}

// MustNewHandler is like NewHandler but panics when the handler cannot be created.
func MustNewHandler(cfg signalg.Config, factory signalg.HubFactory) *ManagedHandler {
	handler, err := NewHandler(cfg, factory)
	if err != nil {
		panic(err)
	}
	return handler
}

// Handler converts a SignalG websocket handler to a Hertz handler.
func Handler(cfg signalg.Config, factory signalg.HubFactory) (app.HandlerFunc, error) {
	handler, err := NewHandler(cfg, factory)
	if err != nil {
		return nil, err
	}
	return handler.HandlerFunc(), nil
}

// MustHandler is like Handler but panics when the handler cannot be created.
func MustHandler(cfg signalg.Config, factory signalg.HubFactory) app.HandlerFunc {
	handler, err := Handler(cfg, factory)
	if err != nil {
		panic(err)
	}
	return handler
}

// HandlerFunc returns the Hertz-compatible handler function.
func (h *ManagedHandler) HandlerFunc() app.HandlerFunc {
	if h == nil {
		return nil
	}
	return h.handlerFunc
}

// Handle serves one Hertz request. It is a convenience wrapper around
// HandlerFunc for route registration.
func (h *ManagedHandler) Handle(ctx context.Context, c *app.RequestContext) {
	h.handlerFunc(ctx, c)
}

// SignalGHandler returns the underlying SignalG HTTP handler.
func (h *ManagedHandler) SignalGHandler() *signalg.Handler {
	if h == nil {
		return nil
	}
	return h.handler
}

// Shutdown drains active websocket connections managed by this handler.
func (h *ManagedHandler) Shutdown(ctx context.Context) error {
	if h == nil || h.handler == nil {
		return nil
	}
	return h.handler.Shutdown(ctx)
}

func newManagedHandler(handler *signalg.Handler) *ManagedHandler {
	return &ManagedHandler{
		handler:     handler,
		handlerFunc: adaptor.HertzHandler(http.Handler(handler)),
	}
}
