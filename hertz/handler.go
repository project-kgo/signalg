package hertz

import (
	"net/http"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/common/adaptor"
	"github.com/project-kgo/signalg"
)

// Handler converts a SignalG websocket handler to a Hertz handler.
func Handler(cfg signalg.Config, factory signalg.HubFactory) (app.HandlerFunc, error) {
	handler, err := signalg.NewHandler(cfg, factory)
	if err != nil {
		return nil, err
	}
	return adaptor.HertzHandler(http.Handler(handler)), nil
}

// MustHandler is like Handler but panics when the handler cannot be created.
func MustHandler(cfg signalg.Config, factory signalg.HubFactory) app.HandlerFunc {
	handler, err := Handler(cfg, factory)
	if err != nil {
		panic(err)
	}
	return handler
}
