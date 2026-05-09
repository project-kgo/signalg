package main

import (
	"context"
	"errors"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/cloudwego/hertz/pkg/app"
	hertzserver "github.com/cloudwego/hertz/pkg/app/server"
	"github.com/cloudwego/hertz/pkg/common/utils"
	"github.com/cloudwego/hertz/pkg/protocol/consts"
	"github.com/project-kgo/signalg"
	signalgHz "github.com/project-kgo/signalg/hertz"
)

type debugHub struct {
	logger *slog.Logger
}

type debugPayload struct {
	ConnectionID string `json:"connection_id" msgpack:"connection_id"`
	UserID       string `json:"user_id,omitempty" msgpack:"user_id,omitempty"`
	RemoteAddr   string `json:"remote_addr,omitempty" msgpack:"remote_addr,omitempty"`
	Method       string `json:"method,omitempty" msgpack:"method,omitempty"`
	Kind         string `json:"kind,omitempty" msgpack:"kind,omitempty"`
	Body         any    `json:"body,omitempty" msgpack:"body,omitempty"`
	Time         string `json:"time" msgpack:"time"`
}

func (h *debugHub) OnConnected(ctx context.Context, conn *signalg.Connection) error {
	h.logger.Info("websocket connected",
		slog.String("connection_id", conn.ID),
		slog.String("user_id", conn.UserID),
		slog.String("remote_addr", remoteAddr(conn)),
	)
	return nil
}

func (h *debugHub) OnDisconnected(_ context.Context, conn *signalg.Connection, err error) {
	attrs := []slog.Attr{
		slog.String("connection_id", conn.ID),
		slog.String("user_id", conn.UserID),
		slog.String("remote_addr", remoteAddr(conn)),
	}
	if err != nil {
		attrs = append(attrs, slog.Any("error", err))
	}
	h.logger.LogAttrs(context.Background(), slog.LevelInfo, "websocket disconnected", attrs...)
}

func (h *debugHub) OnMessage(ctx context.Context, conn *signalg.Connection, msg signalg.Message) error {
	var body any
	if err := msg.Decode(&body); err != nil {
		h.logger.Warn("failed to decode websocket message",
			slog.String("connection_id", conn.ID),
			slog.String("method", msg.Method),
			slog.String("kind", msg.Kind.String()),
			slog.Any("error", err),
		)
		if msg.Kind == signalg.FrameKindInvoke {
			return conn.CompleteError(ctx, msg.InvocationID, err)
		}
		return nil
	}

	payload := debugPayload{
		ConnectionID: conn.ID,
		UserID:       conn.UserID,
		RemoteAddr:   remoteAddr(conn),
		Method:       msg.Method,
		Kind:         msg.Kind.String(),
		Body:         body,
		Time:         time.Now().Format(time.RFC3339Nano),
	}

	h.logger.Info("websocket message received",
		slog.String("connection_id", conn.ID),
		slog.String("method", msg.Method),
		slog.String("kind", msg.Kind.String()),
		slog.String("invocation_id", msg.InvocationID),
		slog.Any("body", body),
	)

	switch msg.Kind {
	case signalg.FrameKindMessage:
		return conn.Send(ctx, "server.echo", payload)
	case signalg.FrameKindInvoke:
		return conn.Complete(ctx, msg.InvocationID, payload)
	default:
		return errors.New("unsupported client frame kind: " + msg.Kind.String())
	}
}

func main() {
	addr := flag.String("addr", "[::]:8888", "hertz listen address")
	path := flag.String("path", signalg.DefaultPath, "websocket path")
	pingInterval := flag.Duration("ping-interval", 15*time.Second, "client ping interval")
	allowAllOrigins := flag.Bool("allow-all-origins", true, "allow websocket requests from any Origin")
	flag.Parse()

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))

	wsHandler, err := signalgHz.NewHandler(signalg.Config{
		Path:               *path,
		Logger:             logger,
		Serialization:      signalg.SerializationJSON,
		PingInterval:       *pingInterval,
		InsecureSkipVerify: *allowAllOrigins,
		UserProvider: signalg.UserProviderFunc(func(r *http.Request) (string, error) {
			return "1000", nil
		}),
	}, func(*signalg.Connection) (signalg.Hub, error) {
		return &debugHub{logger: logger}, nil
	})
	if err != nil {
		logger.Error("failed to create signalg hertz handler", slog.Any("error", err))
		os.Exit(1)
	}

	h := hertzserver.Default(hertzserver.WithHostPorts(*addr))
	h.GET("/healthz", func(ctx context.Context, c *app.RequestContext) {
		c.JSON(consts.StatusOK, utils.H{
			"status": "ok",
			"path":   *path,
		})
	})
	h.GET(*path, wsHandler.Handle)
	h.OnShutdown = append(h.OnShutdown, func(ctx context.Context) {
		if err := wsHandler.Shutdown(ctx); err != nil {
			logger.Error("failed to shutdown signalg handler", slog.Any("error", err))
		}
	})

	logger.Info("signalg hertz example started",
		slog.String("http", "http://"+*addr),
		slog.String("websocket", "ws://"+*addr+*path),
		slog.String("serialization", signalg.SerializationJSON.String()),
		slog.Duration("ping_interval", *pingInterval),
	)
	h.Spin()
}

func remoteAddr(conn *signalg.Connection) string {
	if conn == nil || conn.RemoteAddr() == nil {
		return ""
	}
	return conn.RemoteAddr().String()
}
