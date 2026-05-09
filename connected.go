package signalg

import (
	"time"

	signalgv1 "github.com/project-kgo/signalg/proto/signalg/v1"
)

const (
	// ConnectedMethod is sent once after a websocket connection is accepted.
	ConnectedMethod = "server.connected"
)

// ConnectedPayload describes the server-side connection state and client
// configuration sent immediately after a websocket connection is accepted.
type ConnectedPayload struct {
	ConnectionID string          `json:"connection_id" msgpack:"connection_id"`
	UserID       string          `json:"user_id,omitempty" msgpack:"user_id,omitempty"`
	RemoteAddr   string          `json:"remote_addr,omitempty" msgpack:"remote_addr,omitempty"`
	Config       ConnectedConfig `json:"config" msgpack:"config"`
}

// ConnectedConfig contains client-facing connection configuration.
type ConnectedConfig struct {
	PingIntervalMilliseconds int64 `json:"ping_interval_ms" msgpack:"ping_interval_ms"`
}

func newConnectedPayload(conn *Connection, pingInterval time.Duration) ConnectedPayload {
	if pingInterval < 0 {
		pingInterval = 0
	}
	return ConnectedPayload{
		ConnectionID: conn.ID,
		UserID:       conn.UserID,
		RemoteAddr:   remoteAddr(conn),
		Config: ConnectedConfig{
			PingIntervalMilliseconds: pingInterval.Milliseconds(),
		},
	}
}

func connectedBody(payload ConnectedPayload, serialization Serialization) (any, error) {
	if serialization != SerializationProtobuf {
		return payload, nil
	}
	return &signalgv1.ConnectedPayload{
		ConnectionId: payload.ConnectionID,
		UserId:       payload.UserID,
		RemoteAddr:   payload.RemoteAddr,
		Config: &signalgv1.ConnectedConfig{
			PingIntervalMs: payload.Config.PingIntervalMilliseconds,
		},
	}, nil
}
