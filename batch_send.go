package signalg

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

const maxSendErrorSamples = 4

func normalizeSendConcurrency(n int) int {
	if n <= 0 {
		return DefaultSendConcurrency
	}
	return n
}

func (h *Handler) prepareBatchPayload(method string, body any) ([]byte, error) {
	if h.protocol == nil {
		return nil, ErrUnsupportedCodec
	}
	if err := validateProtocolFrame(FrameKindMessage, method, ""); err != nil {
		return nil, err
	}

	payload, err := h.protocol.marshalAppend(nil, body)
	if err != nil {
		return nil, err
	}
	if err := h.protocol.ensurePayloadSize(len(payload)); err != nil {
		return nil, err
	}
	return payload, nil
}

func (h *Handler) prepareBatchRawPayload(method string, payload []byte) error {
	if h.protocol == nil {
		return ErrUnsupportedCodec
	}
	if err := validateProtocolFrame(FrameKindMessage, method, ""); err != nil {
		return err
	}
	return h.protocol.ensurePayloadSize(len(payload))
}

func (h *Handler) sendConnections(ctx context.Context, connections []*Connection, method string, payload []byte) SendResult {
	if ctx == nil {
		ctx = context.Background()
	}
	result := SendResult{Matched: len(connections)}
	if len(connections) == 0 {
		return result
	}

	workers := h.sendConcurrency
	if workers > len(connections) {
		workers = len(connections)
	}

	jobs := make(chan *Connection)
	results := make(chan error, workers)

	var wg sync.WaitGroup
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go func() {
			defer wg.Done()
			for conn := range jobs {
				results <- conn.SendRaw(ctx, method, payload)
			}
		}()
	}

	go func() {
		for _, conn := range connections {
			jobs <- conn
		}
		close(jobs)
		wg.Wait()
		close(results)
	}()

	var samples []error
	for err := range results {
		if err == nil {
			result.Sent++
			continue
		}
		result.Failed++
		if len(samples) < maxSendErrorSamples {
			samples = append(samples, err)
		}
	}
	result.Err = buildSendError(result.Failed, samples)
	return result
}

func buildSendError(failed int, samples []error) error {
	if failed == 0 {
		return nil
	}
	joined := errors.Join(samples...)
	if joined == nil {
		return fmt.Errorf("signalg: failed to send %d connection(s)", failed)
	}
	return fmt.Errorf("signalg: failed to send %d connection(s): %w", failed, joined)
}
