package signalg

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

// CloseResult describes a batch close operation outcome.
type CloseResult struct {
	Matched int
	Closed  int
	Failed  int
	Err     error
}

// CloseUsers immediately closes every active connection for the provided users.
func (h *Handler) CloseUsers(ctx context.Context, userIDs []string) CloseResult {
	if h == nil {
		return CloseResult{}
	}
	if h.shuttingDown.Load() {
		return CloseResult{Err: ErrHandlerShuttingDown}
	}
	return h.closeConnections(ctx, h.registry.userSnapshotPooled(userIDs))
}

// CloseConnections immediately closes active connections for the provided connection IDs.
func (h *Handler) CloseConnections(ctx context.Context, connectionIDs []string) CloseResult {
	if h == nil {
		return CloseResult{}
	}
	if h.shuttingDown.Load() {
		return CloseResult{Err: ErrHandlerShuttingDown}
	}
	return h.closeConnections(ctx, h.registry.connectionSnapshotPooled(connectionIDs))
}

func (h *Handler) closeConnections(ctx context.Context, snapshot pooledConnections) CloseResult {
	if ctx == nil {
		ctx = context.Background()
	}
	defer snapshot.release()

	connections := snapshot.connections
	result := CloseResult{Matched: len(connections)}
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
				select {
				case <-ctx.Done():
					results <- ctx.Err()
				default:
					conn.closeContext()
					results <- conn.closeNow()
				}
			}
		}()
	}

	go func() {
		for _, conn := range connections {
			select {
			case <-ctx.Done():
				results <- ctx.Err()
			case jobs <- conn:
			}
		}
		close(jobs)
		wg.Wait()
		close(results)
	}()

	var samples []error
	for err := range results {
		if err == nil {
			result.Closed++
			continue
		}
		result.Failed++
		if len(samples) < maxSendErrorSamples {
			samples = append(samples, err)
		}
	}
	result.Err = buildCloseError(result.Failed, samples)
	return result
}

func buildCloseError(failed int, samples []error) error {
	return buildBatchError("close", failed, samples)
}

func buildBatchError(operation string, failed int, samples []error) error {
	if failed == 0 {
		return nil
	}
	joined := errors.Join(samples...)
	if joined == nil {
		return fmt.Errorf("signalg: failed to %s %d connection(s)", operation, failed)
	}
	return fmt.Errorf("signalg: failed to %s %d connection(s): %w", operation, failed, joined)
}
