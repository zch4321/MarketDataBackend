// Package runtime — snapshotInputWorker is a lightweight Kafka consumer for the
// orderbook_snapshot input stream. Unlike inputWorker it does not batch or write
// to the fact tables.  Each decoded snapshot is delivered to the group worker's
// orderBookState through a channel, and the offset is committed synchronously.
package runtime

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"MarketDataBackend/internal/kafka"
	"MarketDataBackend/internal/model"
)

// snapshotResult holds a decoded reference snapshot for one Kafka message.
// The done channel is closed by the group worker after the snapshot has been
// successfully applied to the orderBookState. The input worker must not commit
// the Kafka offset until done is closed — committing earlier risks losing a
// reference snapshot on process restart.
type snapshotResult struct {
	snapshot model.OrderBookSnapshot
	err      error
	offset   int64
	done     chan struct{}
}

// snapshotInputWorker consumes the orderbook_snapshot Kafka topic, decodes every
// message and pushes the result onto a channel owned by the MarketGroupWorker.
// Offsets are committed after the group worker confirms the snapshot was
// successfully applied to the orderBookState.
type snapshotInputWorker struct {
	group    model.MarketGroup
	input    model.GroupInput
	consumer kafka.Consumer
	out      chan<- snapshotResult
	nodeID   string
	logger   *slog.Logger

	mu        sync.Mutex
	status    string
	lastErr   string
	committed *int64
	lastEvent *time.Time
	lastProc  *time.Time
	done      chan struct{}
}

func newSnapshotInputWorker(
	group model.MarketGroup, in model.GroupInput, consumer kafka.Consumer,
	out chan<- snapshotResult, nodeID string, logger *slog.Logger,
) *snapshotInputWorker {
	return &snapshotInputWorker{
		group:    group,
		input:    in,
		consumer: consumer,
		out:      out,
		nodeID:   nodeID,
		logger:   logger,
		status:   model.ActualStatusRunning,
		done:     make(chan struct{}),
	}
}

// run drives the consume loop until ctx is canceled. Offsets are committed
// synchronously — the group worker is expected to drain the out channel and
// signal success by returning from the receive.
func (sw *snapshotInputWorker) run(ctx context.Context) {
	defer close(sw.done)
	for {
		msg, err := sw.consumer.Fetch(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			if errors.Is(err, context.DeadlineExceeded) {
				continue
			}
			sw.setStatus(model.ActualStatusError, "fetch: "+err.Error())
			sw.logger.Warn("snapshot fetch failed",
				"input_id", inputID(sw.input), "err", err)
			if !sleepCtx(ctx, writeBackoff) {
				return
			}
			continue
		}

		snap, err := buildOrderBookSnapshot(sw.input, msg)
		if err != nil {
			sw.setStatus(model.ActualStatusError, "decode: "+err.Error())
			sw.logger.Warn("snapshot decode failed; record left uncommitted",
				"input_id", inputID(sw.input), "offset", msg.Offset, "err", err)
			// Don't commit — wait for the next valid snapshot.
			<-ctx.Done()
			return
		}

		sw.mu.Lock()
		sw.lastEvent = &snap.SnapshotTime
		sw.mu.Unlock()

		done := make(chan struct{})
		select {
		case <-ctx.Done():
			return
		case sw.out <- snapshotResult{snapshot: snap, offset: msg.Offset, done: done}:
		}

		// Wait for the group worker to acknowledge the snapshot was applied
		// before committing the offset. This prevents snapshot loss on restart.
		select {
		case <-ctx.Done():
			return
		case <-done:
		}

		if err := sw.consumer.Commit(ctx, msg); err != nil {
			if ctx.Err() != nil {
				return
			}
			sw.setStatus(model.ActualStatusError, "commit: "+err.Error())
			sw.logger.Warn("snapshot commit failed",
				"input_id", inputID(sw.input), "offset", msg.Offset, "err", err)
			if !sleepCtx(ctx, writeBackoff) {
				return
			}
			continue
		}
		sw.mu.Lock()
		off := msg.Offset
		sw.committed = &off
		sw.mu.Unlock()
	}
}

func (sw *snapshotInputWorker) setStatus(status, lastErr string) {
	sw.mu.Lock()
	defer sw.mu.Unlock()
	sw.status = status
	sw.lastErr = lastErr
}

func (sw *snapshotInputWorker) snapshot() model.StreamRuntimeStatus {
	sw.mu.Lock()
	defer sw.mu.Unlock()
	st := model.StreamRuntimeStatus{
		InputID:      inputID(sw.input),
		GroupID:      sw.group.GroupID,
		StreamKey:    sw.input.StreamKey,
		NodeID:       sw.nodeID,
		ActualStatus: sw.status,
		LastError:    sw.lastErr,
	}
	if sw.committed != nil {
		off := *sw.committed
		st.CommittedOffset = &off
	}
	if sw.lastEvent != nil {
		ev := *sw.lastEvent
		st.LastEventTime = &ev
	}
	now := time.Now()
	st.LastProcessedTime = &now
	return st
}

func (sw *snapshotInputWorker) report(ctx context.Context, reporter statusReporter) {
	st := sw.snapshot()
	if err := reporter.ReportStreamRuntimeStatus(ctx, st); err != nil {
		sw.logger.Warn("snapshot status report failed",
			"input_id", inputID(sw.input), "err", err)
	}
}
