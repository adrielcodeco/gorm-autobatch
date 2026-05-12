package autobatch

import (
	"context"
	"sync"
	"time"
)

// pendingOp is one buffered database operation waiting to be executed in a batch.
type pendingOp struct {
	db   any // *gorm.DB — stored as any to avoid import cycle
	done chan struct{} // closed after the batch containing this op has executed
	err  *error       // written before done is closed; caller reads after
}

// batcher collects pending operations and flushes them in batches.
type batcher struct {
	mu           sync.Mutex
	pending      []*pendingOp
	timer        *time.Timer
	flushTimeout time.Duration
	maxSize      int
	flushFn      func([]*pendingOp) // executes the batch; sets op.err, closes op.done
}

func newBatcher(flushTimeout time.Duration, maxSize int, flushFn func([]*pendingOp)) *batcher {
	return &batcher{
		flushTimeout: flushTimeout,
		maxSize:      maxSize,
		flushFn:      flushFn,
	}
}

// submit enqueues op and returns immediately. The caller blocks on op.done
// to wait for the batch result.
//
// A flush timer is started on the first op in a new batch. If the buffer
// reaches maxSize, the batch is flushed inline before returning.
func (b *batcher) submit(op *pendingOp) {
	b.mu.Lock()

	b.pending = append(b.pending, op)

	if len(b.pending) == 1 {
		b.timer = time.AfterFunc(b.flushTimeout, func() {
			b.mu.Lock()
			batch := b.drain()
			b.mu.Unlock()
			if len(batch) > 0 {
				b.flushFn(batch)
			}
		})
	}

	var earlyBatch []*pendingOp
	if len(b.pending) >= b.maxSize {
		if b.timer != nil {
			b.timer.Stop()
			b.timer = nil
		}
		earlyBatch = b.drain()
	}

	b.mu.Unlock()

	if len(earlyBatch) > 0 {
		b.flushFn(earlyBatch)
	}
}

// drain atomically removes all pending ops and returns them.
// Must be called with b.mu held.
func (b *batcher) drain() []*pendingOp {
	if len(b.pending) == 0 {
		return nil
	}
	batch := b.pending
	b.pending = nil
	return batch
}

// wait blocks until op's batch has been executed or ctx is cancelled.
func wait(ctx context.Context, op *pendingOp) error {
	select {
	case <-op.done:
		return *op.err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func newPendingOp(db any) *pendingOp {
	var errVal error
	return &pendingOp{
		db:   db,
		done: make(chan struct{}),
		err:  &errVal,
	}
}
