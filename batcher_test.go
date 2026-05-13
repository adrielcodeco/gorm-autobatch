package autobatch

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func newTestOp(ctx context.Context) *pendingOp {
	return &pendingOp{
		ctx:  ctx,
		done: make(chan struct{}),
	}
}

func TestBatcher_SizeFlush(t *testing.T) {
	var flushed atomic.Int32
	var mu sync.Mutex
	var batches [][]*pendingOp

	b := newBatcher(10*time.Second, 3, func(ops []*pendingOp) {
		flushed.Add(int32(len(ops)))
		mu.Lock()
		batches = append(batches, ops)
		mu.Unlock()
		for _, op := range ops {
			op.err = nil
			close(op.done)
		}
	})

	ops := make([]*pendingOp, 3)
	var wg sync.WaitGroup
	for i := range ops {
		ops[i] = newTestOp(context.Background())
		wg.Add(1)
		go func(op *pendingOp) {
			defer wg.Done()
			b.submit(op)
			_ = wait(context.Background(), op)
		}(ops[i])
	}
	wg.Wait()

	if got := flushed.Load(); got != 3 {
		t.Fatalf("want 3 ops flushed, got %d", got)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(batches) != 1 {
		t.Fatalf("want 1 batch, got %d", len(batches))
	}
}

func TestBatcher_TimerFlush(t *testing.T) {
	var flushed atomic.Int32

	b := newBatcher(20*time.Millisecond, 100, func(ops []*pendingOp) {
		flushed.Add(int32(len(ops)))
		for _, op := range ops {
			op.err = nil
			close(op.done)
		}
	})

	op := newTestOp(context.Background())
	b.submit(op)

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	if err := wait(ctx, op); err != nil {
		t.Fatalf("wait: %v", err)
	}
	if got := flushed.Load(); got != 1 {
		t.Fatalf("want 1 op flushed, got %d", got)
	}
}

func TestBatcher_ContextCancel(t *testing.T) {
	b := newBatcher(10*time.Second, 100, func(ops []*pendingOp) {
		// never flushes within the test
		_ = ops
	})

	op := newTestOp(context.Background())
	b.submit(op)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	err := wait(ctx, op)
	if err == nil {
		t.Fatal("expected context deadline error, got nil")
	}
}

// TestBatcher_Close_DrainsPending verifies that close() flushes any pending
// ops synchronously and returns control after the flush is done.
func TestBatcher_Close_DrainsPending(t *testing.T) {
	var flushed atomic.Int32
	b := newBatcher(1*time.Hour, 100, func(ops []*pendingOp) {
		flushed.Add(int32(len(ops)))
		for _, op := range ops {
			op.err = nil
			close(op.done)
		}
	})

	op := newTestOp(context.Background())
	b.submit(op)

	b.close()

	if got := flushed.Load(); got != 1 {
		t.Fatalf("close did not drain pending op: flushed=%d", got)
	}
	if err := wait(context.Background(), op); err != nil {
		t.Fatalf("op should have succeeded: %v", err)
	}
}

// TestBatcher_Close_RejectsNewSubmits verifies that after close(), submit()
// returns ErrBatcherClosed and the op's done channel is closed with that err.
func TestBatcher_Close_RejectsNewSubmits(t *testing.T) {
	b := newBatcher(1*time.Hour, 100, func(ops []*pendingOp) {
		for _, op := range ops {
			close(op.done)
		}
	})

	b.close()

	op := newTestOp(context.Background())
	err := b.submit(op)
	if err != ErrBatcherClosed {
		t.Fatalf("expected ErrBatcherClosed, got %v", err)
	}
	if werr := wait(context.Background(), op); werr != ErrBatcherClosed {
		t.Fatalf("wait should surface ErrBatcherClosed, got %v", werr)
	}
}

// TestBatcher_Close_Idempotent verifies that calling close() twice is safe.
func TestBatcher_Close_Idempotent(t *testing.T) {
	b := newBatcher(1*time.Hour, 100, func(ops []*pendingOp) {
		for _, op := range ops {
			close(op.done)
		}
	})
	b.close()
	b.close() // must not panic, must not double-flush
}

// TestBatcher_StaleTimerCallback exercises the generation guard in
// timerFlush: a size-trigger flush bumps timerGen, so the (already armed)
// time.AfterFunc callback must see a mismatched generation and no-op.
func TestBatcher_StaleTimerCallback(t *testing.T) {
	var flushed atomic.Int32
	// flushTimeout very short so the timer fires after the size-trigger drain.
	b := newBatcher(5*time.Millisecond, 1, func(ops []*pendingOp) {
		flushed.Add(int32(len(ops)))
		for _, op := range ops {
			op.err = nil
			close(op.done)
		}
	})

	op := newTestOp(context.Background())
	b.submit(op) // maxSize=1 → flushes immediately

	// Wait long enough for the originally-armed timer to fire. If the
	// generation guard is broken, the callback would either re-flush an empty
	// pending or clobber state. Either way the count must stay at 1.
	time.Sleep(50 * time.Millisecond)

	if got := flushed.Load(); got != 1 {
		t.Fatalf("stale timer callback caused extra flush: got %d, want 1", got)
	}
}
