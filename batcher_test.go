package autobatch

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

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
			*op.err = nil
			close(op.done)
		}
	})

	ops := make([]*pendingOp, 3)
	var wg sync.WaitGroup
	for i := range ops {
		ops[i] = newPendingOp(nil)
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
			*op.err = nil
			close(op.done)
		}
	})

	op := newPendingOp(nil)
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

	op := newPendingOp(nil)
	b.submit(op)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	err := wait(ctx, op)
	if err == nil {
		t.Fatal("expected context deadline error, got nil")
	}
}
