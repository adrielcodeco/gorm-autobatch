package autobatch

import (
	"sync"
	"testing"
	"time"
)

func TestWindowP95_Empty(t *testing.T) {
	w := newWindow(30*time.Second, 5, 64)
	if got := w.P95(); got != 0 {
		t.Fatalf("empty window: want 0, got %v", got)
	}
}

func TestWindowP95_SingleValue(t *testing.T) {
	w := newWindow(30*time.Second, 5, 64)
	w.Record(42 * time.Millisecond)
	if got := w.P95(); got == 0 {
		t.Fatal("expected non-zero P95 after recording one value")
	}
}

func TestWindowP95_KnownDistribution(t *testing.T) {
	w := newWindow(30*time.Second, 5, 512)
	// Record 100 values: 1ms..100ms. P95 should be ~95ms.
	for i := 1; i <= 100; i++ {
		w.Record(time.Duration(i) * time.Millisecond)
	}
	p95 := w.P95()
	// Allow generous range due to interpolation and hot-buffer timing.
	if p95 < 90*time.Millisecond || p95 > 100*time.Millisecond {
		t.Fatalf("P95 out of expected range [90ms,100ms], got %v", p95)
	}
}

func TestWindowP95_ConcurrentRecord(t *testing.T) {
	w := newWindow(30*time.Second, 5, 512)
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			w.Record(time.Duration(n) * time.Millisecond)
		}(i)
	}
	wg.Wait()
	// Just verify no race / panic and we get a plausible value.
	p95 := w.P95()
	if p95 < 0 {
		t.Fatalf("negative P95: %v", p95)
	}
}

// TestWindowP95_HotBufferFlush forces the hot buffer to fill past capacity
// so flushHot is triggered, exercising the hot→cold swap path.
func TestWindowP95_HotBufferFlush(t *testing.T) {
	// bufCap=4 so flushHot fires after 4 records without waiting for the timer.
	w := newWindow(30*time.Second, 5, 4)
	for i := 0; i < 20; i++ {
		w.Record(time.Duration(i+1) * time.Millisecond)
	}
	// Give the async goroutine time to drain cold into the ring.
	time.Sleep(10 * time.Millisecond)
	p95 := w.P95()
	if p95 == 0 {
		t.Fatal("expected non-zero P95 after flushing hot buffer")
	}
}

// TestWindowP95_BucketRotation verifies that observations older than the
// window are evicted when buckets rotate.
func TestWindowP95_BucketRotation(t *testing.T) {
	// 2 buckets of 50ms each → total window = 100ms.
	w := newWindow(100*time.Millisecond, 2, 64)
	w.Record(999 * time.Millisecond) // large outlier
	time.Sleep(120 * time.Millisecond)
	// After the window expires the outlier should be gone.
	w.Record(1 * time.Millisecond)
	p95 := w.P95()
	if p95 > 50*time.Millisecond {
		t.Fatalf("expected outlier evicted, but P95=%v", p95)
	}
}

// TestWindowP95_FastForwardAfterLongIdle verifies that a window left idle for
// longer than its total duration fast-forwards instead of looping through
// every bucket — and that all old observations are evicted.
func TestWindowP95_FastForwardAfterLongIdle(t *testing.T) {
	// 3 buckets of 20ms each → 60ms total. Sleep for 200ms (>3x the window).
	w := newWindow(60*time.Millisecond, 3, 64)
	w.Record(999 * time.Millisecond)

	time.Sleep(200 * time.Millisecond)

	w.Record(1 * time.Millisecond)
	p95 := w.P95()
	if p95 > 50*time.Millisecond {
		t.Fatalf("expected outlier evicted after long idle, got P95=%v", p95)
	}
}

func TestPercentile95_Sorted(t *testing.T) {
	vals := make([]time.Duration, 100)
	for i := range vals {
		vals[i] = time.Duration(i+1) * time.Millisecond
	}
	p := percentile95(vals)
	if p < 94*time.Millisecond || p > 100*time.Millisecond {
		t.Fatalf("unexpected percentile95: %v", p)
	}
}
