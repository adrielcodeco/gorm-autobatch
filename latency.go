package autobatch

import (
	"math"
	"sort"
	"sync"
	"time"
)

// window tracks operation durations using a Prometheus-style bucket ring.
// The ring divides the total window into numBuckets equal-duration slots.
// Observations land in the hot buffer first (low contention), then are
// flushed into the ring by a background goroutine or on P95 query.
type window struct {
	// bufMu guards only hotBuf and hotBufExpTime (high-frequency write path).
	// ringMu guards coldBuf and the bucket ring (lower-frequency).
	// Acquisition order when both are needed: bufMu first, then ringMu.
	bufMu sync.Mutex
	ringMu sync.Mutex

	hotBuf        []time.Duration
	coldBuf       []time.Duration
	hotBufExpTime time.Time

	buckets        [][]time.Duration
	bucketDur      time.Duration
	headIdx        int
	headExpTime    time.Time
}

func newWindow(total time.Duration, numBuckets, bufCap int) *window {
	bd := total / time.Duration(numBuckets)
	now := time.Now()
	buckets := make([][]time.Duration, numBuckets)
	for i := range buckets {
		buckets[i] = make([]time.Duration, 0, bufCap/numBuckets+1)
	}
	return &window{
		hotBuf:        make([]time.Duration, 0, bufCap),
		coldBuf:       make([]time.Duration, 0, bufCap),
		hotBufExpTime: now.Add(bd),
		buckets:       buckets,
		bucketDur:     bd,
		headExpTime:   now.Add(bd),
	}
}

// Record adds d to the hot buffer. Only bufMu is held, so concurrent
// callers do not block on ring operations.
func (w *window) Record(d time.Duration) {
	w.bufMu.Lock()
	defer w.bufMu.Unlock()

	now := time.Now()
	if now.After(w.hotBufExpTime) {
		w.flushHot(now)
	}
	w.hotBuf = append(w.hotBuf, d)
	if len(w.hotBuf) == cap(w.hotBuf) {
		w.flushHot(now)
	}
}

// flushHot swaps hot/cold under both locks, then drains cold into the ring
// in a goroutine so Record() returns quickly. Must be called with bufMu held.
func (w *window) flushHot(now time.Time) {
	w.ringMu.Lock()
	if len(w.coldBuf) == 0 {
		w.hotBuf, w.coldBuf = w.coldBuf, w.hotBuf
		for now.After(w.hotBufExpTime) {
			w.hotBufExpTime = w.hotBufExpTime.Add(w.bucketDur)
		}
	}
	toFlush := w.coldBuf
	w.ringMu.Unlock()

	if len(toFlush) > 0 {
		go func() {
			w.ringMu.Lock()
			w.drainCold()
			w.ringMu.Unlock()
		}()
	}
}

// drainCold appends coldBuf into the current bucket and advances stale buckets.
// Must be called with ringMu held.
func (w *window) drainCold() {
	w.buckets[w.headIdx] = append(w.buckets[w.headIdx], w.coldBuf...)
	w.coldBuf = w.coldBuf[:0]
	w.rotateBuckets()
}

// rotateBuckets advances the ring head past any expired bucket boundaries,
// clearing each stale slot. Must be called with ringMu held.
func (w *window) rotateBuckets() {
	now := time.Now()
	for now.After(w.headExpTime) {
		w.headIdx = (w.headIdx + 1) % len(w.buckets)
		w.buckets[w.headIdx] = w.buckets[w.headIdx][:0]
		w.headExpTime = w.headExpTime.Add(w.bucketDur)
	}
}

// P95 returns the 95th-percentile duration across all live buckets.
// Forces a synchronous hot-buffer flush so recent observations are included.
func (w *window) P95() time.Duration {
	// Force-flush hot buffer synchronously before snapshotting.
	w.bufMu.Lock()
	w.ringMu.Lock()
	if len(w.coldBuf) == 0 {
		w.hotBuf, w.coldBuf = w.coldBuf, w.hotBuf
		now := time.Now()
		for now.After(w.hotBufExpTime) {
			w.hotBufExpTime = w.hotBufExpTime.Add(w.bucketDur)
		}
	}
	w.ringMu.Unlock()
	w.bufMu.Unlock()

	w.ringMu.Lock()
	w.drainCold()
	var all []time.Duration
	for _, b := range w.buckets {
		all = append(all, b...)
	}
	w.ringMu.Unlock()

	if len(all) == 0 {
		return 0
	}
	sort.Slice(all, func(i, j int) bool { return all[i] < all[j] })
	return percentile95(all)
}

// percentile95 uses linear interpolation (same method as Prometheus) on a
// pre-sorted slice. Returns the 95th percentile value.
func percentile95(sorted []time.Duration) time.Duration {
	n := len(sorted)
	if n == 1 {
		return sorted[0]
	}
	pos := 0.95*float64(n) - 0.5
	k := int(math.Floor(pos))
	frac := pos - math.Floor(pos)
	if k >= n-1 {
		return sorted[n-1]
	}
	return time.Duration(float64(sorted[k])*(1-frac) + float64(sorted[k+1])*frac)
}
