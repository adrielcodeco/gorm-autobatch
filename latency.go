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
// flushed into the ring by a single background drainer.
type window struct {
	// bufMu guards only hotBuf and hotBufExpTime (high-frequency write path).
	// ringMu guards coldBuf, the bucket ring, and drainPending.
	// Acquisition order when both are needed: bufMu first, then ringMu.
	bufMu  sync.Mutex
	ringMu sync.Mutex

	hotBuf        []time.Duration
	coldBuf       []time.Duration
	hotBufExpTime time.Time

	buckets     [][]time.Duration
	bucketDur   time.Duration
	headIdx     int
	headExpTime time.Time

	// drainPending coalesces multiple async drain requests into one goroutine.
	drainPending bool
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

// Record adds d to the hot buffer. Only bufMu is held in the fast path; the
// drain into the ring happens asynchronously (coalesced) or synchronously when
// the previous drain is still pending and the hot buffer is full.
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

// flushHot tries to swap hot↔cold and schedule an async drainer. If cold is
// still occupied (drainer hasn't caught up), it drains synchronously so the
// hot buffer never grows unbounded. Must be called with bufMu held.
func (w *window) flushHot(now time.Time) {
	w.ringMu.Lock()
	if len(w.coldBuf) > 0 {
		// Previous drain hasn't run yet — drain synchronously to avoid pile-up.
		w.drainColdLocked()
	}
	w.hotBuf, w.coldBuf = w.coldBuf, w.hotBuf
	for now.After(w.hotBufExpTime) {
		w.hotBufExpTime = w.hotBufExpTime.Add(w.bucketDur)
	}

	if len(w.coldBuf) > 0 && !w.drainPending {
		w.drainPending = true
		w.ringMu.Unlock()
		go w.asyncDrain()
		return
	}
	w.ringMu.Unlock()
}

// asyncDrain runs the cold-buffer drain in a separate goroutine so callers of
// Record return quickly. Only one such goroutine runs at a time (coalesced by
// drainPending), preventing the goroutine pile-up under high write load.
func (w *window) asyncDrain() {
	w.ringMu.Lock()
	w.drainColdLocked()
	w.drainPending = false
	w.ringMu.Unlock()
}

// drainColdLocked appends coldBuf into the current bucket and advances stale
// buckets. Must be called with ringMu held.
func (w *window) drainColdLocked() {
	if len(w.coldBuf) > 0 {
		w.buckets[w.headIdx] = append(w.buckets[w.headIdx], w.coldBuf...)
		w.coldBuf = w.coldBuf[:0]
	}
	w.rotateBucketsLocked()
}

// rotateBucketsLocked advances the ring head past any expired bucket
// boundaries, clearing each stale slot. Capped at len(buckets) iterations so
// long process suspensions or clock skew can't cause a runaway loop.
// Must be called with ringMu held.
func (w *window) rotateBucketsLocked() {
	now := time.Now()
	max := len(w.buckets)
	for i := 0; i < max && now.After(w.headExpTime); i++ {
		w.headIdx = (w.headIdx + 1) % len(w.buckets)
		w.buckets[w.headIdx] = w.buckets[w.headIdx][:0]
		w.headExpTime = w.headExpTime.Add(w.bucketDur)
	}
	// If we hit the cap, the entire ring is older than the window — fast-forward
	// headExpTime to now+bucketDur so subsequent rotations are bounded.
	if now.After(w.headExpTime) {
		for i := range w.buckets {
			w.buckets[i] = w.buckets[i][:0]
		}
		w.headExpTime = now.Add(w.bucketDur)
	}
}

// P95 returns the 95th-percentile duration across all live buckets.
// Forces a synchronous hot-buffer flush so recent observations are included.
func (w *window) P95() time.Duration {
	w.bufMu.Lock()
	w.ringMu.Lock()
	if len(w.coldBuf) > 0 {
		w.drainColdLocked()
	}
	w.hotBuf, w.coldBuf = w.coldBuf, w.hotBuf
	now := time.Now()
	for now.After(w.hotBufExpTime) {
		w.hotBufExpTime = w.hotBufExpTime.Add(w.bucketDur)
	}
	w.drainColdLocked()
	w.bufMu.Unlock()

	totalLen := 0
	for _, b := range w.buckets {
		totalLen += len(b)
	}

	if totalLen == 0 {
		w.ringMu.Unlock()
		return 0
	}

	all := make([]time.Duration, 0, totalLen)
	for _, b := range w.buckets {
		all = append(all, b...)
	}
	w.ringMu.Unlock()

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
