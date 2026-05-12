package autobatch

import (
	"sync"
	"time"

	"gorm.io/gorm"
)

const pluginName = "gorm:autobatch"

// Config controls the autobatch plugin behaviour.
type Config struct {
	// LatencyThreshold is the P95 latency above which the plugin switches to
	// batch mode. Operations below this threshold are sent individually.
	LatencyThreshold time.Duration

	// FlushTimeout is the maximum time an operation waits in the buffer before
	// the batch is flushed, even if MaxBatchSize has not been reached.
	FlushTimeout time.Duration

	// MaxBatchSize is the maximum number of pending operations that triggers an
	// immediate flush regardless of FlushTimeout.
	MaxBatchSize int

	// WindowDuration is the sliding window used to compute P95 latency.
	WindowDuration time.Duration
}

func (c *Config) withDefaults() Config {
	out := *c
	if out.LatencyThreshold == 0 {
		out.LatencyThreshold = 50 * time.Millisecond
	}
	if out.FlushTimeout == 0 {
		out.FlushTimeout = 10 * time.Millisecond
	}
	if out.MaxBatchSize == 0 {
		out.MaxBatchSize = 100
	}
	if out.WindowDuration == 0 {
		out.WindowDuration = 30 * time.Second
	}
	return out
}

// Plugin is the gorm-autobatch GORM plugin. Register it with db.Use().
type Plugin struct {
	cfg     Config
	latency *window

	// p95 cache avoids running a full sort on every incoming operation.
	p95Mu        sync.Mutex
	p95Cached    time.Duration
	p95ExpiresAt time.Time

	creates *batcher
	updates *batcher
	deletes *batcher
}

// New creates the plugin. Call db.Use(New(cfg)) to register it.
func New(cfg Config) *Plugin {
	cfg = cfg.withDefaults()
	return &Plugin{
		cfg:     cfg,
		latency: newWindow(cfg.WindowDuration, 5, 512),
	}
}

func (p *Plugin) Name() string { return pluginName }

// Initialize is called once by GORM when db.Use(plugin) is invoked.
func (p *Plugin) Initialize(db *gorm.DB) error {
	p.creates = newBatcher(p.cfg.FlushTimeout, p.cfg.MaxBatchSize, makeCreateFlush(db, p.latency))
	p.updates = newBatcher(p.cfg.FlushTimeout, p.cfg.MaxBatchSize, makeUpdateFlush(db, p.latency))
	p.deletes = newBatcher(p.cfg.FlushTimeout, p.cfg.MaxBatchSize, makeDeleteFlush(db, p.latency))

	if err := db.Callback().Create().Before("gorm:create").Register(pluginName+":before_create", p.beforeOp(p.creates)); err != nil {
		return err
	}
	if err := db.Callback().Create().After("gorm:create").Register(pluginName+":after_create", p.afterOp()); err != nil {
		return err
	}
	if err := db.Callback().Update().Before("gorm:update").Register(pluginName+":before_update", p.beforeOp(p.updates)); err != nil {
		return err
	}
	if err := db.Callback().Update().After("gorm:update").Register(pluginName+":after_update", p.afterOp()); err != nil {
		return err
	}
	if err := db.Callback().Delete().Before("gorm:delete").Register(pluginName+":before_delete", p.beforeOp(p.deletes)); err != nil {
		return err
	}
	if err := db.Callback().Delete().After("gorm:delete").Register(pluginName+":after_delete", p.afterOp()); err != nil {
		return err
	}

	return nil
}

// isBatchMode returns true when the P95 latency exceeds the configured
// threshold. The result is cached for 200ms to avoid running a full sort
// and double lock acquisition on every incoming operation.
func (p *Plugin) isBatchMode() bool {
	p.p95Mu.Lock()
	defer p.p95Mu.Unlock()

	if time.Now().After(p.p95ExpiresAt) {
		p.p95Cached = p.latency.P95()
		p.p95ExpiresAt = time.Now().Add(200 * time.Millisecond)
	}
	return p.p95Cached > 0 && p.p95Cached >= p.cfg.LatencyThreshold
}
