package autobatch

import (
	"sync"
	"time"

	"gorm.io/gorm"
)

const pluginName = "gorm:autobatch"

// LogLevel represents the severity of a log message emitted by the plugin.
type LogLevel int

const (
	LogLevelDebug LogLevel = iota
	LogLevelInfo
	LogLevelWarn
	LogLevelError
)

func (l LogLevel) String() string {
	switch l {
	case LogLevelDebug:
		return "DEBUG"
	case LogLevelInfo:
		return "INFO"
	case LogLevelWarn:
		return "WARN"
	case LogLevelError:
		return "ERROR"
	default:
		return "UNKNOWN"
	}
}

// Config controls the autobatch plugin behaviour.
//
// Limitations to be aware of:
//   - In batch mode, callbacks registered AFTER "gorm:create"/"gorm:update"/
//     "gorm:delete" on the caller's *gorm.DB will not run for batched ops:
//     the plugin sets DryRun=true after the batch executes the op via a
//     separate session.
//   - Errors from the batch transaction (e.g. BEGIN/COMMIT failures) are
//     returned to every caller in that batch. Per-op errors (constraint
//     violations) are isolated via SAVEPOINT and only affect the offending op.
type Config struct {
	// LatencyThreshold controls the batching behaviour:
	//   nil  — batching is disabled; all operations run individually (plugin is
	//          effectively a no-op).
	//   0    — batch mode is always active regardless of measured latency.
	//   > 0  — batch mode activates when the P95 latency of recent operations
	//          exceeds this value, and deactivates when it drops back below it.
	LatencyThreshold *time.Duration

	// FlushTimeout is the maximum time an operation waits in the buffer before
	// the batch is flushed, even if MaxBatchSize has not been reached.
	FlushTimeout time.Duration

	// MaxBatchSize is the maximum number of pending operations that triggers an
	// immediate flush regardless of FlushTimeout.
	MaxBatchSize int

	// WindowDuration is the sliding window used to compute P95 latency.
	// Only relevant when LatencyThreshold > 0.
	WindowDuration time.Duration

	// Logger is called for plugin events. level indicates severity; msg is a
	// human-readable description; args are key-value pairs compatible with
	// slog, zap sugared, logrus, etc. If nil, no logging is performed.
	//
	// Example with slog:
	//   Logger: func(level autobatch.LogLevel, msg string, args ...any) {
	//       slog.Log(ctx, slogLevel(level), msg, args...)
	//   }
	Logger func(level LogLevel, msg string, args ...any)
}

// resolved is the immutable, defaulted form of Config used inside the Plugin.
// It owns its values; mutating the user's Config after New has no effect.
type resolved struct {
	thresholdEnabled bool
	thresholdValue   time.Duration
	flushTimeout     time.Duration
	maxBatchSize     int
	windowDuration   time.Duration
	logger           func(level LogLevel, msg string, args ...any)
}

func (c Config) resolve() resolved {
	r := resolved{
		flushTimeout:   c.FlushTimeout,
		maxBatchSize:   c.MaxBatchSize,
		windowDuration: c.WindowDuration,
		logger:         c.Logger,
	}
	if c.LatencyThreshold != nil {
		r.thresholdEnabled = true
		r.thresholdValue = *c.LatencyThreshold
	}
	if r.flushTimeout == 0 {
		r.flushTimeout = 10 * time.Millisecond
	}
	if r.maxBatchSize == 0 {
		r.maxBatchSize = 100
	}
	if r.windowDuration == 0 {
		r.windowDuration = 30 * time.Second
	}
	return r
}

func (r *resolved) log(level LogLevel, msg string, args ...any) {
	if r.logger != nil {
		r.logger(level, msg, args...)
	}
}

// Plugin is the gorm-autobatch GORM plugin. Register it with db.Use().
type Plugin struct {
	cfg     resolved
	latency *window

	// p95 cache avoids running a full sort on every incoming operation.
	p95Mu            sync.Mutex
	p95Cached        time.Duration
	p95ExpiresAt     time.Time
	p95BatchModePrev *bool // tracks last reported mode to log only on transitions

	creates *batcher
	updates *batcher
	deletes *batcher

	// disabled is set when Config.LatencyThreshold was explicitly nil — the
	// plugin behaves as a no-op transparent passthrough.
	disabled bool

	closeOnce sync.Once
}

// New creates the plugin. Call db.Use(New(cfg)) to register it.
func New(cfg Config) *Plugin {
	r := cfg.resolve()
	p := &Plugin{
		cfg:      r,
		latency:  newWindow(r.windowDuration, 5, 512),
		disabled: cfg.LatencyThreshold == nil,
	}
	return p
}

func (p *Plugin) Name() string { return pluginName }

// Initialize is called once by GORM when db.Use(plugin) is invoked.
func (p *Plugin) Initialize(db *gorm.DB) error {
	p.cfg.log(LogLevelInfo, "autobatch: initializing plugin",
		"disabled", p.disabled,
		"latency_threshold", p.cfg.thresholdValue,
		"flush_timeout", p.cfg.flushTimeout,
		"max_batch_size", p.cfg.maxBatchSize,
		"window_duration", p.cfg.windowDuration,
	)

	p.creates = newBatcher(p.cfg.flushTimeout, p.cfg.maxBatchSize, makeCreateFlush(db, p.latency, &p.cfg))
	p.updates = newBatcher(p.cfg.flushTimeout, p.cfg.maxBatchSize, makeUpdateFlush(db, p.latency, &p.cfg))
	p.deletes = newBatcher(p.cfg.flushTimeout, p.cfg.maxBatchSize, makeDeleteFlush(db, p.latency, &p.cfg))

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

	p.cfg.log(LogLevelInfo, "autobatch: plugin ready")
	return nil
}

// Close drains any in-flight batches synchronously and refuses new submits.
// After Close, intercepted operations on registered DBs that would have been
// batched will receive ErrBatcherClosed; callers may retry without the plugin.
// Safe to call multiple times.
func (p *Plugin) Close() {
	p.closeOnce.Do(func() {
		p.cfg.log(LogLevelInfo, "autobatch: closing plugin")
		if p.creates != nil {
			p.creates.close()
		}
		if p.updates != nil {
			p.updates.close()
		}
		if p.deletes != nil {
			p.deletes.close()
		}
	})
}

// isBatchMode returns true when batch mode should be active.
//
// Semantics driven by the resolved threshold:
//   - disabled  → always false (plugin is a no-op)
//   - value == 0 → always true  (batching always on)
//   - value  > 0 → true when the cached P95 latency meets or exceeds it
//
// The P95 result is cached for 200ms to avoid a full sort and double lock
// acquisition on every incoming operation.
func (p *Plugin) isBatchMode() bool {
	if p.disabled {
		return false
	}

	p.p95Mu.Lock()

	var active bool
	if p.cfg.thresholdValue == 0 {
		active = true
	} else {
		if time.Now().After(p.p95ExpiresAt) {
			p.p95Cached = p.latency.P95()
			p.p95ExpiresAt = time.Now().Add(200 * time.Millisecond)
		}
		active = p.p95Cached > 0 && p.p95Cached >= p.cfg.thresholdValue
	}

	var logTransition bool
	var logActive bool
	var logP95 time.Duration
	if p.p95BatchModePrev == nil || *p.p95BatchModePrev != active {
		prev := active
		p.p95BatchModePrev = &prev
		logTransition = true
		logActive = active
		logP95 = p.p95Cached
	}
	p.p95Mu.Unlock()

	if logTransition {
		if logActive {
			p.cfg.log(LogLevelWarn, "autobatch: switching to batch mode",
				"p95", logP95,
				"threshold", p.cfg.thresholdValue,
			)
		} else {
			p.cfg.log(LogLevelInfo, "autobatch: switching to individual mode",
				"p95", logP95,
				"threshold", p.cfg.thresholdValue,
			)
		}
	}

	return active
}
