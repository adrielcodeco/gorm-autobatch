package autobatch

import (
	"context"
	"errors"
	"time"

	"gorm.io/gorm"
)

// Instance / context keys. Defined as named consts to avoid string-concat
// surprises and to make grep-ability trivial.
const (
	startTimeKey           = "gorm:autobatch:start_time"
	batchedMarker          = "gorm:autobatch:batched"
	batchedErrMarker       = "gorm:autobatch:batched_err"
	gormStartedTransaction = "gorm:started_transaction"
)

// errSkipCore is an internal sentinel set on db.Error so GORM's core
// create/update/delete callbacks early-return without executing SQL. afterOp
// strips it before returning to the caller. Using db.Error instead of
// db.DryRun avoids racing on the shared *gorm.Config field.
var errSkipCore = errors.New("autobatch: skip core (internal)")

type flushCtxKey struct{}

// flushContext marks a context as originating from the plugin's internal flush,
// so beforeOp can skip intercepting it and avoid a re-entrancy deadlock.
func flushContext(ctx context.Context) context.Context {
	return context.WithValue(ctx, flushCtxKey{}, true)
}

func isFlushContext(ctx context.Context) bool {
	v, _ := ctx.Value(flushCtxKey{}).(bool)
	return v
}

// beforeOp returns a callback that intercepts a GORM operation. In batch mode
// it enqueues the op, blocks until the batch flushes, then sets DryRun so the
// core callback skips execution (the batch already ran the operation).
// In individual mode it is a no-op and the core callback runs normally.
func (p *Plugin) beforeOp(b *batcher) func(*gorm.DB) {
	return func(db *gorm.DB) {
		db.InstanceSet(startTimeKey, time.Now())

		// Skip batching if:
		// 1. Database is already in an error state.
		// 2. This is a recursive call from the flush itself.
		// 3. Batch mode is inactive based on P95 latency.
		// 4. Operation is inside an explicit user transaction (to maintain
		//    atomicity/rollback semantics).
		if db.Error != nil || isFlushContext(db.Statement.Context) || !p.isBatchMode() || isTransaction(db) {
			p.cfg.log(LogLevelDebug, "autobatch: individual mode, op will run directly",
				"table", db.Statement.Table,
				"in_tx", isTransaction(db),
			)
			return
		}

		p.cfg.log(LogLevelDebug, "autobatch: enqueuing op into batch",
			"table", db.Statement.Table,
		)

		op := newPendingOp(db)
		if err := b.submit(op); err != nil {
			db.AddError(err)
			return
		}
		var realErr error
		if err := wait(db.Statement.Context, op); err != nil {
			p.cfg.log(LogLevelError, "autobatch: batch op returned error",
				"table", db.Statement.Table,
				"error", err,
			)
			realErr = err
		}

		// Signal afterOp to skip its latency recording (already recorded by
		// flush) and to strip the sentinel so the caller sees only realErr.
		db.InstanceSet(batchedMarker, true)
		if realErr != nil {
			db.InstanceSet(batchedErrMarker, realErr)
		}
		// Setting db.Error to the sentinel makes the core callback (gorm:create/
		// gorm:update/gorm:delete) early-return without executing SQL. We strip
		// the sentinel back out in afterOp. This is safer than setting
		// db.DryRun because DryRun lives on the shared *gorm.Config and would
		// race across goroutines using the same root *gorm.DB.
		db.Statement.Error = errSkipCore
		db.Error = errSkipCore
	}
}

// isTransaction returns true if the DB is inside an explicit user transaction
// (e.g. db.Transaction(...) or db.Begin()). It returns false for the default
// per-statement transaction that GORM opens automatically when
// SkipDefaultTransaction is false — those are marked with the
// "gorm:started_transaction" instance key by GORM's BeginTransaction callback.
func isTransaction(db *gorm.DB) bool {
	if db.Statement.ConnPool == nil {
		return false
	}
	if _, ok := db.Statement.ConnPool.(gorm.TxCommitter); !ok {
		return false
	}
	_, autoTx := db.InstanceGet(gormStartedTransaction)
	return !autoTx
}

// afterOp records the elapsed time into the latency window for non-batched
// ops. Batched ops have their latency recorded inside the flush function;
// here we also strip the internal sentinel error and restore the real error
// (if any) from the batch.
func (p *Plugin) afterOp() func(*gorm.DB) {
	return func(db *gorm.DB) {
		if _, wasBatched := db.InstanceGet(batchedMarker); wasBatched {
			// Strip the sentinel so the caller doesn't see it.
			if errors.Is(db.Error, errSkipCore) {
				db.Error = nil
				db.Statement.Error = nil
			}
			// Surface the real batch error (if any).
			if v, ok := db.InstanceGet(batchedErrMarker); ok {
				if realErr, ok := v.(error); ok && realErr != nil {
					db.AddError(realErr)
				}
			}
			return
		}
		if v, ok := db.InstanceGet(startTimeKey); ok {
			if start, ok := v.(time.Time); ok {
				elapsed := time.Since(start)
				p.latency.Record(elapsed)
				p.cfg.log(LogLevelDebug, "autobatch: individual op completed",
					"table", db.Statement.Table,
					"elapsed", elapsed,
				)
			}
		}
	}
}

// makeFlush returns a flush function that executes a slice of buffered ops
// inside a single transaction.
//
// Atomicity model: each op runs inside its own SAVEPOINT so that a failure on
// one op (e.g. a unique-constraint violation) only fails that op — the rest of
// the batch still commits. This avoids the "noisy neighbour" problem where one
// caller's bad input would otherwise roll back every other caller in the same
// batch.
func makeFlush(rootDB *gorm.DB, lat *window, cfg *resolved, opFn func(base *gorm.DB, op *pendingOp) *gorm.DB) func([]*pendingOp) {
	return func(ops []*pendingOp) {
		start := time.Now()

		cfg.log(LogLevelDebug, "autobatch: flushing batch",
			"size", len(ops),
		)

		// Separate already-cancelled ops before the transaction so they never
		// hold up the batch and receive their own error immediately after.
		var cancelled []*pendingOp
		var active []*pendingOp
		for _, op := range ops {
			if op.ctx.Err() != nil {
				cancelled = append(cancelled, op)
			} else {
				active = append(active, op)
			}
		}

		// perOpErrs[i] is the error (or nil) for active[i] after savepoint
		// execution. txErr is set only on infrastructure failures (BEGIN/COMMIT).
		perOpErrs := make([]error, len(active))
		var txErr error

		if len(active) > 0 {
			txErr = rootDB.Transaction(func(tx *gorm.DB) error {
				for i, opReq := range active {
					// Recheck ctx right before executing — closes the window
					// between the initial pre-tx check and the actual op.
					if err := opReq.ctx.Err(); err != nil {
						perOpErrs[i] = err
						continue
					}

					spName := savepointName(i)
					if err := tx.SavePoint(spName).Error; err != nil {
						// Savepoint unsupported (or tx broken) — fall back to
						// shared-atomicity mode for the remainder: every op
						// here on shares fate. This is rare.
						perOpErrs[i] = runOpDirect(tx, opReq, opFn)
						if perOpErrs[i] != nil {
							return perOpErrs[i]
						}
						continue
					}

					perOpErrs[i] = runOpDirect(tx, opReq, opFn)
					if perOpErrs[i] != nil {
						// Roll back just this op; keep the outer tx alive.
						if rbErr := tx.RollbackTo(spName).Error; rbErr != nil {
							// Can't recover the tx — propagate to all.
							return rbErr
						}
					}
				}
				return nil
			})
		}

		elapsed := time.Since(start)
		lat.Record(elapsed)

		if txErr != nil {
			cfg.log(LogLevelError, "autobatch: batch transaction failed, rolling back",
				"size", len(active),
				"elapsed", elapsed,
				"error", txErr,
			)
		} else if len(active) > 0 {
			cfg.log(LogLevelInfo, "autobatch: batch flushed successfully",
				"size", len(active),
				"elapsed", elapsed,
			)
		}

		for _, op := range cancelled {
			op.err = op.ctx.Err()
			close(op.done)
		}
		for i, op := range active {
			if txErr != nil {
				op.err = txErr
			} else {
				op.err = perOpErrs[i]
			}
			close(op.done)
		}
	}
}

// runOpDirect executes a single op against the given tx, captures its
// RowsAffected on the pendingOp, and returns its error.
func runOpDirect(tx *gorm.DB, opReq *pendingOp, opFn func(base *gorm.DB, op *pendingOp) *gorm.DB) error {
	base := tx.Session(&gorm.Session{
		Context:                flushContext(opReq.ctx),
		SkipDefaultTransaction: true,
		NewDB:                  true,
	})
	if opReq.model != nil {
		base = base.Model(opReq.model)
	}
	if opReq.table != "" {
		base = base.Table(opReq.table)
	}

	res := opFn(base, opReq)
	opReq.rows = res.RowsAffected
	return res.Error
}

// savepointName returns a deterministic, SQL-safe savepoint identifier.
// Underscore-only / numeric suffix is portable across PostgreSQL, MySQL, etc.
func savepointName(i int) string {
	// Pre-allocated small set is cheaper than fmt.Sprintf; flushes are bounded
	// by MaxBatchSize so a fixed namespace works.
	const prefix = "ab_sp_"
	// itoa without fmt to keep this hot path allocation-light.
	var buf [20]byte
	pos := len(buf)
	n := i
	if n == 0 {
		pos--
		buf[pos] = '0'
	} else {
		for n > 0 {
			pos--
			buf[pos] = byte('0' + n%10)
			n /= 10
		}
	}
	return prefix + string(buf[pos:])
}

func makeCreateFlush(rootDB *gorm.DB, lat *window, cfg *resolved) func([]*pendingOp) {
	return makeFlush(rootDB, lat, cfg, func(base *gorm.DB, op *pendingOp) *gorm.DB {
		return base.Create(op.dest)
	})
}

func makeUpdateFlush(rootDB *gorm.DB, lat *window, cfg *resolved) func([]*pendingOp) {
	return makeFlush(rootDB, lat, cfg, func(base *gorm.DB, op *pendingOp) *gorm.DB {
		return base.Updates(op.dest)
	})
}

func makeDeleteFlush(rootDB *gorm.DB, lat *window, cfg *resolved) func([]*pendingOp) {
	return makeFlush(rootDB, lat, cfg, func(base *gorm.DB, op *pendingOp) *gorm.DB {
		return base.Delete(op.dest)
	})
}
