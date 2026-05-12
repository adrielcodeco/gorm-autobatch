package autobatch

import (
	"time"

	"gorm.io/gorm"
)

const (
	startTimeKey  = pluginName + ":start_time"
	batchedMarker = pluginName + ":batched"
)

// beforeOp returns a callback that intercepts a GORM operation. In batch mode
// it enqueues the op, blocks until the batch flushes, then sets DryRun so the
// core callback skips execution (the batch already ran the operation).
// In individual mode it is a no-op and the core callback runs normally.
func (p *Plugin) beforeOp(b *batcher) func(*gorm.DB) {
	return func(db *gorm.DB) {
		db.InstanceSet(startTimeKey, time.Now())

		if db.Error != nil || !p.isBatchMode() {
			return
		}

		op := newPendingOp(db)
		b.submit(op)
		if err := wait(db.Statement.Context, op); err != nil {
			db.AddError(err)
		}

		// Signal afterOp to skip its latency recording (already recorded by flush).
		db.InstanceSet(batchedMarker, true)
		// DryRun prevents the core callback from executing a second time.
		db.DryRun = true
	}
}

// afterOp records the elapsed time into the latency window for non-batched ops.
// Batched ops have their latency recorded inside the flush function.
func (p *Plugin) afterOp() func(*gorm.DB) {
	return func(db *gorm.DB) {
		if _, wasBatched := db.InstanceGet(batchedMarker); wasBatched {
			return
		}
		if v, ok := db.InstanceGet(startTimeKey); ok {
			if start, ok := v.(time.Time); ok {
				p.latency.Record(time.Since(start))
			}
		}
	}
}

// makeFlush returns a flush function that executes a slice of buffered ops
// inside a single transaction.
//
// Each op uses tx (clean statement, real conn pool) as the base, carrying
// over only the caller's context, table override, and model/dest pointers.
// This avoids inheriting the DryRun=true and stub SQL that beforeOp set on
// src to prevent the intercepted core callback from re-executing.
//
// Batch semantics are all-or-nothing: if any op fails the transaction rolls
// back and all callers in the batch receive the same error.
func makeFlush(rootDB *gorm.DB, lat *window, exec func(base *gorm.DB, model, dest any) error) func([]*pendingOp) {
	return func(ops []*pendingOp) {
		start := time.Now()

		err := rootDB.Transaction(func(tx *gorm.DB) error {
			for _, op := range ops {
				src := op.db.(*gorm.DB)
				base := tx.WithContext(src.Statement.Context)
				if src.Statement.Table != "" {
					base = base.Table(src.Statement.Table)
				}
				if e := exec(base, src.Statement.Model, src.Statement.Dest); e != nil {
					return e
				}
			}
			return nil
		})

		lat.Record(time.Since(start))

		for _, op := range ops {
			*op.err = err
			close(op.done)
		}
	}
}

func makeCreateFlush(rootDB *gorm.DB, lat *window) func([]*pendingOp) {
	return makeFlush(rootDB, lat, func(base *gorm.DB, model, dest any) error {
		return base.Model(model).Create(dest).Error
	})
}

func makeUpdateFlush(rootDB *gorm.DB, lat *window) func([]*pendingOp) {
	return makeFlush(rootDB, lat, func(base *gorm.DB, model, dest any) error {
		return base.Model(model).Updates(dest).Error
	})
}

func makeDeleteFlush(rootDB *gorm.DB, lat *window) func([]*pendingOp) {
	return makeFlush(rootDB, lat, func(base *gorm.DB, model, dest any) error {
		return base.Model(model).Delete(dest).Error
	})
}
