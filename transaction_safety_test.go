package autobatch_test

import (
	"testing"
	"time"

	autobatch "github.com/adrielcodeco/gorm-autobatch"
	"gorm.io/gorm"
)

// TestPlugin_NoBatching_InsideUserTransaction is the regression guard for the
// loss-of-write bug: if isTransaction() ever stops detecting an explicit user
// transaction, ops inside the tx will be re-routed to the batcher (which uses
// the root DB), bypassing the user's rollback.
//
// The test forces a rollback inside db.Transaction(...) and asserts that no
// record survives. If this fails, the plugin is unsafe — the batcher executed
// the op on the root DB after the user's tx aborted.
func TestPlugin_NoBatching_InsideUserTransaction(t *testing.T) {
	db := openBatchDB(t)

	registerPlugin(t, db, autobatch.Config{
		LatencyThreshold: durPtr(0), // always batch
		FlushTimeout:     50 * time.Millisecond,
		MaxBatchSize:     100,
	})

	err := db.Transaction(func(tx *gorm.DB) error {
		p := Product{Name: "inside-tx", Price: 10.0}
		if err := tx.Create(&p).Error; err != nil {
			return err
		}
		return gorm.ErrInvalidTransaction // force rollback
	})

	if err != gorm.ErrInvalidTransaction {
		t.Fatalf("expected ErrInvalidTransaction, got %v", err)
	}

	// Give any (incorrect) batcher flush a chance to run.
	time.Sleep(100 * time.Millisecond)

	var count int64
	db.Model(&Product{}).Where("name = ?", "inside-tx").Count(&count)
	if count != 0 {
		t.Fatalf("CRITICAL: record survived rollback (count=%d) — batcher bypassed user transaction", count)
	}
}
