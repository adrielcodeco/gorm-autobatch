package autobatch

import (
	"testing"
	"time"

	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

// TestIsTransaction_DetectsExplicitUserTx asserts that isTransaction returns
// true for db.Begin() and inside db.Transaction(...), but false for a fresh
// per-statement context with SkipDefaultTransaction.
//
// If GORM renames "gorm:started_transaction" (the InstanceSet key we rely on),
// this test will fail loudly instead of silently re-introducing the rollback
// bypass bug.
func TestIsTransaction_DetectsExplicitUserTx(t *testing.T) {
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{SkipDefaultTransaction: true})
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	// Fresh statement — no tx.
	if isTransaction(db) {
		t.Error("fresh db: expected isTransaction=false")
	}

	// db.Begin() returns a *gorm.DB whose ConnPool is a TxCommitter.
	tx := db.Begin()
	t.Cleanup(func() { _ = tx.Rollback() })
	if !isTransaction(tx) {
		t.Error("after Begin(): expected isTransaction=true")
	}

	// db.Transaction wraps the body in a real tx as well.
	called := false
	err = db.Transaction(func(inner *gorm.DB) error {
		called = true
		if !isTransaction(inner) {
			t.Error("inside Transaction(): expected isTransaction=true")
		}
		return nil
	})
	if err != nil || !called {
		t.Fatalf("Transaction did not run: err=%v called=%v", err, called)
	}
}

// TestIsTransaction_NilConnPool covers the defensive nil-ConnPool guard.
func TestIsTransaction_NilConnPool(t *testing.T) {
	db := &gorm.DB{Statement: &gorm.Statement{}}
	if isTransaction(db) {
		t.Fatal("expected isTransaction=false for nil ConnPool")
	}
}

// TestPercentile95_SingleValue covers the n==1 short-circuit branch.
func TestPercentile95_SingleValue(t *testing.T) {
	got := percentile95([]time.Duration{42 * time.Millisecond})
	if got != 42*time.Millisecond {
		t.Fatalf("expected 42ms, got %v", got)
	}
}
