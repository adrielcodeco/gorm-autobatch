package autobatch_test

import (
	"path/filepath"
	"testing"

	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// openBatchDB returns a GORM DB backed by a SQLite file in t.TempDir(). A
// real file (instead of :memory:) is required so the flush goroutine and the
// caller goroutines see the same database when they open separate
// connections. WAL mode plus a busy_timeout keeps concurrent writers from
// failing with SQLITE_BUSY under batch contention.
func openBatchDB(t *testing.T) *gorm.DB {
	t.Helper()

	dsn := filepath.Join(t.TempDir(), "batch.db") + "?_journal=WAL&_busy_timeout=5000&_synchronous=NORMAL"
	db, err := gorm.Open(sqlite.Open(dsn), &gorm.Config{
		Logger:                 logger.Default.LogMode(logger.Silent),
		SkipDefaultTransaction: true,
	})
	if err != nil {
		t.Fatalf("open batch db: %v", err)
	}

	if err := db.AutoMigrate(&Product{}); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	return db
}
