package autobatch_test

import (
	"fmt"
	"sync"
	"testing"
	"time"

	autobatch "github.com/adrielcodeco/gorm-autobatch"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

type Product struct {
	ID    uint   `gorm:"primarykey"`
	Name  string `gorm:"not null"`
	Price float64
}

func openDB(t *testing.T) *gorm.DB {
	t.Helper()
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{
		Logger:                 logger.Default.LogMode(logger.Silent),
		SkipDefaultTransaction: true,
	})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	// SQLite :memory: creates a separate database per connection. Limit to one
	// connection so all operations (including those from the flush goroutine)
	// share the same in-memory database.
	sqlDB, _ := db.DB()
	sqlDB.SetMaxOpenConns(1)

	if err := db.AutoMigrate(&Product{}); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	return db
}

func registerPlugin(t *testing.T, db *gorm.DB, cfg autobatch.Config) {
	t.Helper()
	if err := db.Use(autobatch.New(cfg)); err != nil {
		t.Fatalf("db.Use: %v", err)
	}
}

func durPtr(d time.Duration) *time.Duration { return &d }

// TestPlugin_IndividualMode verifies that when P95 is below the threshold
// the plugin is transparent: creates, updates, and deletes work normally.
func TestPlugin_IndividualMode(t *testing.T) {
	db := openDB(t)
	registerPlugin(t, db, autobatch.Config{
		LatencyThreshold: durPtr(10 * time.Hour), // effectively never batch
		FlushTimeout:     10 * time.Millisecond,
		MaxBatchSize:     100,
	})

	p := Product{Name: "Widget", Price: 9.99}
	if err := db.Create(&p).Error; err != nil {
		t.Fatalf("create: %v", err)
	}
	if p.ID == 0 {
		t.Fatal("expected auto-increment ID to be set after create")
	}

	if err := db.Model(&p).Update("Price", 19.99).Error; err != nil {
		t.Fatalf("update: %v", err)
	}

	var found Product
	db.First(&found, p.ID)
	if found.Price != 19.99 {
		t.Fatalf("price after update: want 19.99, got %v", found.Price)
	}

	if err := db.Delete(&p).Error; err != nil {
		t.Fatalf("delete: %v", err)
	}

	var count int64
	db.Model(&Product{}).Count(&count)
	if count != 0 {
		t.Fatalf("expected 0 records after delete, got %d", count)
	}
}

// TestPlugin_BatchMode_Create verifies that concurrent creates in batch mode
// all succeed and produce distinct records in the database.
func TestPlugin_BatchMode_Create(t *testing.T) {
	db := openBatchDB(t)

	plugin := autobatch.New(autobatch.Config{
		LatencyThreshold: durPtr(0), // always batch
		FlushTimeout:     20 * time.Millisecond,
		MaxBatchSize:     50,
		Logger: func(level autobatch.LogLevel, msg string, args ...any) {
			t.Logf("[%s] %s %v", level, msg, args)
		},
	})
	if err := db.Use(plugin); err != nil {
		t.Fatalf("db.Use: %v", err)
	}

	const n = 10
	var wg sync.WaitGroup
	errs := make([]error, n)

	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			p := Product{Name: "item", Price: float64(idx)}
			errs[idx] = db.Create(&p).Error
		}(i)
	}
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Errorf("goroutine %d create error: %v", i, err)
		}
	}

	var count int64
	db.Model(&Product{}).Count(&count)
	if count != n {
		t.Fatalf("expected %d records, got %d", n, count)
	}
}

// TestPlugin_BatchMode_Update verifies that concurrent updates in batch mode
// all succeed.
func TestPlugin_BatchMode_Update(t *testing.T) {
	db := openBatchDB(t)

	// Insert records without the plugin active.
	products := make([]Product, 5)
	for i := range products {
		products[i] = Product{Name: "before", Price: 1.0}
		if err := db.Create(&products[i]).Error; err != nil {
			t.Fatalf("seed create: %v", err)
		}
	}

	registerPlugin(t, db, autobatch.Config{
		LatencyThreshold: durPtr(0),
		FlushTimeout:     20 * time.Millisecond,
		MaxBatchSize:     50,
	})

	var wg sync.WaitGroup
	for i := range products {
		wg.Add(1)
		go func(p Product) {
			defer wg.Done()
			db.Model(&p).Update("Name", "after")
		}(products[i])
	}
	wg.Wait()

	var count int64
	db.Model(&Product{}).Where("name = ?", "after").Count(&count)
	if count != int64(len(products)) {
		t.Fatalf("expected %d updated records, got %d", len(products), count)
	}
}

// TestPlugin_BatchMode_Delete verifies that concurrent deletes in batch mode
// all succeed.
func TestPlugin_BatchMode_Delete(t *testing.T) {
	db := openBatchDB(t)

	products := make([]Product, 5)
	for i := range products {
		products[i] = Product{Name: "to-delete", Price: float64(i)}
		if err := db.Create(&products[i]).Error; err != nil {
			t.Fatalf("seed create: %v", err)
		}
	}

	registerPlugin(t, db, autobatch.Config{
		LatencyThreshold: durPtr(0),
		FlushTimeout:     20 * time.Millisecond,
		MaxBatchSize:     50,
	})

	var wg sync.WaitGroup
	for i := range products {
		wg.Add(1)
		go func(p Product) {
			defer wg.Done()
			db.Delete(&p)
		}(products[i])
	}
	wg.Wait()

	var count int64
	db.Model(&Product{}).Count(&count)
	if count != 0 {
		t.Fatalf("expected 0 records after delete, got %d", count)
	}
}

// TestPlugin_LatencyThreshold verifies the mode-switch: when latency is
// recorded above the threshold the plugin enters batch mode; when it drops
// below (window cleared / new plugin with high threshold) it goes back to
// individual mode.
func TestPlugin_LatencyThreshold(t *testing.T) {
	db := openDB(t)

	// Plugin starts in individual mode (high threshold, no recorded latency).
	registerPlugin(t, db, autobatch.Config{
		LatencyThreshold: durPtr(10 * time.Hour),
		FlushTimeout:     5 * time.Millisecond,
		MaxBatchSize:     100,
	})

	p := Product{Name: "solo", Price: 1.0}
	if err := db.Create(&p).Error; err != nil {
		t.Fatalf("individual create: %v", err)
	}
	if p.ID == 0 {
		t.Fatal("ID must be set in individual mode")
	}
}

// TestPlugin_Initialize_Idempotent verifies that registering the plugin twice
// returns an error (GORM prevents duplicate plugin names).
func TestPlugin_Initialize_Idempotent(t *testing.T) {
	db := openDB(t)
	cfg := autobatch.Config{LatencyThreshold: durPtr(50 * time.Millisecond)}
	if err := db.Use(autobatch.New(cfg)); err != nil {
		t.Fatalf("first Use: %v", err)
	}
	if err := db.Use(autobatch.New(cfg)); err == nil {
		t.Fatal("expected error on second Use with same plugin name, got nil")
	}
}

// TestPlugin_DefaultConfig verifies that zero-value Config uses sane defaults
// without panicking.
func TestPlugin_DefaultConfig(t *testing.T) {
	db := openDB(t)
	if err := db.Use(autobatch.New(autobatch.Config{})); err != nil {
		t.Fatalf("Use with zero Config: %v", err)
	}
	p := Product{Name: "default", Price: 3.14}
	if err := db.Create(&p).Error; err != nil {
		t.Fatalf("create with default config: %v", err)
	}
}

// TestPlugin_BatchMode_ErrorPropagation verifies that a batch-wide error (e.g.
// constraint violation) is returned to all callers in that batch.
func TestPlugin_BatchMode_ErrorPropagation(t *testing.T) {
	db := openBatchDB(t)

	// Add a unique constraint on Name.
	if err := db.Exec("CREATE UNIQUE INDEX idx_products_name ON products(name)").Error; err != nil {
		t.Fatalf("create index: %v", err)
	}

	// Pre-insert a record that will cause a conflict.
	if err := db.Exec("INSERT INTO products (name, price) VALUES ('conflict', 0)").Error; err != nil {
		t.Fatalf("seed: %v", err)
	}

	registerPlugin(t, db, autobatch.Config{
		LatencyThreshold: durPtr(0),
		FlushTimeout:     20 * time.Millisecond,
		MaxBatchSize:     50,
	})

	// Both goroutines try to insert the same name — the whole batch fails.
	var wg sync.WaitGroup
	errs := make([]error, 2)
	for i := range errs {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			errs[idx] = db.Create(&Product{Name: "conflict", Price: float64(idx)}).Error
		}(i)
	}
	wg.Wait()

	for i, err := range errs {
		if err == nil {
			t.Errorf("goroutine %d: expected error, got nil", i)
		}
	}
}

// TestPlugin_BatchMode_PerOpIsolation verifies that one failing op in a batch
// does not poison its neighbours: the good ops still commit, only the bad one
// gets the error. This is the savepoint-isolation guarantee.
func TestPlugin_BatchMode_PerOpIsolation(t *testing.T) {
	db := openBatchDB(t)

	if err := db.Exec("CREATE UNIQUE INDEX idx_iso_name ON products(name)").Error; err != nil {
		t.Fatalf("create index: %v", err)
	}
	if err := db.Exec("INSERT INTO products (name, price) VALUES ('dup', 0)").Error; err != nil {
		t.Fatalf("seed: %v", err)
	}

	registerPlugin(t, db, autobatch.Config{
		LatencyThreshold: durPtr(0),
		FlushTimeout:     20 * time.Millisecond,
		MaxBatchSize:     50,
	})

	// One bad op (duplicate "dup") and three good ops in the same batch.
	type result struct {
		err  error
		name string
	}
	results := make([]result, 4)
	names := []string{"dup", "alpha", "beta", "gamma"}

	var wg sync.WaitGroup
	for i, name := range names {
		wg.Add(1)
		go func(idx int, n string) {
			defer wg.Done()
			results[idx] = result{
				err:  db.Create(&Product{Name: n, Price: float64(idx)}).Error,
				name: n,
			}
		}(i, name)
	}
	wg.Wait()

	if results[0].err == nil {
		t.Error("expected duplicate insert to fail, got nil")
	}
	for i := 1; i < 4; i++ {
		if results[i].err != nil {
			t.Errorf("op %d (%q) should have succeeded but got: %v", i, results[i].name, results[i].err)
		}
	}

	var count int64
	db.Model(&Product{}).Where("name IN ?", []string{"alpha", "beta", "gamma"}).Count(&count)
	if count != 3 {
		t.Fatalf("expected 3 good rows committed, got %d", count)
	}
}

// TestPlugin_BatchMode_TableOverride verifies that Table() overrides are
// respected when the plugin flushes in batch mode.
func TestPlugin_BatchMode_TableOverride(t *testing.T) {
	db := openBatchDB(t)
	// Create an alias table.
	if err := db.Exec("CREATE TABLE products_alt AS SELECT * FROM products WHERE 0=1").Error; err != nil {
		t.Fatalf("create alt table: %v", err)
	}

	registerPlugin(t, db, autobatch.Config{
		LatencyThreshold: durPtr(0),
		FlushTimeout:     20 * time.Millisecond,
		MaxBatchSize:     50,
	})

	p := Product{Name: "alt", Price: 7.0}
	if err := db.Table("products_alt").Create(&p).Error; err != nil {
		t.Fatalf("create with table override: %v", err)
	}

	var count int64
	db.Table("products_alt").Count(&count)
	if count != 1 {
		t.Fatalf("expected 1 row in products_alt, got %d", count)
	}
	// Main table should be untouched.
	db.Model(&Product{}).Count(&count)
	if count != 0 {
		t.Fatalf("expected 0 rows in main products table, got %d", count)
	}
}

// TestPlugin_Disabled verifies that LatencyThreshold=nil disables batching:
// the plugin registers without error and all ops run individually (ID is set
// after create, which only happens when the real GORM callback executes).
func TestPlugin_Disabled(t *testing.T) {
	db := openDB(t)
	registerPlugin(t, db, autobatch.Config{
		LatencyThreshold: nil, // plugin disabled
	})

	p := Product{Name: "nodisable", Price: 1.0}
	if err := db.Create(&p).Error; err != nil {
		t.Fatalf("create: %v", err)
	}
	if p.ID == 0 {
		t.Fatal("ID must be set when plugin is disabled (individual mode)")
	}

	if err := db.Model(&p).Update("Price", 2.0).Error; err != nil {
		t.Fatalf("update: %v", err)
	}
	if err := db.Delete(&p).Error; err != nil {
		t.Fatalf("delete: %v", err)
	}

	var count int64
	db.Model(&Product{}).Count(&count)
	if count != 0 {
		t.Fatalf("expected 0 records after delete, got %d", count)
	}
}

// TestPlugin_AlwaysBatch verifies that LatencyThreshold=0 keeps batch mode
// permanently active: concurrent creates all land in the database.
func TestPlugin_AlwaysBatch(t *testing.T) {
	db := openBatchDB(t)
	registerPlugin(t, db, autobatch.Config{
		LatencyThreshold: durPtr(0), // always batch
		FlushTimeout:     20 * time.Millisecond,
		MaxBatchSize:     50,
	})

	const n = 5
	var wg sync.WaitGroup
	errs := make([]error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			errs[idx] = db.Create(&Product{Name: fmt.Sprintf("always-%d", idx), Price: float64(idx)}).Error
		}(i)
	}
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Errorf("goroutine %d: %v", i, err)
		}
	}

	var count int64
	db.Model(&Product{}).Count(&count)
	if count != n {
		t.Fatalf("expected %d records, got %d", n, count)
	}
}

// TestPlugin_Logger_Levels verifies that the Logger receives the correct level
// for each event type: Info on init/flush success, Warn on switch to batch,
// Debug on individual ops and enqueue, Error on batch failure.
func TestPlugin_Logger_Levels(t *testing.T) {
	type logEntry struct {
		level autobatch.LogLevel
		msg   string
	}

	var mu sync.Mutex
	var entries []logEntry

	capture := func(level autobatch.LogLevel, msg string, _ ...any) {
		mu.Lock()
		entries = append(entries, logEntry{level, msg})
		mu.Unlock()
	}

	hasLevel := func(level autobatch.LogLevel) bool {
		mu.Lock()
		defer mu.Unlock()
		for _, e := range entries {
			if e.level == level {
				return true
			}
		}
		return false
	}

	hasMsg := func(msg string) bool {
		mu.Lock()
		defer mu.Unlock()
		for _, e := range entries {
			if e.msg == msg {
				return true
			}
		}
		return false
	}

	db := openBatchDB(t)
	registerPlugin(t, db, autobatch.Config{
		LatencyThreshold: durPtr(0), // always batch → triggers Warn on first op
		FlushTimeout:     20 * time.Millisecond,
		MaxBatchSize:     50,
		Logger:           capture,
	})

	// Initialize emits Info.
	if !hasMsg("autobatch: initializing plugin") {
		t.Error("expected Info log for plugin initialization")
	}
	if !hasLevel(autobatch.LogLevelInfo) {
		t.Error("expected at least one Info log entry after initialize")
	}

	// Run an op to trigger batch mode transition (Warn) and flush success (Info).
	if err := db.Create(&Product{Name: "log-test", Price: 1.0}).Error; err != nil {
		t.Fatalf("create: %v", err)
	}

	// Wait for the async flush timer to fire.
	time.Sleep(50 * time.Millisecond)

	if !hasLevel(autobatch.LogLevelWarn) {
		t.Error("expected Warn log when switching to batch mode")
	}
	if !hasMsg("autobatch: switching to batch mode") {
		t.Error("expected batch mode transition message")
	}
	if !hasMsg("autobatch: batch flushed successfully") {
		t.Error("expected flush success Info log")
	}
	if !hasLevel(autobatch.LogLevelDebug) {
		t.Error("expected Debug logs for enqueue/flush")
	}
}

// TestPlugin_Logger_BatchError verifies that a failed batch transaction emits
// an Error-level log.
func TestPlugin_Logger_BatchError(t *testing.T) {
	db := openBatchDB(t)

	if err := db.Exec("CREATE UNIQUE INDEX idx_log_err_name ON products(name)").Error; err != nil {
		t.Fatalf("create index: %v", err)
	}
	if err := db.Exec("INSERT INTO products (name, price) VALUES ('dup', 0)").Error; err != nil {
		t.Fatalf("seed: %v", err)
	}

	var mu sync.Mutex
	var errorLogs []string
	registerPlugin(t, db, autobatch.Config{
		LatencyThreshold: durPtr(0),
		FlushTimeout:     20 * time.Millisecond,
		MaxBatchSize:     50,
		Logger: func(level autobatch.LogLevel, msg string, _ ...any) {
			if level == autobatch.LogLevelError {
				mu.Lock()
				errorLogs = append(errorLogs, msg)
				mu.Unlock()
			}
		},
	})

	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			db.Create(&Product{Name: "dup", Price: 1.0}) //nolint:errcheck
		}()
	}
	wg.Wait()

	time.Sleep(50 * time.Millisecond)

	mu.Lock()
	n := len(errorLogs)
	mu.Unlock()
	if n == 0 {
		t.Error("expected at least one Error log for failed batch transaction")
	}
}

// TestPlugin_Close_FlushesPendingBatch verifies that calling Close() drains a
// batch that was still waiting for its flush timer, so callers are unblocked
// and the data lands in the DB.
func TestPlugin_Close_FlushesPendingBatch(t *testing.T) {
	db := openBatchDB(t)
	plugin := autobatch.New(autobatch.Config{
		LatencyThreshold: durPtr(0),
		FlushTimeout:     10 * time.Second, // long: only Close() will drain
		MaxBatchSize:     1000,             // size won't trigger flush either
	})
	if err := db.Use(plugin); err != nil {
		t.Fatalf("Use: %v", err)
	}

	const n = 3
	var wg sync.WaitGroup
	errs := make([]error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			errs[idx] = db.Create(&Product{Name: fmt.Sprintf("close-%d", idx), Price: float64(idx)}).Error
		}(i)
	}

	// Give the goroutines time to enqueue.
	time.Sleep(50 * time.Millisecond)

	plugin.Close()
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Errorf("op %d should have succeeded after Close(): %v", i, err)
		}
	}

	var count int64
	db.Model(&Product{}).Where("name LIKE 'close-%'").Count(&count)
	if count != n {
		t.Fatalf("expected %d rows after Close(), got %d", n, count)
	}
}

// TestPlugin_Close_Idempotent verifies that calling Close() twice is safe.
func TestPlugin_Close_Idempotent(t *testing.T) {
	db := openDB(t)
	plugin := autobatch.New(autobatch.Config{LatencyThreshold: durPtr(0)})
	if err := db.Use(plugin); err != nil {
		t.Fatalf("Use: %v", err)
	}
	plugin.Close()
	plugin.Close() // must not panic
}

// TestPlugin_Close_RejectsNewOps verifies that ops submitted after Close()
// return ErrBatcherClosed to the caller.
func TestPlugin_Close_RejectsNewOps(t *testing.T) {
	db := openBatchDB(t)
	plugin := autobatch.New(autobatch.Config{
		LatencyThreshold: durPtr(0),
		FlushTimeout:     20 * time.Millisecond,
		MaxBatchSize:     50,
	})
	if err := db.Use(plugin); err != nil {
		t.Fatalf("Use: %v", err)
	}
	plugin.Close()

	err := db.Create(&Product{Name: "after-close", Price: 1.0}).Error
	if err != autobatch.ErrBatcherClosed {
		t.Fatalf("expected ErrBatcherClosed after Close(), got %v", err)
	}
}

// TestLogLevel_String verifies the String() method on LogLevel.
func TestLogLevel_String(t *testing.T) {
	cases := []struct {
		level autobatch.LogLevel
		want  string
	}{
		{autobatch.LogLevelDebug, "DEBUG"},
		{autobatch.LogLevelInfo, "INFO"},
		{autobatch.LogLevelWarn, "WARN"},
		{autobatch.LogLevelError, "ERROR"},
		{autobatch.LogLevel(99), "UNKNOWN"},
	}
	for _, c := range cases {
		if got := c.level.String(); got != c.want {
			t.Errorf("LogLevel(%d).String() = %q, want %q", c.level, got, c.want)
		}
	}
}
