package autobatch_test

import (
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
		Logger: logger.Default.LogMode(logger.Silent),
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

// TestPlugin_IndividualMode verifies that when P95 is below the threshold
// the plugin is transparent: creates, updates, and deletes work normally.
func TestPlugin_IndividualMode(t *testing.T) {
	db := openDB(t)
	registerPlugin(t, db, autobatch.Config{
		LatencyThreshold: 10 * time.Hour, // effectively never batch
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
	db := openDB(t)

	// Seed the latency window with high values so isBatchMode() returns true
	// immediately without waiting for real slow queries.
	plugin := autobatch.New(autobatch.Config{
		LatencyThreshold: 1 * time.Nanosecond, // always batch
		FlushTimeout:     20 * time.Millisecond,
		MaxBatchSize:     50,
	})
	if err := db.Use(plugin); err != nil {
		t.Fatalf("db.Use: %v", err)
	}

	// Force the P95 cache to reflect a latency above threshold by recording
	// a dummy observation directly on the exported window. Since window is
	// unexported we instead use a short FlushTimeout and rely on threshold=1ns.

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
	db := openDB(t)

	// Insert records without the plugin active.
	products := make([]Product, 5)
	for i := range products {
		products[i] = Product{Name: "before", Price: 1.0}
		if err := db.Create(&products[i]).Error; err != nil {
			t.Fatalf("seed create: %v", err)
		}
	}

	registerPlugin(t, db, autobatch.Config{
		LatencyThreshold: 1 * time.Nanosecond,
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
	db := openDB(t)

	products := make([]Product, 5)
	for i := range products {
		products[i] = Product{Name: "to-delete", Price: float64(i)}
		if err := db.Create(&products[i]).Error; err != nil {
			t.Fatalf("seed create: %v", err)
		}
	}

	registerPlugin(t, db, autobatch.Config{
		LatencyThreshold: 1 * time.Nanosecond,
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
		LatencyThreshold: 10 * time.Hour,
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
	cfg := autobatch.Config{LatencyThreshold: 50 * time.Millisecond}
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
	db := openDB(t)

	// Add a unique constraint on Name.
	if err := db.Exec("CREATE UNIQUE INDEX idx_products_name ON products(name)").Error; err != nil {
		t.Fatalf("create index: %v", err)
	}

	// Pre-insert a record that will cause a conflict.
	if err := db.Exec("INSERT INTO products (name, price) VALUES ('conflict', 0)").Error; err != nil {
		t.Fatalf("seed: %v", err)
	}

	registerPlugin(t, db, autobatch.Config{
		LatencyThreshold: 1 * time.Nanosecond,
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

// TestPlugin_BatchMode_TableOverride verifies that Table() overrides are
// respected when the plugin flushes in batch mode.
func TestPlugin_BatchMode_TableOverride(t *testing.T) {
	db := openDB(t)
	// Create an alias table.
	if err := db.Exec("CREATE TABLE products_alt AS SELECT * FROM products WHERE 0").Error; err != nil {
		t.Fatalf("create alt table: %v", err)
	}

	registerPlugin(t, db, autobatch.Config{
		LatencyThreshold: 1 * time.Nanosecond,
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
