package autobatch_test

import (
	"fmt"
	"time"

	autobatch "github.com/adrielcodeco/gorm-autobatch"
	"gorm.io/gorm"
)

type User struct {
	ID   uint
	Name string
}

func Example() {
	// Open your GORM DB as usual (dialector omitted for brevity).
	var db *gorm.DB // = gorm.Open(...)

	err := db.Use(autobatch.New(autobatch.Config{
		LatencyThreshold: 50 * time.Millisecond, // switch to batch when P95 > 50ms
		FlushTimeout:     10 * time.Millisecond, // flush batch after 10ms idle
		MaxBatchSize:     100,                   // or when 100 ops are buffered
		WindowDuration:   30 * time.Second,      // P95 measured over last 30s
	}))
	if err != nil {
		fmt.Println("failed to register plugin:", err)
		return
	}

	// Regular GORM calls — the plugin decides whether to batch transparently.
	user := User{Name: "Alice"}
	db.Create(&user)
}
