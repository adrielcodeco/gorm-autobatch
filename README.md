# gorm-autobatch

A GORM plugin that automatically switches between individual and batch database operations based on measured P95 latency.

When latency is high, operations are buffered and flushed as a single transaction — reducing round-trips. When latency is low, operations are sent individually with no overhead.

## How it works

- Tracks operation latency using a **P95 sliding window** (Prometheus-style bucket ring, last 30s by default)
- When `P95 > LatencyThreshold` → **batch mode**: operations are buffered and flushed together
- When `P95 ≤ LatencyThreshold` → **individual mode**: operations pass through normally
- Flush triggers: elapsed time **or** buffer size, whichever comes first
- Batch semantics are **all-or-nothing** inside a single transaction

## Install

```bash
go get github.com/adrielcodeco/gorm-autobatch
```

> Requires Go 1.21+ and GORM v1.30+

## Usage

```go
import (
    autobatch "github.com/adrielcodeco/gorm-autobatch"
    "gorm.io/gorm"
)

db, err := gorm.Open(...)

err = db.Use(autobatch.New(autobatch.Config{
    LatencyThreshold: 50 * time.Millisecond, // switch to batch when P95 > 50ms
    FlushTimeout:     10 * time.Millisecond, // flush batch after 10ms idle
    MaxBatchSize:     100,                   // or when 100 ops are buffered
    WindowDuration:   30 * time.Second,      // P95 measured over last 30s
}))

// Regular GORM calls — the plugin decides whether to batch transparently.
db.Create(&user)
db.Model(&user).Updates(&payload)
db.Delete(&record)
```

## Config

| Field | Default | Description |
|---|---|---|
| `LatencyThreshold` | `50ms` | P95 above this switches to batch mode |
| `FlushTimeout` | `10ms` | Max wait before flushing a partial batch |
| `MaxBatchSize` | `100` | Max ops per batch before forced flush |
| `WindowDuration` | `30s` | Sliding window duration for P95 measurement |

All fields are optional — zero values use the defaults above.

## Supported operations

- `db.Create()`
- `db.Updates()`
- `db.Delete()`

Queries (`Find`, `First`, etc.) are not batched.

## Batch semantics

All operations in a batch run inside a **single transaction**. If any operation fails, the entire batch is rolled back and all callers receive the error. Callers block transparently until their batch is flushed — from the caller's perspective it looks like a normal synchronous GORM call.

## License

MIT
