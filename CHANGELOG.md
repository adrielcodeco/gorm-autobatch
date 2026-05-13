# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.2.0] - 2026-05-13

### Added
- `Plugin.Close()` — drains in-flight batches and rejects new submits with `ErrBatcherClosed`.
- Per-op `SAVEPOINT` isolation in batch flush: one failing op no longer poisons unrelated ops in the same batch.
- `RowsAffected` is now propagated from the batch back to the caller's `*gorm.DB`.
- New tests for `Close()`, savepoint isolation, transaction safety, stale-timer races, and long-idle bucket rotation. Coverage: 92.9%.
- README documents the limitations (skipped hooks, savepoint semantics, graceful shutdown).

### Changed
- **Breaking:** `Config.LatencyThreshold` is now `*time.Duration` with ternary semantics — `nil` disables batching, `0` always batches, `>0` is adaptive.
- **Breaking:** Replaced `db.DryRun = true` with an internal sentinel error to skip the core callback. Removes a data race on the shared `*gorm.Config.DryRun` field when multiple goroutines submit through the same root `*gorm.DB`.
- `Statement.Dest/Model/Table` are snapshotted at submit time; the flush goroutine no longer reads from the caller's live `Statement`.
- Configuration is captured immutably inside the plugin; mutating `Config` fields after `New` no longer affects the running plugin.
- Latency window's async drainer is coalesced via a single in-flight flag instead of spawning a goroutine per `Record` call.
- `rotateBuckets` is capped at `len(buckets)` iterations with a fast-forward fallback, preventing runaway loops after long process suspensions.
- Integration tests run against a SQLite file in `t.TempDir()` (WAL mode) instead of a PostgreSQL testcontainer — suite is ~8× faster.

### Fixed
- Loss-of-write bug when an op was enqueued inside `db.Transaction(...)`: explicit user transactions are now reliably detected via the `gorm:started_transaction` instance key, and a regression test guards the detection.
- Timer race in `batcher.submit`: a stale `time.AfterFunc` callback could clobber a freshly armed timer. Generation guard added.
- Context cancellation is re-checked immediately before each op runs inside the flush transaction.

### Removed
- Dependency on `testcontainers-go` and the PostgreSQL driver from the test suite.

## [0.1.0] - 2026-05-12

### Added
- Initial release
- P95 sliding window latency tracker (Prometheus-style hot/cold bucket ring)
- Automatic batch mode switching based on configurable latency threshold
- Batch support for `Create`, `Updates`, and `Delete` GORM operations
- Flush triggers by time (`FlushTimeout`) and size (`MaxBatchSize`)
- All-or-nothing transaction semantics per batch
- Thread-safe throughout with race detector verified
