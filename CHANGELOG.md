# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.0] - 2026-05-12

### Added
- Initial release
- P95 sliding window latency tracker (Prometheus-style hot/cold bucket ring)
- Automatic batch mode switching based on configurable latency threshold
- Batch support for `Create`, `Updates`, and `Delete` GORM operations
- Flush triggers by time (`FlushTimeout`) and size (`MaxBatchSize`)
- All-or-nothing transaction semantics per batch
- Thread-safe throughout with race detector verified
