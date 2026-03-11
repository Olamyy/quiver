# Changelog

All notable changes to Quiver are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- Multi-platform release process (Linux, macOS, Windows)
- Docker container distribution via GHCR
- Release workflow with automated binary compilation and checksums

## [0.0.1] - 2026-03-15

### Added
- **Fanout Execution Engine**: Parallel multi-backend feature resolution with Arrow-native merging
  - Distributed feature requests across multiple adapters in parallel
  - 4-phase Arrow LEFT JOIN merge algorithm for sparse result semantics
  - Clock-skew aware temporal routing with 30s default safety margin
  - 11-point instrumentation and metrics pipeline

- **Observability Service**: Dedicated metrics exposure and request tracking
  - Request-scoped UUID tracking across distributed backend calls
  - In-memory metrics store with LRU eviction and 1-hour TTL
  - GetMetrics RPC on port 8816 for latency inspection
  - FlushMetricsStore admin RPC for cache testing

- **Benchmark Suite**: Statistical performance analysis framework
  - Criterion-based benchmarking with statistical analysis
  - Dynamic fixture generation supporting configurable entity counts
  - 4 core scenarios: Redis baseline, PostgreSQL baseline, Fanout 2x, Fanout 3x
  - Batch scaling tests (1-10k entities) with HTML reports
  - Unified `benchmark.sh` workflow script for end-to-end testing
  - Cold cache measurements with iterative flushing

### Fixed
- Object store v0.13.1 compatibility: Proper `ObjectStoreExt` imports for S3/Parquet adapter
- Entity-aligned merge algorithm eliminates O(n*m) bottleneck in multi-backend scenarios
- Adapter ordering guarantees enable fast-path optimization for request-ordered results
- Timestamp timezone enforcement prevents ambiguous temporal queries (UTC-only)

### Changed
- **Adapter Interface**: Enhanced `get_with_resolutions()` replaces `get()` for type-aware operations
  - Adapters now receive explicit type + source path metadata from resolver
  - Enables per-feature type consistency validation
  - Backwards compatible: default implementation wraps `get()`

- **Resolver Signature**: Returns `(RecordBatch, FanoutLatencies)` tuple
  - Latencies include registry lookup, partition, dispatch, per-adapter timing, merge duration
  - Server stores metrics indexed by request UUID

- **Configuration Structure**: FanoutServerConfig with validation rules
  - Per-adapter failure handling policies (allow_partial, require_all)
  - Temporal routing margin configuration (clock-skew tolerance)
  - Ordered result hints for performance optimization

### Performance Improvements
- Parallel backend dispatch: ~15-20% latency improvement vs sequential
- Fanout 2x scenario: ~2-3% faster than single Redis (merge overhead negligible)
- Merge algorithm: O(n) complexity with entity-aligned streaming
- Sparse result handling: Null-filling without row reordering

### Testing
- 135+ tests across all phases (unit + integration)
- Multi-adapter E2E testing: PostgreSQL, Redis, S3/Parquet verified
- Windows CI integration with all test scenarios passing
- Observability metrics validation with 23 instrumentation tests
- Cold cache benchmark verification with 8+ scenario variations

## Version History Format

Each release section follows this structure:

```markdown
## [X.Y.Z] - YYYY-MM-DD

### Added
- Major new features and capabilities

### Fixed
- Bug fixes and corrections

### Changed
- Breaking changes and modifications to existing behavior

### Deprecated
- Features scheduled for removal

### Removed
- Features that have been removed

### Performance
- Performance improvements and optimizations

### Testing
- Test coverage and validation improvements

### Security
- Security fixes and improvements
```

---

