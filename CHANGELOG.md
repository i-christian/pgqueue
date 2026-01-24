# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.5.0] - 2026-01-24

### Added
- **Functional Option `WithBatchSize`**: Users can now configure the number of tasks fetched per database round-trip (defaults to 10).
- **Worker Startup Jitter**: Introduced a random 0-2s delay when starting workers to prevent synchronized database spikes (Thundering Herd) during deployments.
- **Formal Documentation**: Added comprehensive GoDoc comments for all public types and methods.
- **Contribution Guide**: Added a contribution section to the `README.md`.

### Changed
- **Worker Loop Refactor**: Re-engineered the core loop into a state machine that clearly separates "Drain," "Idle," and "Error Backoff" states.
- **Aggressive Error Recovery**: Database errors now trigger a specific exponential backoff (starting at 100ms) that retries independently of the system ticker, improving recovery speed from transient connection blips.
- **Notification Efficiency**: Optimized the `LISTEN/NOTIFY` handling to eliminate redundant database queries when multiple notifications arrive in a short window.
- **Batching Defaults**: introduced a batch task processing which defaults to 10 to improve high-throughput performance.

### Fixed
- **The "Double-Dip" Logic Bug**: Fixed an issue where a worker would query the database an extra time immediately after successfully processing a batch, even if no new tasks were available.
- **Context Handling**: Improved `ctx.Done()` checks throughout the worker lifecycle to ensure immediate response to `Shutdown()` signals.
- **Log Noise**: Standardized error logging to prevent log flooding during sustained database outages.

---
