# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

CDC Sync System — a Python tool that replicates data from Azure SQL Database to Amazon RDS for SQL Server. It exists because AWS DMS does not support CDC from Azure SQL Database. The tool combines snapshot (bulk-load), continuous CDC streaming, and user-signaled cutover phases into a single long-running process with at-least-once delivery and per-table consistency.

Replication uses Azure SQL CDC system functions (`sys.fn_cdc_get_all_changes_<capture_instance>`) with per-table LSN watermarks. State is persisted in a control schema on target RDS so the process can restart/resume without data loss.

## Architecture

Six main components:

- **Config_Loader** — Reads YAML config (`CONFIG_FILE_PATH` env var), validates via Pydantic. Per-table overrides: batch size, polling interval, column allowlist/denylist.
- **Secret_Resolver** — Resolves connection credentials from Azure Key Vault, AWS Secrets Manager, or env vars. Supports asymmetric sources (e.g., Azure KV for source, AWS SM for target).
- **State_Store** — Control schema on target RDS (default `cdc_sync`). Tables for: per-table watermarks, snapshot progress, run state, DLQ records. Watermark updates are transactional with data applies.
- **Snapshot_Engine** — Bulk-loads existing source rows using idempotent merge-by-PK. Records Snapshot_Start_LSN before reading, checkpoints progress for resumability.
- **CDC_Engine** — Polls capture instances per-table at configured intervals. Applies inserts/updates/deletes via merge-by-PK. Retries failed records 5x with exponential backoff, then quarantines to DLQ.
- **Cutover_Controller** — User-triggered. Drains CDC to a fixed `cutover_target_lsn`, validates row counts, marks run COMPLETED on success.

Supporting components: Health_Server (HTTP liveness/readiness), Metrics_Server (Prometheus), Validator (row-count and optional checksum comparisons).

## Operational Modes

Tables transition through: `SNAPSHOT` → `CDC` → `CUTOVER`. Process-level mode advances when all tables reach the next phase. A table can also enter `PAUSED_DDL` if DDL changes are detected (no auto-propagation in v1).

## Key Design Constraints

- DDL replication is out of scope for v1.
- Target schema must pre-exist.
- At-least-once delivery; idempotent merge-by-PK absorbs duplicates.
- No table-specific logic in source code — everything is config-driven.
- Per-table watermarks are independent; a stalled table does not block others.
- Watermark advancement and data apply must share the same DB transaction.
- Graceful shutdown on SIGTERM/SIGINT: finish in-flight transactions, persist watermarks.

## Environment Variables

See `.env.example` for the full set. Key variables:

| Variable | Purpose |
|----------|---------|
| `CONFIG_FILE_PATH` | Path to YAML config file |
| `AZURE_SQL_CONNECTION_STRING` | Source connection (if not using vault) |
| `RDS_SQL_CONNECTION_STRING` | Target connection (if not using vault) |
| `LOG_LEVEL` | Structured JSON log level (default: INFO) |
| `PROMETHEUS_PORT` | Metrics endpoint port |
| `HEALTH_CHECK_PORT` | Health endpoint port |

## Tech Stack

- Python (structured logging via `structlog`, validation via Pydantic)
- Azure SQL CDC functions as the change source
- SQL Server on RDS as the target
- Prometheus for metrics exposition
- Azure Key Vault / AWS Secrets Manager for secret resolution
