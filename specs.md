# Requirements Document

## Introduction

The CDC Sync System is a Python-based unified migration tool that replicates data from
an Azure SQL Database (source) to an Amazon RDS for SQL Server instance (target). It
exists because AWS DMS does not currently support Change Data Capture (CDC) from Azure
SQL Database. The tool combines an initial bulk-load (snapshot) phase with a continuous
CDC streaming phase and a user-signaled cutover phase, providing a single long-running
process that can carry a database from "live on Azure" to "live on RDS" with at-least-once
delivery semantics and per-table consistency.

Replication is driven by Azure SQL CDC system functions
(`sys.fn_cdc_get_all_changes_<capture_instance>`) using per-table Log Sequence Number (LSN)
watermarks. Replication state is persisted in a control schema on the target RDS instance
so that the process can restart and resume without data loss or duplication beyond what
idempotent merge-by-primary-key allows. DDL replication is explicitly out of scope for v1;
the target schema is assumed to already exist.

## Glossary

- **CDC_Sync_System**: The complete tool described by this document. Used as the system
  name for ubiquitous requirements that apply across all components.
- **Snapshot_Engine**: The component responsible for the initial bulk load of existing
  source rows into the target.
- **CDC_Engine**: The component responsible for polling Azure SQL CDC change tables and
  applying changes to the target.
- **Cutover_Controller**: The component that handles the user-signaled cutover phase,
  draining CDC and running validation.
- **Validator**: The component that performs row-count (and optional checksum) comparisons
  between source and target.
- **State_Store**: The control schema and tables on the target RDS instance that persist
  per-table watermarks, snapshot progress, run state, and dead-letter records.
- **Config_Loader**: The component that loads and validates the YAML configuration file.
- **Secret_Resolver**: The component that resolves connection credentials from Azure Key
  Vault, AWS Secrets Manager, or environment variables.
- **Health_Server**: The HTTP endpoint that reports liveness and readiness.
- **Metrics_Server**: The Prometheus-format HTTP endpoint that exposes process metrics.
- **LSN**: Log Sequence Number. A monotonically non-decreasing 10-byte binary value
  produced by SQL Server / Azure SQL CDC that identifies a position in the transaction log.
- **Watermark**: The per-table LSN value recorded in the State_Store indicating the
  highest LSN whose changes have been durably applied to the target for that table.
- **Capture_Instance**: An Azure SQL CDC capture instance for a source table, accessed via
  `sys.fn_cdc_get_all_changes_<capture_instance>`.
- **Snapshot_Start_LSN**: The source database LSN captured immediately before a table's
  snapshot read begins. Used as the lower bound for that table's CDC stream.
- **DLQ**: Dead-letter quarantine table in the State_Store that stores change records
  which exhausted their retry budget.
- **Replicated_Table**: A source table listed in the configuration file as in-scope for
  replication.
- **Operational_Mode**: One of `SNAPSHOT`, `CDC`, or `CUTOVER`. The CDC_Sync_System
  transitions through these phases per table and at the process level.
- **Run**: A single invocation of the CDC_Sync_System process from start to either
  successful cutover or error exit.

## Requirements

### Requirement 1: Configuration Loading and Validation

**User Story:** As an operator, I want to define which tables to replicate and per-table
overrides in a YAML file, so that I can run the same tool against different workloads
without code changes.

#### Acceptance Criteria

1. WHEN the CDC_Sync_System starts, THE Config_Loader SHALL read the YAML configuration
   file located at the path specified by the `CONFIG_FILE_PATH` environment variable.
2. IF the `CONFIG_FILE_PATH` environment variable is not set, THEN THE Config_Loader
   SHALL exit with a non-zero status code and log an error identifying the missing
   variable.
3. IF the configuration file cannot be read or is not valid YAML, THEN THE Config_Loader
   SHALL exit with a non-zero status code and log the parse error.
4. THE Config_Loader SHALL validate the configuration against a Pydantic schema that
   defines the list of Replicated_Tables and supported per-table overrides.
5. IF the configuration fails Pydantic validation, THEN THE Config_Loader SHALL exit with
   a non-zero status code and log every validation error.
6. THE Config_Loader SHALL accept per-table overrides for at minimum: batch size, polling
   interval, column allowlist, and column denylist.
7. IF a Replicated_Table entry specifies both an allowlist and a denylist for columns,
   THEN THE Config_Loader SHALL exit with a non-zero status code and log a configuration
   error.
8. THE Config_Loader SHALL apply system-wide default values for any per-table override
   that is not explicitly set on a given Replicated_Table.

### Requirement 2: Secret Resolution

**User Story:** As an operator, I want connection credentials sourced from Azure Key Vault
or AWS Secrets Manager, so that secrets are not stored in plain text or environment
variables in production.

#### Acceptance Criteria

1. THE Secret_Resolver SHALL support resolving the source database connection from any
   of: Azure Key Vault, AWS Secrets Manager, or the `AZURE_SQL_CONNECTION_STRING`
   environment variable.
2. THE Secret_Resolver SHALL support resolving the target database connection from any
   of: Azure Key Vault, AWS Secrets Manager, or the `RDS_SQL_CONNECTION_STRING`
   environment variable.
3. THE Secret_Resolver SHALL allow asymmetric secret sources, meaning the source
   connection MAY be resolved from a different provider than the target connection within
   the same Run.
4. IF a configured secret provider is unreachable, THEN THE Secret_Resolver SHALL retry
   up to a configured maximum number of attempts before exiting with a non-zero status
   code.
5. WHEN the Secret_Resolver successfully resolves a connection string, THE
   CDC_Sync_System SHALL NOT log the connection string value or any password contained
   within it.
6. WHERE a `.env` file is present in the working directory, THE Secret_Resolver SHALL
   load environment variables from the file before resolving secrets.

### Requirement 3: State Store Initialization

**User Story:** As an operator, I want replication state stored on the target RDS
instance, so that the tool can restart and resume without external state services.

#### Acceptance Criteria

1. WHEN the CDC_Sync_System starts a new Run, THE State_Store SHALL ensure the existence
   of a control schema on the target RDS instance with a configurable name (default
   `cdc_sync`).
2. THE State_Store SHALL ensure the existence of tables for: per-table watermarks,
   per-table snapshot progress, run state, and DLQ records.
3. WHEN State_Store tables already exist, THE State_Store SHALL NOT drop or recreate
   them.
4. IF the target RDS user lacks permission to create the control schema or its tables,
   THEN THE CDC_Sync_System SHALL exit with a non-zero status code and log the missing
   privilege.
5. THE State_Store SHALL persist watermark updates inside the same transaction that
   applies the corresponding batch of changes to the target user tables, so that a
   watermark value never advances beyond changes that have been durably applied.

### Requirement 4: Primary-Key Precondition

**User Story:** As an operator, I want the tool to fail fast when a configured table
lacks a primary or unique key, so that I do not start a Run that cannot guarantee
idempotent CDC apply.

#### Acceptance Criteria

1. WHEN the CDC_Sync_System starts a new Run, THE CDC_Sync_System SHALL verify that every
   Replicated_Table has a primary key or a unique key on the source.
2. IF any Replicated_Table lacks both a primary key and a unique key on the source, THEN
   THE CDC_Sync_System SHALL exit with a non-zero status code, log every offending table,
   and SHALL NOT begin snapshot or CDC.
3. WHEN the CDC_Sync_System starts a new Run, THE CDC_Sync_System SHALL verify that every
   Replicated_Table has a corresponding table on the target with a matching primary or
   unique key.
4. IF any Replicated_Table is missing on the target or has a mismatched key definition,
   THEN THE CDC_Sync_System SHALL exit with a non-zero status code and log every
   offending table.

### Requirement 5: Snapshot Phase

**User Story:** As an operator, I want the tool to bulk-load existing source data into
the target before streaming CDC, so that the target reflects the full source state.

#### Acceptance Criteria

1. WHEN the Snapshot_Engine begins snapshotting a Replicated_Table, THE Snapshot_Engine
   SHALL record a Snapshot_Start_LSN obtained from the source database before reading any
   rows.
2. THE Snapshot_Engine SHALL persist the Snapshot_Start_LSN in the State_Store before
   reading any rows for that table.
3. WHEN the Snapshot_Engine reads rows for a Replicated_Table, THE Snapshot_Engine SHALL
   read in batches of the configured batch size for that table.
4. WHEN the Snapshot_Engine writes rows to the target, THE Snapshot_Engine SHALL write
   using an idempotent merge-by-primary-key operation so that re-running a partially
   completed snapshot does not produce duplicate rows.
5. WHEN the Snapshot_Engine completes a Replicated_Table, THE Snapshot_Engine SHALL
   record the table's snapshot status as `COMPLETE` in the State_Store and set the
   table's CDC starting watermark to the Snapshot_Start_LSN.
6. WHEN the CDC_Sync_System restarts during the snapshot phase of a Replicated_Table,
   THE Snapshot_Engine SHALL resume that table's snapshot from the last persisted
   progress checkpoint.
7. IF a Replicated_Table's snapshot has already completed in a previous Run, THEN THE
   Snapshot_Engine SHALL skip that table and proceed directly to CDC for it.
8. WHEN every Replicated_Table reaches snapshot status `COMPLETE`, THE CDC_Sync_System
   SHALL transition its Operational_Mode to `CDC`.

### Requirement 6: Snapshot Validation

**User Story:** As an operator, I want row counts compared between source and target
after snapshot, so that I can detect bulk-load defects before CDC begins.

#### Acceptance Criteria

1. WHEN a Replicated_Table reaches snapshot status `COMPLETE`, THE Validator SHALL
   compare the source row count taken at Snapshot_Start_LSN with the target row count
   for that table.
2. IF the source and target row counts for a Replicated_Table differ after snapshot,
   THEN THE Validator SHALL record a validation failure in the State_Store and emit a
   metric counter incrementing by one.
3. WHERE checksum validation is enabled in configuration for a Replicated_Table, THE
   Validator SHALL compute a deterministic row-hash aggregate over the snapshot range on
   both source and target and compare them.
4. IF a snapshot validation failure is recorded, THEN THE CDC_Sync_System SHALL continue
   to CDC for the affected table only when the configuration sets
   `continue_on_validation_failure` to `true`.

### Requirement 7: CDC Streaming Phase

**User Story:** As an operator, I want continuous polling of Azure SQL CDC change tables
with per-table watermarks, so that ongoing source changes are applied to the target in
near real time.

#### Acceptance Criteria

1. WHILE the CDC_Sync_System Operational_Mode is `CDC`, THE CDC_Engine SHALL poll each
   Replicated_Table's Capture_Instance at the table's configured polling interval.
2. WHEN the CDC_Engine polls a Replicated_Table, THE CDC_Engine SHALL request changes
   in the LSN range `(last_committed_watermark, current_source_max_lsn]` using
   `sys.fn_cdc_get_all_changes_<capture_instance>`.
3. WHEN the CDC_Engine receives a batch of change rows, THE CDC_Engine SHALL apply
   inserts, updates, and deletes to the target table using an idempotent merge-by-primary-key
   operation.
4. WHEN the CDC_Engine successfully applies a batch of change rows, THE CDC_Engine SHALL
   advance the Replicated_Table's watermark to the maximum applied LSN within the same
   transaction as the apply.
5. THE CDC_Engine SHALL ensure that a Replicated_Table's watermark never decreases across
   any pair of successful poll cycles within a Run.
6. WHEN the CDC_Sync_System restarts, THE CDC_Engine SHALL resume polling each
   Replicated_Table from the watermark persisted in the State_Store.
7. WHEN no changes are returned for a Replicated_Table in a poll cycle, THE CDC_Engine
   SHALL leave the table's watermark unchanged.
8. WHILE the CDC_Engine is processing changes for one Replicated_Table, THE CDC_Engine
   SHALL NOT block polling of other Replicated_Tables.

### Requirement 8: Apply Retry and Dead-Letter Quarantine

**User Story:** As an operator, I want failed change applications retried a bounded
number of times and then quarantined, so that one bad row does not stall replication
for an entire table.

#### Acceptance Criteria

1. IF the CDC_Engine fails to apply a change record to the target, THEN THE CDC_Engine
   SHALL retry the apply up to 5 times with exponential backoff.
2. IF a change record fails apply on the 5th retry, THEN THE CDC_Engine SHALL write the
   change record, the source LSN, the table name, and the last error message to the DLQ
   table in the State_Store.
3. WHEN a change record is written to the DLQ, THE CDC_Engine SHALL advance the
   table's watermark past the failed LSN within the same transaction as the DLQ write,
   so that a quarantined record does not block subsequent changes.
4. WHEN a change record is written to the DLQ, THE CDC_Engine SHALL emit a metric
   counter incrementing by one and log a structured warning containing the table name
   and source LSN.
5. THE DLQ table SHALL retain quarantined records until an operator removes them.

### Requirement 9: DDL Change Detection

**User Story:** As an operator, I want the tool to pause and alert when source DDL
changes occur, so that I do not silently corrupt the target schema.

#### Acceptance Criteria

1. WHEN the CDC_Engine detects a DDL change for a Replicated_Table via Azure SQL CDC's
   DDL history function, THE CDC_Engine SHALL stop polling that table and record the
   table's run state as `PAUSED_DDL` in the State_Store.
2. WHEN a Replicated_Table is in run state `PAUSED_DDL`, THE CDC_Engine SHALL emit a
   metric counter incrementing by one per detection cycle and log a structured error
   containing the table name and the DDL statement.
3. WHILE any Replicated_Table is in run state `PAUSED_DDL`, THE Cutover_Controller
   SHALL refuse to begin cutover and SHALL log the offending tables.
4. THE CDC_Sync_System SHALL NOT auto-propagate any DDL change in v1.

### Requirement 10: Cutover Phase

**User Story:** As an operator, I want a user-triggered cutover that drains CDC to the
current source LSN and validates row counts, so that I can confidently switch application
traffic to the target.

#### Acceptance Criteria

1. WHEN the operator issues the cutover command, THE Cutover_Controller SHALL transition
   the CDC_Sync_System Operational_Mode to `CUTOVER`.
2. WHILE Operational_Mode is `CUTOVER`, THE CDC_Engine SHALL continue polling but SHALL
   set the upper bound of each poll's LSN range to a single fixed `cutover_target_lsn`
   captured at cutover start.
3. WHEN every Replicated_Table's watermark reaches the `cutover_target_lsn`, THE
   Cutover_Controller SHALL trigger the Validator to compare source and target row counts
   for every Replicated_Table.
4. IF the Validator reports any row count mismatch during cutover, THEN the
   Cutover_Controller SHALL exit the process with a non-zero status code and log every
   offending table.
5. IF every Replicated_Table passes cutover validation, THEN the Cutover_Controller
   SHALL mark the Run state as `COMPLETED` in the State_Store and exit the process with
   status code zero.
6. IF cutover does not complete within a configured cutover timeout, THEN the
   Cutover_Controller SHALL exit with a non-zero status code and log the tables that did
   not reach `cutover_target_lsn`.

### Requirement 11: Restart and Resume Semantics

**User Story:** As an operator, I want the tool to resume cleanly after a process crash
or restart, so that I do not lose progress or duplicate already-applied rows.

#### Acceptance Criteria

1. WHEN the CDC_Sync_System starts, THE CDC_Sync_System SHALL read the Run state and
   per-table watermarks from the State_Store before opening any source CDC cursor.
2. WHEN the CDC_Sync_System resumes a Run interrupted during snapshot, THE
   Snapshot_Engine SHALL resume each Replicated_Table from the last persisted snapshot
   progress checkpoint.
3. WHEN the CDC_Sync_System resumes a Run interrupted during CDC, THE CDC_Engine SHALL
   resume each Replicated_Table from the persisted watermark.
4. THE CDC_Sync_System SHALL guarantee at-least-once delivery of every change row whose
   source LSN exceeds a Replicated_Table's last persisted watermark.
5. THE CDC_Sync_System SHALL rely on idempotent merge-by-primary-key apply to absorb any
   duplicate delivery that results from at-least-once semantics.

### Requirement 12: Observability — Logging

**User Story:** As an operator, I want structured JSON logs, so that I can ingest and
query operational events.

#### Acceptance Criteria

1. THE CDC_Sync_System SHALL emit logs in JSON format using `structlog`.
2. THE CDC_Sync_System SHALL set the minimum log level from the `LOG_LEVEL` environment
   variable, defaulting to `INFO` when unset.
3. THE CDC_Sync_System SHALL include at minimum the following fields in every log
   record: timestamp, level, logger name, message, run id, and (where applicable) table
   name and source LSN.
4. THE CDC_Sync_System SHALL NOT log connection strings, passwords, or secret values.

### Requirement 13: Observability — Metrics

**User Story:** As an operator, I want Prometheus metrics, so that I can alert on
replication lag and error rates.

#### Acceptance Criteria

1. THE Metrics_Server SHALL expose Prometheus-format metrics over HTTP on the port
   specified by the `PROMETHEUS_PORT` environment variable.
2. THE Metrics_Server SHALL expose at minimum: per-table rows applied counter, per-table
   apply error counter, per-table DLQ counter, per-table replication lag in seconds,
   per-table current watermark, and Operational_Mode gauge.
3. WHEN the CDC_Sync_System fails to bind to `PROMETHEUS_PORT`, THE CDC_Sync_System
   SHALL exit with a non-zero status code and log the bind error.

### Requirement 14: Observability — Health Check

**User Story:** As an operator, I want a health endpoint, so that orchestration tooling
can detect when the process is unhealthy.

#### Acceptance Criteria

1. THE Health_Server SHALL expose an HTTP endpoint on the port specified by the
   `HEALTH_CHECK_PORT` environment variable.
2. WHEN the CDC_Sync_System has loaded configuration, resolved secrets, and connected
   to both source and target, THE Health_Server SHALL respond to `GET /health` with HTTP
   status 200.
3. IF the CDC_Sync_System has lost its connection to either source or target for longer
   than a configured threshold, THEN THE Health_Server SHALL respond to `GET /health`
   with HTTP status 503.
4. WHEN the CDC_Sync_System fails to bind to `HEALTH_CHECK_PORT`, THE CDC_Sync_System
   SHALL exit with a non-zero status code and log the bind error.

### Requirement 15: Per-Table Independence and Workload Generality

**User Story:** As an operator, I want the tool to work for varying table counts and
change rates without code changes, so that I can use the same binary across migrations.

#### Acceptance Criteria

1. THE CDC_Sync_System SHALL NOT contain table-specific logic, table-specific names, or
   table-specific schema in source code.
2. THE CDC_Sync_System SHALL handle a configuration listing one or more Replicated_Tables
   without modification.
3. THE CDC_Sync_System SHALL track and advance watermarks per table independently, so
   that a stalled table does not block progress on other tables.

### Requirement 16: Backpressure Policy

**User Story:** As an operator, I want the tool to buffer changes rather than slowing
the source, so that source application performance is not affected.

#### Acceptance Criteria

1. WHILE the CDC_Engine is applying changes more slowly than the source produces them,
   THE CDC_Engine SHALL continue to read and buffer changes without throttling the
   source.
2. IF the CDC_Engine's in-memory buffer for a Replicated_Table exceeds a configured
   threshold, THEN THE CDC_Engine SHALL emit a metric counter and log a structured
   warning, but SHALL NOT throttle source reads in v1.

### Requirement 17: Correctness Properties (for Property-Based Testing)

**User Story:** As an engineer, I want explicit correctness properties, so that I can
write property-based tests that exercise the CDC_Sync_System over many generated inputs.

#### Acceptance Criteria

1. **Watermark monotonicity (invariant):** FOR ALL Replicated_Tables and FOR ALL pairs
   of successful poll cycles `(p_i, p_{i+1})` within a Run, THE CDC_Engine SHALL ensure
   `watermark(p_{i+1}) >= watermark(p_i)`.
2. **Apply idempotence:** FOR ALL change records `c`, applying `c` to the target one or
   more times SHALL produce the same target row state as applying `c` exactly once.
3. **Snapshot/CDC boundary completeness:** FOR ALL Replicated_Tables `t`, the union of
   rows produced by snapshot at `Snapshot_Start_LSN(t)` with the change stream
   `(Snapshot_Start_LSN(t), cutover_target_lsn]` SHALL equal the source row state at
   `cutover_target_lsn`, modulo rows quarantined to the DLQ.
4. **Snapshot/CDC boundary non-duplication:** FOR ALL Replicated_Tables `t`, no source
   row SHALL appear in the target more than once as a result of overlap between snapshot
   and CDC at `Snapshot_Start_LSN(t)`.
5. **Restart resume correctness:** FOR ALL Runs interrupted at any point and restarted,
   the final target row state at successful cutover SHALL equal the final target row
   state of an equivalent uninterrupted Run, modulo DLQ membership.
6. **Retry exhaustion implies DLQ:** FOR ALL change records `c` whose apply fails on
   every retry attempt, `c` SHALL appear in the DLQ exactly once, and the watermark for
   `c`'s table SHALL advance past `LSN(c)`.
7. **Watermark/apply atomicity:** FOR ALL successful apply transactions, the persisted
   watermark advancement and the user-table change SHALL either both commit or both
   roll back.
8. **Configuration round-trip:** FOR ALL valid configuration objects `cfg`, parsing the
   YAML serialization of `cfg` SHALL produce a configuration object equivalent to `cfg`.
9. **DLQ no-loss:** FOR ALL change records `c` whose retries are exhausted, `c` SHALL be
   recoverable from the DLQ without re-reading the source CDC stream.

### Requirement 18: Process Lifecycle and Signals

**User Story:** As an operator, I want the tool to handle shutdown signals gracefully,
so that I can stop it without corrupting state.

#### Acceptance Criteria

1. WHEN the CDC_Sync_System receives `SIGTERM` or `SIGINT`, THE CDC_Sync_System SHALL
   stop accepting new poll cycles, finish any in-flight apply transactions, persist
   watermarks, and exit with status code zero.
2. IF an in-flight apply transaction cannot complete within a configured shutdown grace
   period, THEN THE CDC_Sync_System SHALL roll back the transaction and exit with a
   non-zero status code.
3. WHEN the operator issues the cutover command via CLI or signal, THE Cutover_Controller
   SHALL begin the cutover phase as defined in Requirement 10.

### Requirement 19: Out-of-Scope (TODO for Future Work)

**User Story:** As a stakeholder, I want explicit non-goals captured, so that v1 scope is
unambiguous and future work is recorded.

#### Acceptance Criteria

1. THE CDC_Sync_System v1 SHALL NOT replicate DDL (tables, views, stored procedures,
   constraints, indexes, or triggers) from source to target.
2. THE CDC_Sync_System v1 SHALL NOT enforce cross-table transactional consistency on the
   target.
3. THE CDC_Sync_System v1 SHALL NOT throttle the source under backpressure.
4. THE CDC_Sync_System v1 SHALL NOT auto-recover or auto-retry DLQ records; operators
   reprocess DLQ records manually.
5. The following items are recorded as TODOs for future versions: DDL replication,
   automated DLQ reprocessing, source throttling under backpressure, cross-table
   transactional consistency, and containerized/Kubernetes deployment manifests.
