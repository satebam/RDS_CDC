# CDC Sync System

Replicates data from Azure SQL Database to Amazon RDS for SQL Server using Change Data Capture (CDC). Built because AWS DMS does not support CDC from Azure SQL Database.

The tool combines three phases into a single long-running process:
1. **Snapshot** — bulk-loads existing source rows into the target
2. **CDC** — continuously polls Azure SQL CDC change tables and applies changes
3. **Cutover** — drains CDC to a fixed LSN, validates row counts, and marks the run complete

## Prerequisites

### System Dependencies

**macOS:**
```bash
brew install unixodbc
brew tap microsoft/mssql-release https://github.com/Microsoft/homebrew-mssql-release
brew install msodbcsql18
```

**Linux (Debian/Ubuntu):**
```bash
curl https://packages.microsoft.com/keys/microsoft.asc | sudo tee /etc/apt/trusted.gpg.d/microsoft.asc
sudo add-apt-repository "$(curl https://packages.microsoft.com/config/ubuntu/$(lsb_release -rs)/prod.list)"
sudo apt-get update
sudo apt-get install -y unixodbc-dev msodbcsql18
```

### Python

Requires Python 3.12 or later.

### Source Database (Azure SQL)

CDC must be enabled on the source database **before** running this tool. The tool does not enable CDC itself — it only reads from existing capture instances.

```sql
-- Enable CDC at the database level
EXEC sys.sp_cdc_enable_db;

-- Enable CDC for each table you want to replicate
EXEC sys.sp_cdc_enable_table
    @source_schema = N'dbo',
    @source_name = N'customers',
    @role_name = NULL,
    @capture_instance = N'dbo_customers';
```

The capture instance name must match what you configure in `config.yaml`. By default the tool expects `{schema}_{table}` (e.g., `dbo_customers`).

### Target Database (RDS SQL Server)

- The target tables **must already exist** with matching schemas. The tool does not create user tables.
- The target user needs permission to create a schema (default `cdc_sync`) for control tables, or the schema must be pre-created.
- Primary keys (or unique keys) must match between source and target tables.

## Installation

```bash
git clone <repo-url>
cd RDS_cdc
python3 -m venv .venv
source .venv/bin/activate
pip install -e .
```

## Configuration

Create a YAML configuration file (see `config/config.example.yaml` for reference):

```yaml
connections:
  source_connection_string: "Driver={ODBC Driver 18 for SQL Server};Server=your-azure-sql.database.windows.net;Database=mydb;Uid=reader;Pwd=secret;Encrypt=yes;TrustServerCertificate=no;"
  target_connection_string: "Driver={ODBC Driver 18 for SQL Server};Server=your-rds.region.rds.amazonaws.com;Database=mydb;Uid=writer;Pwd=secret;Encrypt=yes;TrustServerCertificate=yes;"

state_store:
  schema_name: cdc_sync  # control schema on target (created automatically)

tables:
  - source_schema: dbo
    source_table: customers
    capture_instance: dbo_customers
    batch_size: 50000
    polling_interval_seconds: 2.0

  - source_schema: dbo
    source_table: orders
    capture_instance: dbo_orders

cutover_timeout_seconds: 3600
shutdown_grace_period_seconds: 30
```

### Connection Strings

Connection strings can be provided in the YAML config or via environment variables:

| Config field | Environment variable fallback |
|---|---|
| `connections.source_connection_string` | `AZURE_SQL_CONNECTION_STRING` |
| `connections.target_connection_string` | `RDS_SQL_CONNECTION_STRING` |

The tool checks the config file first, then falls back to environment variables.

### Per-Table Options

| Option | Default | Description |
|--------|---------|-------------|
| `source_schema` | `dbo` | Schema of the source table |
| `source_table` | (required) | Source table name |
| `target_schema` | `dbo` | Schema of the target table |
| `target_table` | same as source | Target table name (if different) |
| `capture_instance` | `{schema}_{table}` | CDC capture instance name |
| `batch_size` | `10000` | Rows per batch during snapshot |
| `polling_interval_seconds` | `5.0` | Seconds between CDC poll cycles |
| `columns.allowlist` | (none) | Only replicate these columns |
| `columns.denylist` | (none) | Exclude these columns from replication |
| `checksum_validation` | `false` | Enable checksum validation after snapshot |
| `continue_on_validation_failure` | `false` | Continue to CDC even if snapshot validation fails |

You cannot specify both `allowlist` and `denylist` for the same table.

## Usage

### Running

```bash
# Set the config path
export CONFIG_FILE_PATH=config/my_config.yaml

# Run the tool
python -m cdc_sync
```

Or inline:
```bash
CONFIG_FILE_PATH=config/my_config.yaml python -m cdc_sync
```

### What Happens on Startup

1. Loads and validates the YAML config
2. Connects to source (Azure SQL) and target (RDS)
3. Creates the control schema (`cdc_sync`) on the target if it doesn't exist
4. Checks that all tables have primary keys on both source and target
5. Verifies CDC capture instances exist on the source
6. Checks for LSN retention gaps (if resuming a previous run)
7. Begins the snapshot phase (or resumes from where it left off)

### Phases

**Snapshot:**
- Captures the current source LSN before reading any rows
- Reads source rows in batches (PK-paginated) and bulk-inserts into a staging table on target
- MERGEs from staging into the target table (idempotent — safe to re-run)
- Checkpoints progress after each batch (resumable on restart)
- Once all tables complete snapshot, transitions to CDC

**CDC:**
- Polls each table's capture instance independently at its configured interval
- Fetches changes using `sys.fn_cdc_get_all_changes_<capture_instance>`
- Applies inserts/updates via MERGE, deletes via DELETE by PK
- Advances watermarks atomically with data applies (same transaction)
- Failed records retry 5 times with exponential backoff (1s, 2s, 4s, 8s, 16s)
- After 5 failures, records are quarantined to the dead-letter queue (DLQ)

**Cutover:**
- Triggered via `SIGUSR1` signal: `kill -USR1 <pid>`
- Captures a fixed `cutover_target_lsn` from the source
- Drains all tables until their watermarks reach that LSN
- Validates row counts between source and target
- Marks the run as COMPLETED and exits with code 0

### Triggering Cutover

```bash
# Find the process ID
pgrep -f "python -m cdc_sync"

# Send the cutover signal
kill -USR1 <pid>
```

The process will drain remaining changes, validate, and exit.

### Graceful Shutdown

Send `SIGTERM` or `SIGINT` (Ctrl+C):
- Finishes any in-flight batch transactions
- Persists current watermarks
- Exits with code 0

The next run will resume from the persisted watermarks.

## Resumability

The tool is designed to be stopped and restarted at any point:

- **During snapshot:** Resumes from the last checkpointed primary key position
- **During CDC:** Resumes from the last committed watermark per table
- **Idempotent applies:** MERGE-by-PK means duplicate delivery never creates duplicate rows

State is stored in the `cdc_sync` schema on the target RDS instance (configurable). Tables:
- `run_state` — current operational mode
- `watermarks` — per-table LSN watermarks
- `snapshot_progress` — snapshot checkpoint per table
- `dlq` — dead-letter queue for failed records

## Logging

Structured JSON logs via `structlog`. Control the log level with:

```bash
export LOG_LEVEL=DEBUG  # DEBUG, INFO, WARNING, ERROR (default: INFO)
```

Every log entry includes: timestamp, level, logger name, run ID, and (where applicable) table name and LSN.

Connection strings and passwords are never logged.

## Dead-Letter Queue (DLQ)

Records that fail to apply after 5 retries are written to `cdc_sync.dlq` on the target. The DLQ contains:
- Table schema and name
- Source LSN
- Operation type (insert/update/delete)
- Full row data (JSON)
- Error message
- Timestamp

DLQ records do **not** block replication — the watermark advances past them. Operators must review and reprocess DLQ records manually.

```sql
-- View DLQ records
SELECT * FROM cdc_sync.dlq WHERE resolved_at IS NULL ORDER BY quarantined_at;
```

## Important Limitations

- **DDL replication is not supported.** If the source schema changes, the affected table is paused (`PAUSED_DDL` state). You must manually apply DDL to the target and restart.
- **Target schema must pre-exist.** The tool does not create user tables.
- **At-least-once delivery.** The same row may be applied more than once on restart, but MERGE-by-PK makes this safe.
- **No cross-table transactional consistency.** Tables are replicated independently.
- **CDC retention.** Azure SQL CDC has a default retention of 3 days. If the tool is stopped longer than the retention period, it cannot resume — the affected table will be flagged with a retention gap error.
- **No source throttling.** Under backpressure, the tool buffers changes and logs a warning but does not slow down reads from the source.

## Environment Variables

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `CONFIG_FILE_PATH` | Yes | — | Path to YAML config file |
| `AZURE_SQL_CONNECTION_STRING` | If not in config | — | Source connection string |
| `RDS_SQL_CONNECTION_STRING` | If not in config | — | Target connection string |
| `LOG_LEVEL` | No | `INFO` | Minimum log level |

## Exit Codes

| Code | Meaning |
|------|---------|
| 0 | Successful cutover or graceful shutdown |
| 1 | Fatal error (config invalid, connection failed, PK check failed, cutover validation failed, etc.) |
