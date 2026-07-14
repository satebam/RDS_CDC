def create_schema_sql(schema_name: str) -> str:
    return (
        f"IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = '{schema_name}') "
        f"EXEC('CREATE SCHEMA [{schema_name}]')"
    )


def create_run_state_table_sql(schema_name: str) -> str:
    return f"""
IF NOT EXISTS (SELECT 1 FROM sys.tables t
    JOIN sys.schemas s ON t.schema_id = s.schema_id
    WHERE s.name = '{schema_name}' AND t.name = 'run_state')
CREATE TABLE [{schema_name}].[run_state] (
    run_id          NVARCHAR(128) NOT NULL PRIMARY KEY,
    mode            NVARCHAR(32) NOT NULL,
    started_at      DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
    cutover_target_lsn BINARY(10) NULL
)
"""


def create_watermarks_table_sql(schema_name: str) -> str:
    return f"""
IF NOT EXISTS (SELECT 1 FROM sys.tables t
    JOIN sys.schemas s ON t.schema_id = s.schema_id
    WHERE s.name = '{schema_name}' AND t.name = 'watermarks')
CREATE TABLE [{schema_name}].[watermarks] (
    table_schema    NVARCHAR(128) NOT NULL,
    table_name      NVARCHAR(128) NOT NULL,
    lsn             BINARY(10) NOT NULL,
    state           NVARCHAR(32) NOT NULL,
    updated_at      DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
    PRIMARY KEY (table_schema, table_name)
)
"""


def create_snapshot_progress_table_sql(schema_name: str) -> str:
    return f"""
IF NOT EXISTS (SELECT 1 FROM sys.tables t
    JOIN sys.schemas s ON t.schema_id = s.schema_id
    WHERE s.name = '{schema_name}' AND t.name = 'snapshot_progress')
CREATE TABLE [{schema_name}].[snapshot_progress] (
    table_schema        NVARCHAR(128) NOT NULL,
    table_name          NVARCHAR(128) NOT NULL,
    snapshot_start_lsn  BINARY(10) NOT NULL,
    last_pk_values      NVARCHAR(MAX) NULL,
    rows_copied         BIGINT NOT NULL DEFAULT 0,
    state               NVARCHAR(32) NOT NULL,
    updated_at          DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
    PRIMARY KEY (table_schema, table_name)
)
"""


def create_dlq_table_sql(schema_name: str) -> str:
    return f"""
IF NOT EXISTS (SELECT 1 FROM sys.tables t
    JOIN sys.schemas s ON t.schema_id = s.schema_id
    WHERE s.name = '{schema_name}' AND t.name = 'dlq')
CREATE TABLE [{schema_name}].[dlq] (
    id              BIGINT IDENTITY(1,1) PRIMARY KEY,
    table_schema    NVARCHAR(128) NOT NULL,
    table_name      NVARCHAR(128) NOT NULL,
    source_lsn      BINARY(10) NOT NULL,
    operation       TINYINT NOT NULL,
    change_data     NVARCHAR(MAX) NOT NULL,
    error_message   NVARCHAR(4000) NOT NULL,
    retry_count     INT NOT NULL,
    quarantined_at  DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
    resolved_at     DATETIME2 NULL
)
"""


UPSERT_RUN_STATE = """
MERGE [{schema}].[run_state] AS target
USING (SELECT ? AS run_id, ? AS mode, ? AS started_at, ? AS cutover_target_lsn) AS source
ON target.run_id = source.run_id
WHEN MATCHED THEN UPDATE SET
    mode = source.mode,
    cutover_target_lsn = source.cutover_target_lsn
WHEN NOT MATCHED THEN INSERT (run_id, mode, started_at, cutover_target_lsn)
    VALUES (source.run_id, source.mode, source.started_at, source.cutover_target_lsn);
"""

GET_RUN_STATE = "SELECT run_id, mode, started_at, cutover_target_lsn FROM [{schema}].[run_state]"

UPSERT_WATERMARK = """
MERGE [{schema}].[watermarks] AS target
USING (SELECT ? AS table_schema, ? AS table_name, ? AS lsn, ? AS state) AS source
ON target.table_schema = source.table_schema AND target.table_name = source.table_name
WHEN MATCHED THEN UPDATE SET
    lsn = source.lsn,
    state = source.state,
    updated_at = SYSUTCDATETIME()
WHEN NOT MATCHED THEN INSERT (table_schema, table_name, lsn, state)
    VALUES (source.table_schema, source.table_name, source.lsn, source.state);
"""

GET_WATERMARK = """
SELECT table_schema, table_name, lsn, state, updated_at
FROM [{schema}].[watermarks]
WHERE table_schema = ? AND table_name = ?
"""

GET_ALL_WATERMARKS = """
SELECT table_schema, table_name, lsn, state, updated_at
FROM [{schema}].[watermarks]
"""

UPSERT_SNAPSHOT_PROGRESS = """
MERGE [{schema}].[snapshot_progress] AS target
USING (SELECT ? AS table_schema, ? AS table_name, ? AS snapshot_start_lsn,
       ? AS last_pk_values, ? AS rows_copied, ? AS state) AS source
ON target.table_schema = source.table_schema AND target.table_name = source.table_name
WHEN MATCHED THEN UPDATE SET
    snapshot_start_lsn = source.snapshot_start_lsn,
    last_pk_values = source.last_pk_values,
    rows_copied = source.rows_copied,
    state = source.state,
    updated_at = SYSUTCDATETIME()
WHEN NOT MATCHED THEN INSERT (table_schema, table_name, snapshot_start_lsn, last_pk_values, rows_copied, state)
    VALUES (source.table_schema, source.table_name, source.snapshot_start_lsn,
            source.last_pk_values, source.rows_copied, source.state);
"""

GET_SNAPSHOT_PROGRESS = """
SELECT table_schema, table_name, snapshot_start_lsn, last_pk_values, rows_copied, state
FROM [{schema}].[snapshot_progress]
WHERE table_schema = ? AND table_name = ?
"""

INSERT_DLQ = """
INSERT INTO [{schema}].[dlq]
    (table_schema, table_name, source_lsn, operation, change_data, error_message, retry_count)
VALUES (?, ?, ?, ?, ?, ?, ?)
"""

ADVANCE_WATERMARK_SQL = """
UPDATE [{schema}].[watermarks]
SET lsn = ?, state = ?, updated_at = SYSUTCDATETIME()
WHERE table_schema = ? AND table_name = ?
"""
