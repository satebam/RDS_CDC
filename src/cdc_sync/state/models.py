from dataclasses import dataclass, field
from datetime import datetime
from enum import StrEnum


class OperationalMode(StrEnum):
    SNAPSHOT = "SNAPSHOT"
    CDC = "CDC"
    CUTOVER = "CUTOVER"


class TableState(StrEnum):
    PENDING = "PENDING"
    SNAPSHOT_IN_PROGRESS = "SNAPSHOT_IN_PROGRESS"
    SNAPSHOT_COMPLETE = "SNAPSHOT_COMPLETE"
    CDC = "CDC"
    PAUSED_DDL = "PAUSED_DDL"
    PAUSED_RETENTION_GAP = "PAUSED_RETENTION_GAP"
    CUTOVER_COMPLETE = "CUTOVER_COMPLETE"


@dataclass
class RunState:
    run_id: str
    mode: OperationalMode
    started_at: datetime
    cutover_target_lsn: bytes | None = None


@dataclass
class TableWatermark:
    table_name: str
    schema_name: str
    lsn: bytes
    state: TableState
    updated_at: datetime


@dataclass
class SnapshotProgress:
    table_name: str
    schema_name: str
    snapshot_start_lsn: bytes
    last_pk_values: str | None = None
    rows_copied: int = 0
    state: TableState = TableState.SNAPSHOT_IN_PROGRESS


@dataclass
class DLQRecord:
    table_schema: str
    table_name: str
    source_lsn: bytes
    operation: int
    change_data: str
    error_message: str
    retry_count: int = 5
    quarantined_at: datetime = field(default_factory=datetime.utcnow)
