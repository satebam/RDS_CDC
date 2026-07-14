import asyncio
from datetime import datetime

import pyodbc
import structlog

from cdc_sync.db.connection import DatabaseConnection
from cdc_sync.state.models import (
    DLQRecord,
    OperationalMode,
    RunState,
    SnapshotProgress,
    TableState,
    TableWatermark,
)
from cdc_sync.state import queries

logger = structlog.get_logger()


class StateStore:
    """Manages the control schema and replication state on the target RDS instance."""

    def __init__(self, target_conn: DatabaseConnection, schema_name: str = "cdc_sync"):
        self._conn = target_conn
        self._schema = schema_name

    async def initialize(self) -> None:
        await self._conn.execute(queries.create_schema_sql(self._schema))
        await self._conn.execute(queries.create_run_state_table_sql(self._schema))
        await self._conn.execute(queries.create_watermarks_table_sql(self._schema))
        await self._conn.execute(queries.create_snapshot_progress_table_sql(self._schema))
        await self._conn.execute(queries.create_dlq_table_sql(self._schema))
        await logger.ainfo("state_store_initialized", schema=self._schema)

    async def get_run_state(self) -> RunState | None:
        sql = queries.GET_RUN_STATE.format(schema=self._schema)
        row = await self._conn.fetchone(sql)
        if not row:
            return None
        return RunState(
            run_id=row.run_id,
            mode=OperationalMode(row.mode),
            started_at=row.started_at,
            cutover_target_lsn=row.cutover_target_lsn,
        )

    async def upsert_run_state(self, run_state: RunState) -> None:
        sql = queries.UPSERT_RUN_STATE.format(schema=self._schema)
        await self._conn.execute(
            sql,
            (
                run_state.run_id,
                run_state.mode.value,
                run_state.started_at,
                run_state.cutover_target_lsn,
            ),
        )

    async def get_watermark(self, schema_name: str, table_name: str) -> TableWatermark | None:
        sql = queries.GET_WATERMARK.format(schema=self._schema)
        row = await self._conn.fetchone(sql, (schema_name, table_name))
        if not row:
            return None
        return TableWatermark(
            table_name=row.table_name,
            schema_name=row.table_schema,
            lsn=row.lsn,
            state=TableState(row.state),
            updated_at=row.updated_at,
        )

    async def get_all_watermarks(self) -> list[TableWatermark]:
        sql = queries.GET_ALL_WATERMARKS.format(schema=self._schema)
        rows = await self._conn.fetchall(sql)
        return [
            TableWatermark(
                table_name=row.table_name,
                schema_name=row.table_schema,
                lsn=row.lsn,
                state=TableState(row.state),
                updated_at=row.updated_at,
            )
            for row in rows
        ]

    async def upsert_watermark(
        self, schema_name: str, table_name: str, lsn: bytes, state: TableState
    ) -> None:
        sql = queries.UPSERT_WATERMARK.format(schema=self._schema)
        await self._conn.execute(sql, (schema_name, table_name, lsn, state.value))

    def advance_watermark_in_txn(
        self,
        cursor: pyodbc.Cursor,
        schema_name: str,
        table_name: str,
        lsn: bytes,
        state: TableState,
    ) -> None:
        """Advance watermark within an existing transaction (synchronous, called inside to_thread)."""
        sql = queries.ADVANCE_WATERMARK_SQL.format(schema=self._schema)
        cursor.execute(sql, (lsn, state.value, schema_name, table_name))

    async def get_snapshot_progress(
        self, schema_name: str, table_name: str
    ) -> SnapshotProgress | None:
        sql = queries.GET_SNAPSHOT_PROGRESS.format(schema=self._schema)
        row = await self._conn.fetchone(sql, (schema_name, table_name))
        if not row:
            return None
        return SnapshotProgress(
            table_name=row.table_name,
            schema_name=row.table_schema,
            snapshot_start_lsn=row.snapshot_start_lsn,
            last_pk_values=row.last_pk_values,
            rows_copied=row.rows_copied,
            state=TableState(row.state),
        )

    async def upsert_snapshot_progress(self, progress: SnapshotProgress) -> None:
        sql = queries.UPSERT_SNAPSHOT_PROGRESS.format(schema=self._schema)
        await self._conn.execute(
            sql,
            (
                progress.schema_name,
                progress.table_name,
                progress.snapshot_start_lsn,
                progress.last_pk_values,
                progress.rows_copied,
                progress.state.value,
            ),
        )

    def write_dlq_in_txn(self, cursor: pyodbc.Cursor, record: DLQRecord) -> None:
        """Write a DLQ record within an existing transaction (synchronous)."""
        sql = queries.INSERT_DLQ.format(schema=self._schema)
        cursor.execute(
            sql,
            (
                record.table_schema,
                record.table_name,
                record.source_lsn,
                record.operation,
                record.change_data,
                record.error_message,
                record.retry_count,
            ),
        )
