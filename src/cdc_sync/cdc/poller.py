import asyncio
import json

import pyodbc
import structlog

from cdc_sync.cdc.retry import ChangeRecord, RetryPolicy
from cdc_sync.config.models import TableConfig
from cdc_sync.db.connection import DatabaseConnection
from cdc_sync.db.merge import MergeBuilder
from cdc_sync.state.models import DLQRecord, TableState
from cdc_sync.state.store import StateStore

logger = structlog.get_logger()

GET_MAX_LSN_SQL = "SELECT sys.fn_cdc_get_max_lsn()"

GET_ALL_CHANGES_SQL = """
SELECT *
FROM cdc.fn_cdc_get_all_changes_{capture_instance}(?, ?, N'all update old')
ORDER BY __$start_lsn, __$seqval
"""

CHECK_DDL_SQL = """
SELECT TOP 1 ddl_command
FROM cdc.ddl_history
WHERE source_object_id = OBJECT_ID(?)
    AND ddl_command IS NOT NULL
ORDER BY ddl_phase DESC
"""

# CDC operation codes
OP_DELETE = 1
OP_INSERT = 2
OP_UPDATE_BEFORE = 3
OP_UPDATE_AFTER = 4


class TablePoller:
    """Async coroutine that polls a single table's CDC capture instance."""

    def __init__(
        self,
        table: TableConfig,
        pk_columns: list[str],
        all_columns: list[str],
        source_conn: DatabaseConnection,
        target_conn: DatabaseConnection,
        state_store: StateStore,
        shutdown_event: asyncio.Event,
        cutover_lsn: asyncio.Future | None = None,
    ):
        self._table = table
        self._pk_columns = pk_columns
        self._all_columns = all_columns
        self._source = source_conn
        self._target = target_conn
        self._state = state_store
        self._shutdown = shutdown_event
        self._cutover_lsn = cutover_lsn
        self._merge_builder = MergeBuilder()
        self._retry_policy = RetryPolicy()
        self._current_watermark: bytes | None = None
        self._buffer_size = 0

    async def poll_loop(self) -> None:
        watermark = await self._state.get_watermark(
            self._table.source_schema, self._table.source_table
        )
        if watermark:
            self._current_watermark = watermark.lsn
        else:
            await logger.aerror(
                "no_watermark_found", table=self._table.full_source_name
            )
            return

        await logger.ainfo(
            "cdc_poller_started",
            table=self._table.full_source_name,
            watermark=self._current_watermark.hex(),
        )

        while not self._shutdown.is_set():
            try:
                await self._poll_once()
            except Exception as e:
                await logger.aerror(
                    "poll_cycle_error",
                    table=self._table.full_source_name,
                    error=str(e),
                )

            if self._cutover_lsn and self._cutover_lsn.done():
                target_lsn = self._cutover_lsn.result()
                if self._current_watermark and self._current_watermark >= target_lsn:
                    await logger.ainfo(
                        "cdc_poller_reached_cutover",
                        table=self._table.full_source_name,
                    )
                    return

            try:
                await asyncio.wait_for(
                    self._shutdown.wait(),
                    timeout=self._table.polling_interval_seconds,
                )
                return
            except asyncio.TimeoutError:
                pass

    async def _poll_once(self) -> None:
        if await self._check_ddl():
            return

        upper_lsn = await self._get_upper_lsn()
        if not upper_lsn or upper_lsn <= self._current_watermark:
            return

        changes = await self._fetch_changes(self._current_watermark, upper_lsn)
        if not changes:
            return

        self._buffer_size = len(changes)
        await self._apply_changes(changes)
        self._buffer_size = 0

    async def _get_upper_lsn(self) -> bytes | None:
        if self._cutover_lsn and self._cutover_lsn.done():
            return self._cutover_lsn.result()

        row = await self._source.fetchone(GET_MAX_LSN_SQL)
        if not row or not row[0]:
            return None
        return row[0]

    async def _fetch_changes(self, from_lsn: bytes, to_lsn: bytes) -> list[ChangeRecord]:
        sql = GET_ALL_CHANGES_SQL.format(
            capture_instance=self._table.resolved_capture_instance
        )
        rows = await self._source.fetchall(sql, (from_lsn, to_lsn))

        changes = []
        for row in rows:
            lsn = row[0]  # __$start_lsn
            operation = row[2]  # __$operation

            if operation == OP_UPDATE_BEFORE:
                continue

            data_values = row[4:]  # skip __$start_lsn, __$seqval, __$operation, __$update_mask
            data = dict(zip(self._all_columns, data_values))
            pk_values = {pk: data[pk] for pk in self._pk_columns}

            changes.append(ChangeRecord(
                lsn=lsn,
                operation=operation,
                pk_values=pk_values,
                data=data,
                table_schema=self._table.source_schema,
                table_name=self._table.source_table,
            ))

        return changes

    async def _apply_changes(self, changes: list[ChangeRecord]) -> None:
        max_lsn = changes[-1].lsn

        for record in changes:
            success = await self._retry_policy.execute_with_retry(
                self._apply_single, record
            )
            if not success:
                await self._quarantine_record(record)

        async with self._target.transaction() as conn:
            def _advance(conn=conn):
                cursor = conn.cursor()
                self._state.advance_watermark_in_txn(
                    cursor,
                    self._table.source_schema,
                    self._table.source_table,
                    max_lsn,
                    TableState.CDC,
                )
                cursor.close()

            await asyncio.to_thread(_advance)

        self._current_watermark = max_lsn

        await logger.ainfo(
            "cdc_batch_applied",
            table=self._table.full_source_name,
            changes=len(changes),
            watermark=max_lsn.hex(),
        )

    async def _apply_single(self, record: ChangeRecord) -> None:
        if record.operation in (OP_INSERT, OP_UPDATE_AFTER):
            sql = self._merge_builder.build_upsert(
                target_schema=self._table.target_schema,
                target_table=self._table.resolved_target_table,
                columns=self._all_columns,
                pk_columns=self._pk_columns,
            )
            params = tuple(record.data[c] for c in self._all_columns)
            await self._target.execute(sql, params)

        elif record.operation == OP_DELETE:
            sql = self._merge_builder.build_delete(
                target_schema=self._table.target_schema,
                target_table=self._table.resolved_target_table,
                pk_columns=self._pk_columns,
            )
            params = tuple(record.pk_values[c] for c in self._pk_columns)
            await self._target.execute(sql, params)

    async def _quarantine_record(self, record: ChangeRecord) -> None:
        dlq_record = self._retry_policy.build_dlq_record(
            record, "Max retries exhausted"
        )
        async with self._target.transaction() as conn:
            def _write_dlq(conn=conn):
                cursor = conn.cursor()
                self._state.write_dlq_in_txn(cursor, dlq_record)
                self._state.advance_watermark_in_txn(
                    cursor,
                    self._table.source_schema,
                    self._table.source_table,
                    record.lsn,
                    TableState.CDC,
                )
                cursor.close()

            await asyncio.to_thread(_write_dlq)

        self._current_watermark = record.lsn
        await logger.awarning(
            "record_quarantined_to_dlq",
            table=self._table.full_source_name,
            lsn=record.lsn.hex(),
        )

    async def _check_ddl(self) -> bool:
        source_object = f"{self._table.source_schema}.{self._table.source_table}"
        row = await self._source.fetchone(CHECK_DDL_SQL, (source_object,))
        if row:
            await self._state.upsert_watermark(
                self._table.source_schema,
                self._table.source_table,
                self._current_watermark,
                TableState.PAUSED_DDL,
            )
            await logger.aerror(
                "ddl_detected_pausing",
                table=self._table.full_source_name,
                ddl_command=row[0],
            )
            return True
        return False
