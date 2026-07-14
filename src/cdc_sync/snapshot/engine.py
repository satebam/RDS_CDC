import asyncio
import json

import structlog

from cdc_sync.config.models import TableConfig
from cdc_sync.db.connection import DatabaseConnection
from cdc_sync.db.merge import MergeBuilder
from cdc_sync.state.models import SnapshotProgress, TableState
from cdc_sync.state.store import StateStore
from cdc_sync.validation.validator import validate_row_counts

logger = structlog.get_logger()

GET_MAX_LSN_SQL = "SELECT sys.fn_cdc_get_max_lsn()"

GET_TABLE_COLUMNS_SQL = """
SELECT COLUMN_NAME
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
ORDER BY ORDINAL_POSITION
"""


class SnapshotEngine:
    def __init__(
        self,
        source_conn: DatabaseConnection,
        target_conn: DatabaseConnection,
        state_store: StateStore,
        pk_map: dict[str, list[str]],
    ):
        self._source = source_conn
        self._target = target_conn
        self._state = state_store
        self._pk_map = pk_map
        self._merge_builder = MergeBuilder()

    async def run(self, tables: list[TableConfig]) -> None:
        for table in tables:
            progress = await self._state.get_snapshot_progress(
                table.source_schema, table.source_table
            )

            if progress and progress.state == TableState.SNAPSHOT_COMPLETE:
                await logger.ainfo(
                    "snapshot_skipped_already_complete", table=table.full_source_name
                )
                continue

            await self._snapshot_table(table, progress)

        await logger.ainfo("snapshot_phase_complete")

    async def _snapshot_table(
        self, table: TableConfig, existing_progress: SnapshotProgress | None
    ) -> None:
        key = f"{table.source_schema}.{table.source_table}"
        pk_columns = self._pk_map[key]

        if existing_progress:
            snapshot_start_lsn = existing_progress.snapshot_start_lsn
            last_pk_values = (
                json.loads(existing_progress.last_pk_values)
                if existing_progress.last_pk_values
                else None
            )
            rows_copied = existing_progress.rows_copied
            await logger.ainfo(
                "snapshot_resuming",
                table=table.full_source_name,
                rows_already_copied=rows_copied,
            )
        else:
            snapshot_start_lsn = await self._get_snapshot_start_lsn()
            last_pk_values = None
            rows_copied = 0

            progress = SnapshotProgress(
                table_name=table.source_table,
                schema_name=table.source_schema,
                snapshot_start_lsn=snapshot_start_lsn,
                rows_copied=0,
                state=TableState.SNAPSHOT_IN_PROGRESS,
            )
            await self._state.upsert_snapshot_progress(progress)
            await logger.ainfo(
                "snapshot_started",
                table=table.full_source_name,
                snapshot_start_lsn=snapshot_start_lsn.hex(),
            )

        columns = await self._get_columns(table)

        while True:
            batch = await self._read_batch(table, pk_columns, columns, last_pk_values)
            if not batch:
                break

            await self._write_batch(table, columns, pk_columns, batch)
            rows_copied += len(batch)

            last_row = batch[-1]
            last_pk_values = [last_row[pk] for pk in pk_columns]

            progress = SnapshotProgress(
                table_name=table.source_table,
                schema_name=table.source_schema,
                snapshot_start_lsn=snapshot_start_lsn,
                last_pk_values=json.dumps(last_pk_values, default=str),
                rows_copied=rows_copied,
                state=TableState.SNAPSHOT_IN_PROGRESS,
            )
            await self._state.upsert_snapshot_progress(progress)

            await logger.ainfo(
                "snapshot_batch_complete",
                table=table.full_source_name,
                batch_size=len(batch),
                total_rows=rows_copied,
            )

        progress = SnapshotProgress(
            table_name=table.source_table,
            schema_name=table.source_schema,
            snapshot_start_lsn=snapshot_start_lsn,
            last_pk_values=json.dumps(last_pk_values, default=str) if last_pk_values else None,
            rows_copied=rows_copied,
            state=TableState.SNAPSHOT_COMPLETE,
        )
        await self._state.upsert_snapshot_progress(progress)

        await self._state.upsert_watermark(
            table.source_schema,
            table.source_table,
            snapshot_start_lsn,
            TableState.SNAPSHOT_COMPLETE,
        )

        await logger.ainfo(
            "snapshot_table_complete",
            table=table.full_source_name,
            total_rows=rows_copied,
        )

    async def _get_snapshot_start_lsn(self) -> bytes:
        row = await self._source.fetchone(GET_MAX_LSN_SQL)
        if not row or not row[0]:
            raise RuntimeError("Could not obtain snapshot start LSN from source")
        return row[0]

    async def _get_columns(self, table: TableConfig) -> list[str]:
        rows = await self._source.fetchall(
            GET_TABLE_COLUMNS_SQL, (table.source_schema, table.source_table)
        )
        all_columns = [row.COLUMN_NAME for row in rows]

        if table.columns.allowlist:
            return [c for c in all_columns if c in table.columns.allowlist]
        elif table.columns.denylist:
            return [c for c in all_columns if c not in table.columns.denylist]
        return all_columns

    async def _read_batch(
        self,
        table: TableConfig,
        pk_columns: list[str],
        columns: list[str],
        last_pk_values: list | None,
    ) -> list[dict]:
        col_list = ", ".join(f"[{c}]" for c in columns)
        order_by = ", ".join(f"[{c}]" for c in pk_columns)

        if last_pk_values:
            where_parts = []
            for i, pk in enumerate(pk_columns):
                eq_parts = [f"[{pk_columns[j]}] = ?" for j in range(i)]
                eq_parts.append(f"[{pk}] > ?")
                where_parts.append("(" + " AND ".join(eq_parts) + ")")
            where_clause = "WHERE " + " OR ".join(where_parts)

            params = []
            for i in range(len(pk_columns)):
                for j in range(i):
                    params.append(last_pk_values[j])
                params.append(last_pk_values[i])

            sql = (
                f"SELECT TOP({table.batch_size}) {col_list} "
                f"FROM [{table.source_schema}].[{table.source_table}] "
                f"{where_clause} ORDER BY {order_by}"
            )
            rows = await self._source.fetchall(sql, tuple(params))
        else:
            sql = (
                f"SELECT TOP({table.batch_size}) {col_list} "
                f"FROM [{table.source_schema}].[{table.source_table}] "
                f"ORDER BY {order_by}"
            )
            rows = await self._source.fetchall(sql)

        return [dict(zip(columns, row)) for row in rows]

    async def _write_batch(
        self,
        table: TableConfig,
        columns: list[str],
        pk_columns: list[str],
        batch: list[dict],
    ) -> None:
        staging_table = f"_staging_{table.source_table}"
        schema = self._state._schema

        await self._ensure_staging_table(table, columns, schema, staging_table)

        await self._target.execute(f"TRUNCATE TABLE [{schema}].[{staging_table}]")

        await self._target.execute(
            f"SET IDENTITY_INSERT [{schema}].[{staging_table}] ON"
        )

        col_list = ", ".join(f"[{c}]" for c in columns)
        placeholders = ", ".join("?" for _ in columns)
        insert_sql = f"INSERT INTO [{schema}].[{staging_table}] ({col_list}) VALUES ({placeholders})"

        param_sets = [tuple(row[c] for c in columns) for row in batch]
        await self._target.execute_many(insert_sql, param_sets, fast=True)

        await self._target.execute(
            f"SET IDENTITY_INSERT [{schema}].[{staging_table}] OFF"
        )

        merge_sql = self._merge_builder.build_bulk_merge_from_staging(
            target_schema=table.target_schema,
            target_table=table.resolved_target_table,
            staging_schema=schema,
            staging_table=staging_table,
            columns=columns,
            pk_columns=pk_columns,
        )
        await self._target.execute(merge_sql)

    async def _ensure_staging_table(
        self, table: TableConfig, columns: list[str], schema: str, staging_table: str
    ) -> None:
        check_sql = (
            f"IF NOT EXISTS (SELECT 1 FROM sys.tables t "
            f"JOIN sys.schemas s ON t.schema_id = s.schema_id "
            f"WHERE s.name = '{schema}' AND t.name = '{staging_table}') "
            f"SELECT [{table.source_schema}].[{table.source_table}].* "
            f"INTO [{schema}].[{staging_table}] "
            f"FROM [{table.source_schema}].[{table.source_table}] WHERE 1=0"
        )
        await self._target.execute(check_sql)
