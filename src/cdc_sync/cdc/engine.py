import asyncio

import structlog

from cdc_sync.cdc.poller import TablePoller
from cdc_sync.config.models import AppConfig, TableConfig
from cdc_sync.db.connection import DatabaseConnection
from cdc_sync.state.models import TableState
from cdc_sync.state.store import StateStore

logger = structlog.get_logger()

GET_TABLE_COLUMNS_SQL = """
SELECT COLUMN_NAME
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
ORDER BY ORDINAL_POSITION
"""


class CDCEngine:
    """Multi-table async CDC coordinator. Launches per-table pollers as async tasks."""

    def __init__(
        self,
        source_conn: DatabaseConnection,
        target_conn: DatabaseConnection,
        state_store: StateStore,
        pk_map: dict[str, list[str]],
        config: AppConfig,
    ):
        self._source = source_conn
        self._target = target_conn
        self._state = state_store
        self._pk_map = pk_map
        self._config = config
        self._shutdown_event = asyncio.Event()
        self._cutover_lsn_future: asyncio.Future | None = None
        self._pollers: list[TablePoller] = []
        self._tasks: list[asyncio.Task] = []

    async def run(self, tables: list[TableConfig]) -> None:
        active_tables = await self._filter_active_tables(tables)
        if not active_tables:
            await logger.ainfo("no_tables_ready_for_cdc")
            return

        self._cutover_lsn_future = asyncio.get_event_loop().create_future()

        for table in active_tables:
            poller = await self._create_poller(table)
            self._pollers.append(poller)
            task = asyncio.create_task(
                poller.poll_loop(),
                name=f"cdc_poller_{table.source_schema}.{table.source_table}",
            )
            self._tasks.append(task)

        await logger.ainfo("cdc_engine_started", poller_count=len(self._tasks))
        await asyncio.gather(*self._tasks, return_exceptions=True)
        await logger.ainfo("cdc_engine_stopped")

    async def signal_cutover(self, cutover_target_lsn: bytes) -> None:
        if self._cutover_lsn_future and not self._cutover_lsn_future.done():
            self._cutover_lsn_future.set_result(cutover_target_lsn)
            await logger.ainfo(
                "cutover_signaled", cutover_target_lsn=cutover_target_lsn.hex()
            )

    async def shutdown(self) -> None:
        self._shutdown_event.set()
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)
        await logger.ainfo("cdc_engine_shutdown_complete")

    async def _filter_active_tables(self, tables: list[TableConfig]) -> list[TableConfig]:
        active = []
        for table in tables:
            watermark = await self._state.get_watermark(
                table.source_schema, table.source_table
            )
            if not watermark:
                continue
            if watermark.state in (
                TableState.SNAPSHOT_COMPLETE,
                TableState.CDC,
            ):
                active.append(table)
            elif watermark.state == TableState.PAUSED_DDL:
                await logger.awarning(
                    "table_paused_ddl_skipping", table=table.full_source_name
                )
            elif watermark.state == TableState.PAUSED_RETENTION_GAP:
                await logger.awarning(
                    "table_paused_retention_gap_skipping", table=table.full_source_name
                )
        return active

    async def _create_poller(self, table: TableConfig) -> TablePoller:
        key = f"{table.source_schema}.{table.source_table}"
        pk_columns = self._pk_map[key]

        columns = await self._get_columns(table)

        return TablePoller(
            table=table,
            pk_columns=pk_columns,
            all_columns=columns,
            source_conn=self._source,
            target_conn=self._target,
            state_store=self._state,
            shutdown_event=self._shutdown_event,
            cutover_lsn=self._cutover_lsn_future,
        )

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
