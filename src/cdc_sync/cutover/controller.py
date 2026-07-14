import asyncio

import structlog

from cdc_sync.cdc.engine import CDCEngine
from cdc_sync.config.models import AppConfig, TableConfig
from cdc_sync.db.connection import DatabaseConnection
from cdc_sync.state.models import OperationalMode, TableState
from cdc_sync.state.store import StateStore
from cdc_sync.validation.validator import validate_row_counts

logger = structlog.get_logger()

GET_MAX_LSN_SQL = "SELECT sys.fn_cdc_get_max_lsn()"


class CutoverController:
    """Handles cutover: drain CDC to a fixed LSN, validate, and mark COMPLETED."""

    def __init__(
        self,
        source_conn: DatabaseConnection,
        target_conn: DatabaseConnection,
        state_store: StateStore,
        cdc_engine: CDCEngine,
        config: AppConfig,
        tables: list[TableConfig],
    ):
        self._source = source_conn
        self._target = target_conn
        self._state = state_store
        self._cdc_engine = cdc_engine
        self._config = config
        self._tables = tables

    async def initiate(self) -> int:
        """
        Initiate cutover sequence.
        Returns exit code: 0 = success, 1 = failure.
        """
        paused = await self._check_paused_tables()
        if paused:
            for t in paused:
                await logger.aerror("cutover_blocked_paused_table", table=t)
            return 1

        cutover_target_lsn = await self._capture_cutover_lsn()
        await logger.ainfo(
            "cutover_initiated", cutover_target_lsn=cutover_target_lsn.hex()
        )

        run_state = await self._state.get_run_state()
        if run_state:
            run_state.mode = OperationalMode.CUTOVER
            run_state.cutover_target_lsn = cutover_target_lsn
            await self._state.upsert_run_state(run_state)

        await self._cdc_engine.signal_cutover(cutover_target_lsn)

        drained = await self._wait_for_drain(cutover_target_lsn)
        if not drained:
            await logger.aerror("cutover_timeout")
            return 1

        errors = await validate_row_counts(
            self._source, self._target, self._tables
        )
        if errors:
            for err in errors:
                await logger.aerror("cutover_validation_failed", error=err)
            return 1

        if run_state:
            run_state.mode = OperationalMode.CUTOVER
            await self._state.upsert_run_state(run_state)

        for table in self._tables:
            await self._state.upsert_watermark(
                table.source_schema,
                table.source_table,
                cutover_target_lsn,
                TableState.CUTOVER_COMPLETE,
            )

        await logger.ainfo("cutover_complete")
        return 0

    async def _capture_cutover_lsn(self) -> bytes:
        row = await self._source.fetchone(GET_MAX_LSN_SQL)
        if not row or not row[0]:
            raise RuntimeError("Could not obtain cutover target LSN from source")
        return row[0]

    async def _wait_for_drain(self, target_lsn: bytes) -> bool:
        timeout = self._config.cutover_timeout_seconds
        elapsed = 0.0
        poll_interval = 2.0

        while elapsed < timeout:
            all_drained = True
            for table in self._tables:
                watermark = await self._state.get_watermark(
                    table.source_schema, table.source_table
                )
                if not watermark or watermark.lsn < target_lsn:
                    all_drained = False
                    break

            if all_drained:
                return True

            await asyncio.sleep(poll_interval)
            elapsed += poll_interval

        not_drained = []
        for table in self._tables:
            watermark = await self._state.get_watermark(
                table.source_schema, table.source_table
            )
            if not watermark or watermark.lsn < target_lsn:
                not_drained.append(table.full_source_name)

        for t in not_drained:
            await logger.aerror("cutover_table_not_drained", table=t)
        return False

    async def _check_paused_tables(self) -> list[str]:
        paused = []
        for table in self._tables:
            watermark = await self._state.get_watermark(
                table.source_schema, table.source_table
            )
            if watermark and watermark.state in (
                TableState.PAUSED_DDL,
                TableState.PAUSED_RETENTION_GAP,
            ):
                paused.append(table.full_source_name)
        return paused
