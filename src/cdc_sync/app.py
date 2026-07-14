import asyncio
import signal
import uuid
from datetime import datetime, timezone

import structlog

from cdc_sync.cdc.engine import CDCEngine
from cdc_sync.config.loader import load_config
from cdc_sync.config.models import AppConfig
from cdc_sync.cutover.controller import CutoverController
from cdc_sync.db.connection import DatabaseConnection
from cdc_sync.logging.setup import setup_logging
from cdc_sync.preconditions.cdc_check import check_capture_instances, check_retention_gaps
from cdc_sync.preconditions.pk_check import check_primary_keys
from cdc_sync.snapshot.engine import SnapshotEngine
from cdc_sync.state.models import OperationalMode, RunState, TableState
from cdc_sync.state.store import StateStore

logger = structlog.get_logger()


class Application:
    """Full lifecycle orchestrator for the CDC Sync System."""

    def __init__(self):
        self._config: AppConfig | None = None
        self._source_conn: DatabaseConnection | None = None
        self._target_conn: DatabaseConnection | None = None
        self._state_store: StateStore | None = None
        self._cdc_engine: CDCEngine | None = None
        self._shutdown_event = asyncio.Event()
        self._cutover_event = asyncio.Event()

    async def run(self) -> int:
        run_id = str(uuid.uuid4())[:8]

        self._config = load_config()
        setup_logging(run_id)

        await logger.ainfo("starting", run_id=run_id)

        try:
            self._source_conn = DatabaseConnection(
                self._config.connections.source_connection_string, name="source"
            )
            self._target_conn = DatabaseConnection(
                self._config.connections.target_connection_string, name="target"
            )

            await self._source_conn.connect()
            await self._target_conn.connect()

            self._state_store = StateStore(
                self._target_conn, self._config.state_store.schema_name
            )
            await self._state_store.initialize()

            run_state = await self._state_store.get_run_state()
            if not run_state:
                run_state = RunState(
                    run_id=run_id,
                    mode=OperationalMode.SNAPSHOT,
                    started_at=datetime.now(timezone.utc),
                )
                await self._state_store.upsert_run_state(run_state)
                await logger.ainfo("new_run_created", run_id=run_id)
            else:
                await logger.ainfo(
                    "resuming_existing_run",
                    run_id=run_state.run_id,
                    mode=run_state.mode,
                )
                run_id = run_state.run_id

            pk_map = await check_primary_keys(
                self._source_conn, self._target_conn, self._config.tables
            )

            await check_capture_instances(self._source_conn, self._config.tables)

            existing_watermarks = await self._state_store.get_all_watermarks()
            watermark_dict = {
                f"{w.schema_name}.{w.table_name}": w for w in existing_watermarks
            }
            active_tables = await check_retention_gaps(
                self._source_conn, self._config.tables, watermark_dict
            )

            self._setup_signal_handlers()

            if run_state.mode == OperationalMode.SNAPSHOT:
                snapshot_engine = SnapshotEngine(
                    self._source_conn, self._target_conn, self._state_store, pk_map
                )
                await snapshot_engine.run(active_tables)

                run_state.mode = OperationalMode.CDC
                await self._state_store.upsert_run_state(run_state)

            self._cdc_engine = CDCEngine(
                self._source_conn,
                self._target_conn,
                self._state_store,
                pk_map,
                self._config,
            )

            cdc_task = asyncio.create_task(self._cdc_engine.run(active_tables))
            cutover_task = asyncio.create_task(self._wait_for_cutover())
            shutdown_task = asyncio.create_task(self._shutdown_event.wait())

            done, pending = await asyncio.wait(
                [cdc_task, cutover_task, shutdown_task],
                return_when=asyncio.FIRST_COMPLETED,
            )

            if cutover_task in done:
                controller = CutoverController(
                    self._source_conn,
                    self._target_conn,
                    self._state_store,
                    self._cdc_engine,
                    self._config,
                    active_tables,
                )
                exit_code = await controller.initiate()
                await self._cdc_engine.shutdown()
                return exit_code

            if shutdown_task in done:
                await logger.ainfo("graceful_shutdown_initiated")
                await self._cdc_engine.shutdown()
                return 0

            for task in pending:
                task.cancel()

            return 0

        except SystemExit as e:
            return e.code if isinstance(e.code, int) else 1
        except Exception as e:
            await logger.aerror("fatal_error", error=str(e), exc_info=True)
            return 1
        finally:
            await self._cleanup()

    async def _wait_for_cutover(self) -> None:
        await self._cutover_event.wait()

    async def _cleanup(self) -> None:
        if self._source_conn:
            await self._source_conn.close()
        if self._target_conn:
            await self._target_conn.close()

    def _setup_signal_handlers(self) -> None:
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(sig, self._handle_shutdown)
        loop.add_signal_handler(signal.SIGUSR1, self._handle_cutover)

    def _handle_shutdown(self) -> None:
        self._shutdown_event.set()

    def _handle_cutover(self) -> None:
        self._cutover_event.set()
