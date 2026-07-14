import structlog

from cdc_sync.config.models import TableConfig
from cdc_sync.db.connection import DatabaseConnection
from cdc_sync.state.models import TableState, TableWatermark

logger = structlog.get_logger()

CHECK_CAPTURE_INSTANCE_SQL = """
EXEC sys.sp_cdc_help_change_data_capture @source_schema = ?, @source_name = ?
"""

GET_MIN_LSN_SQL = "SELECT sys.fn_cdc_get_min_lsn(?)"


async def check_capture_instances(
    source_conn: DatabaseConnection,
    tables: list[TableConfig],
) -> None:
    """Verify CDC capture instances exist for all configured tables. Exits on failure."""
    errors: list[str] = []

    for table in tables:
        rows = await source_conn.fetchall(
            CHECK_CAPTURE_INSTANCE_SQL,
            (table.source_schema, table.source_table),
        )
        if not rows:
            errors.append(
                f"No CDC capture instance found for {table.full_source_name}. "
                f"Expected capture instance: {table.resolved_capture_instance}"
            )
            continue

        instance_names = [row[0] for row in rows]
        if table.resolved_capture_instance not in instance_names:
            errors.append(
                f"Capture instance '{table.resolved_capture_instance}' not found for "
                f"{table.full_source_name}. Available: {instance_names}"
            )

    if errors:
        for err in errors:
            await logger.aerror("cdc_check_failed", error=err)
        raise SystemExit(1)

    await logger.ainfo("cdc_check_passed", table_count=len(tables))


async def check_retention_gaps(
    source_conn: DatabaseConnection,
    tables: list[TableConfig],
    watermarks: dict[str, TableWatermark],
) -> list[TableConfig]:
    """
    Check for LSN retention gaps. Returns list of tables that are OK to proceed.
    Tables with gaps are logged as errors and excluded.
    """
    valid_tables: list[TableConfig] = []

    for table in tables:
        key = f"{table.source_schema}.{table.source_table}"
        watermark = watermarks.get(key)

        if not watermark:
            valid_tables.append(table)
            continue

        row = await source_conn.fetchone(
            GET_MIN_LSN_SQL, (table.resolved_capture_instance,)
        )
        if not row or not row[0]:
            valid_tables.append(table)
            continue

        min_lsn = row[0]
        if watermark.lsn < min_lsn:
            await logger.aerror(
                "retention_gap_detected",
                table=table.full_source_name,
                watermark_lsn=watermark.lsn.hex(),
                min_available_lsn=min_lsn.hex(),
            )
        else:
            valid_tables.append(table)

    return valid_tables
