import structlog

from cdc_sync.config.models import TableConfig
from cdc_sync.db.connection import DatabaseConnection

logger = structlog.get_logger()


async def get_row_count(
    conn: DatabaseConnection, schema_name: str, table_name: str
) -> int:
    row = await conn.fetchone(
        f"SELECT COUNT(*) FROM [{schema_name}].[{table_name}]"
    )
    return row[0] if row else 0


async def validate_row_counts(
    source_conn: DatabaseConnection,
    target_conn: DatabaseConnection,
    tables: list[TableConfig],
) -> list[str]:
    """
    Compare row counts between source and target for all tables.
    Returns list of error messages for mismatches (empty = all pass).
    """
    errors: list[str] = []

    for table in tables:
        source_count = await get_row_count(
            source_conn, table.source_schema, table.source_table
        )
        target_count = await get_row_count(
            target_conn, table.target_schema, table.resolved_target_table
        )

        if source_count != target_count:
            msg = (
                f"Row count mismatch for {table.full_source_name}: "
                f"source={source_count}, target={target_count}"
            )
            errors.append(msg)
            await logger.awarning("validation_row_count_mismatch", table=table.full_source_name,
                                  source_count=source_count, target_count=target_count)
        else:
            await logger.ainfo("validation_row_count_match", table=table.full_source_name,
                               count=source_count)

    return errors
