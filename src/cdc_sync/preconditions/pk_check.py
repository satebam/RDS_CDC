import structlog

from cdc_sync.config.models import TableConfig
from cdc_sync.db.connection import DatabaseConnection

logger = structlog.get_logger()

GET_PK_COLUMNS_SQL = """
SELECT kcu.COLUMN_NAME
FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu
    ON tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME
    AND tc.TABLE_SCHEMA = kcu.TABLE_SCHEMA
    AND tc.TABLE_NAME = kcu.TABLE_NAME
WHERE tc.TABLE_SCHEMA = ?
    AND tc.TABLE_NAME = ?
    AND tc.CONSTRAINT_TYPE IN ('PRIMARY KEY', 'UNIQUE')
ORDER BY kcu.ORDINAL_POSITION
"""


async def get_pk_columns(
    conn: DatabaseConnection, schema_name: str, table_name: str
) -> list[str]:
    rows = await conn.fetchall(GET_PK_COLUMNS_SQL, (schema_name, table_name))
    return [row.COLUMN_NAME for row in rows]


async def check_primary_keys(
    source_conn: DatabaseConnection,
    target_conn: DatabaseConnection,
    tables: list[TableConfig],
) -> dict[str, list[str]]:
    """
    Verify all tables have PK/unique keys on both source and target.
    Returns a dict mapping table full name -> pk columns.
    Raises SystemExit if any table fails the check.
    """
    errors: list[str] = []
    pk_map: dict[str, list[str]] = {}

    for table in tables:
        source_pks = await get_pk_columns(
            source_conn, table.source_schema, table.source_table
        )
        if not source_pks:
            errors.append(
                f"Source table {table.full_source_name} has no primary key or unique key"
            )
            continue

        target_pks = await get_pk_columns(
            target_conn, table.target_schema, table.resolved_target_table
        )
        if not target_pks:
            errors.append(
                f"Target table {table.full_target_name} has no primary key or unique key"
            )
            continue

        if source_pks != target_pks:
            errors.append(
                f"Key mismatch for {table.full_source_name}: "
                f"source={source_pks}, target={target_pks}"
            )
            continue

        pk_map[f"{table.source_schema}.{table.source_table}"] = source_pks

    if errors:
        for err in errors:
            await logger.aerror("pk_check_failed", error=err)
        raise SystemExit(1)

    await logger.ainfo("pk_check_passed", table_count=len(tables))
    return pk_map
