class MergeBuilder:
    """Generates parameterized T-SQL MERGE statements for idempotent upsert/delete."""

    def build_upsert(
        self,
        target_schema: str,
        target_table: str,
        columns: list[str],
        pk_columns: list[str],
    ) -> str:
        source_cols = ", ".join(f"? AS [{c}]" for c in columns)
        join_clause = " AND ".join(
            f"target.[{c}] = source.[{c}]" for c in pk_columns
        )
        update_cols = ", ".join(
            f"target.[{c}] = source.[{c}]" for c in columns if c not in pk_columns
        )
        insert_cols = ", ".join(f"[{c}]" for c in columns)
        insert_vals = ", ".join(f"source.[{c}]" for c in columns)

        sql = f"""MERGE [{target_schema}].[{target_table}] AS target
USING (SELECT {source_cols}) AS source ({', '.join(f'[{c}]' for c in columns)})
ON {join_clause}
WHEN MATCHED THEN UPDATE SET {update_cols}
WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals});"""
        return sql

    def build_delete(
        self,
        target_schema: str,
        target_table: str,
        pk_columns: list[str],
    ) -> str:
        where_clause = " AND ".join(f"[{c}] = ?" for c in pk_columns)
        return f"DELETE FROM [{target_schema}].[{target_table}] WHERE {where_clause}"

    def build_bulk_merge_from_staging(
        self,
        target_schema: str,
        target_table: str,
        staging_schema: str,
        staging_table: str,
        columns: list[str],
        pk_columns: list[str],
    ) -> str:
        """MERGE from a staging table into the target — used for snapshot bulk loads."""
        join_clause = " AND ".join(
            f"target.[{c}] = source.[{c}]" for c in pk_columns
        )
        update_cols = ", ".join(
            f"target.[{c}] = source.[{c}]" for c in columns if c not in pk_columns
        )
        insert_cols = ", ".join(f"[{c}]" for c in columns)
        insert_vals = ", ".join(f"source.[{c}]" for c in columns)

        sql = f"""MERGE [{target_schema}].[{target_table}] AS target
USING [{staging_schema}].[{staging_table}] AS source
ON {join_clause}
WHEN MATCHED THEN UPDATE SET {update_cols}
WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals});"""
        return sql
