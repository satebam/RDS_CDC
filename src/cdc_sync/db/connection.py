import asyncio
from contextlib import asynccontextmanager
from collections.abc import AsyncGenerator

import pyodbc
import structlog

logger = structlog.get_logger()


class DatabaseConnection:
    """Manages pyodbc connections with asyncio.to_thread for non-blocking DB access."""

    def __init__(self, connection_string: str, name: str = "db"):
        self._connection_string = connection_string
        self._name = name
        self._conn: pyodbc.Connection | None = None

    async def connect(self) -> None:
        self._conn = await asyncio.to_thread(
            pyodbc.connect, self._connection_string, autocommit=True
        )
        await logger.ainfo("database_connected", name=self._name)

    async def close(self) -> None:
        if self._conn:
            await asyncio.to_thread(self._conn.close)
            self._conn = None
            await logger.ainfo("database_disconnected", name=self._name)

    @property
    def connection(self) -> pyodbc.Connection:
        if not self._conn:
            raise RuntimeError(f"Database connection '{self._name}' not established")
        return self._conn

    async def execute(self, sql: str, params: tuple | None = None) -> pyodbc.Cursor:
        def _exec():
            cursor = self.connection.cursor()
            if params:
                cursor.execute(sql, params)
            else:
                cursor.execute(sql)
            return cursor

        return await asyncio.to_thread(_exec)

    async def execute_many(
        self, sql: str, param_sets: list[tuple], *, fast: bool = True
    ) -> None:
        def _exec():
            cursor = self.connection.cursor()
            cursor.fast_executemany = fast
            cursor.executemany(sql, param_sets)
            cursor.close()

        await asyncio.to_thread(_exec)

    async def fetchall(self, sql: str, params: tuple | None = None) -> list[pyodbc.Row]:
        def _exec():
            cursor = self.connection.cursor()
            if params:
                cursor.execute(sql, params)
            else:
                cursor.execute(sql)
            rows = cursor.fetchall()
            cursor.close()
            return rows

        return await asyncio.to_thread(_exec)

    async def fetchone(self, sql: str, params: tuple | None = None) -> pyodbc.Row | None:
        def _exec():
            cursor = self.connection.cursor()
            if params:
                cursor.execute(sql, params)
            else:
                cursor.execute(sql)
            row = cursor.fetchone()
            cursor.close()
            return row

        return await asyncio.to_thread(_exec)

    @asynccontextmanager
    async def transaction(self) -> AsyncGenerator[pyodbc.Connection, None]:
        """Context manager for explicit transactions on the target."""

        def _begin():
            self.connection.autocommit = False

        def _commit():
            self.connection.commit()
            self.connection.autocommit = True

        def _rollback():
            self.connection.rollback()
            self.connection.autocommit = True

        await asyncio.to_thread(_begin)
        try:
            yield self.connection
            await asyncio.to_thread(_commit)
        except Exception:
            await asyncio.to_thread(_rollback)
            raise

    async def is_healthy(self) -> bool:
        try:
            await self.fetchone("SELECT 1")
            return True
        except Exception:
            return False
