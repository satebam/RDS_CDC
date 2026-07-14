import asyncio
import json
from dataclasses import dataclass

import structlog

from cdc_sync.state.models import DLQRecord

logger = structlog.get_logger()

MAX_RETRIES = 5
BASE_DELAY_SECONDS = 1.0


@dataclass
class ChangeRecord:
    lsn: bytes
    operation: int
    pk_values: dict
    data: dict
    table_schema: str
    table_name: str


class RetryPolicy:
    """Handles exponential backoff retry and DLQ quarantine for failed applies."""

    def __init__(self, max_retries: int = MAX_RETRIES, base_delay: float = BASE_DELAY_SECONDS):
        self._max_retries = max_retries
        self._base_delay = base_delay

    async def execute_with_retry(
        self,
        func,
        record: ChangeRecord,
    ) -> bool:
        """
        Execute func with retry. Returns True if successful, False if exhausted (DLQ needed).
        On exhaustion, returns the DLQRecord to be written.
        """
        last_error: Exception | None = None

        for attempt in range(1, self._max_retries + 1):
            try:
                await func(record)
                return True
            except Exception as e:
                last_error = e
                if attempt < self._max_retries:
                    delay = self._base_delay * (2 ** (attempt - 1))
                    await logger.awarning(
                        "apply_retry",
                        table=f"[{record.table_schema}].[{record.table_name}]",
                        lsn=record.lsn.hex(),
                        attempt=attempt,
                        next_delay=delay,
                        error=str(e),
                    )
                    await asyncio.sleep(delay)

        await logger.aerror(
            "apply_exhausted",
            table=f"[{record.table_schema}].[{record.table_name}]",
            lsn=record.lsn.hex(),
            error=str(last_error),
        )
        return False

    def build_dlq_record(self, record: ChangeRecord, error: str) -> DLQRecord:
        return DLQRecord(
            table_schema=record.table_schema,
            table_name=record.table_name,
            source_lsn=record.lsn,
            operation=record.operation,
            change_data=json.dumps(record.data, default=str),
            error_message=error[:4000],
            retry_count=self._max_retries,
        )
