from pydantic import BaseModel, model_validator


class ColumnFilter(BaseModel):
    allowlist: list[str] | None = None
    denylist: list[str] | None = None

    @model_validator(mode="after")
    def check_mutual_exclusion(self) -> "ColumnFilter":
        if self.allowlist and self.denylist:
            raise ValueError("Cannot specify both allowlist and denylist for columns")
        return self


class TableConfig(BaseModel):
    source_schema: str = "dbo"
    source_table: str
    target_schema: str = "dbo"
    target_table: str | None = None
    capture_instance: str | None = None
    batch_size: int = 10_000
    polling_interval_seconds: float = 5.0
    columns: ColumnFilter = ColumnFilter()
    checksum_validation: bool = False
    continue_on_validation_failure: bool = False

    @property
    def resolved_target_table(self) -> str:
        return self.target_table or self.source_table

    @property
    def resolved_capture_instance(self) -> str:
        return self.capture_instance or f"{self.source_schema}_{self.source_table}"

    @property
    def full_source_name(self) -> str:
        return f"[{self.source_schema}].[{self.source_table}]"

    @property
    def full_target_name(self) -> str:
        return f"[{self.target_schema}].[{self.resolved_target_table}]"


class ConnectionConfig(BaseModel):
    source_connection_string: str | None = None
    target_connection_string: str | None = None


class StateStoreConfig(BaseModel):
    schema_name: str = "cdc_sync"


class AppConfig(BaseModel):
    connections: ConnectionConfig = ConnectionConfig()
    state_store: StateStoreConfig = StateStoreConfig()
    tables: list[TableConfig]
    cutover_timeout_seconds: int = 3600
    shutdown_grace_period_seconds: int = 30
    backpressure_buffer_threshold: int = 100_000

    @model_validator(mode="after")
    def check_tables_not_empty(self) -> "AppConfig":
        if not self.tables:
            raise ValueError("At least one table must be configured")
        return self
