import pytest

from cdc_sync.config.models import AppConfig, ConnectionConfig, TableConfig


@pytest.fixture
def sample_table_config() -> TableConfig:
    return TableConfig(
        source_schema="dbo",
        source_table="customers",
        capture_instance="dbo_customers",
        batch_size=1000,
        polling_interval_seconds=1.0,
    )


@pytest.fixture
def sample_app_config(sample_table_config: TableConfig) -> AppConfig:
    return AppConfig(
        connections=ConnectionConfig(
            source_connection_string="Driver={ODBC Driver 18 for SQL Server};Server=localhost;Database=test;Uid=sa;Pwd=test;",
            target_connection_string="Driver={ODBC Driver 18 for SQL Server};Server=localhost;Database=test;Uid=sa;Pwd=test;",
        ),
        tables=[sample_table_config],
    )
