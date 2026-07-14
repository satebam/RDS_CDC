import os
import sys

import yaml
from dotenv import load_dotenv
from pydantic import ValidationError

from cdc_sync.config.models import AppConfig


def load_config() -> AppConfig:
    load_dotenv()

    config_path = os.environ.get("CONFIG_FILE_PATH")
    if not config_path:
        print("ERROR: CONFIG_FILE_PATH environment variable is not set", file=sys.stderr)
        sys.exit(1)

    try:
        with open(config_path) as f:
            raw = yaml.safe_load(f)
    except FileNotFoundError:
        print(f"ERROR: Configuration file not found: {config_path}", file=sys.stderr)
        sys.exit(1)
    except yaml.YAMLError as e:
        print(f"ERROR: Invalid YAML in configuration file: {e}", file=sys.stderr)
        sys.exit(1)

    if raw is None:
        raw = {}

    try:
        config = AppConfig(**raw)
    except ValidationError as e:
        print(f"ERROR: Configuration validation failed:\n{e}", file=sys.stderr)
        sys.exit(1)

    config = _resolve_connection_strings(config)
    return config


def _resolve_connection_strings(config: AppConfig) -> AppConfig:
    source = config.connections.source_connection_string
    target = config.connections.target_connection_string

    if not source:
        source = os.environ.get("AZURE_SQL_CONNECTION_STRING")
    if not target:
        target = os.environ.get("RDS_SQL_CONNECTION_STRING")

    if not source:
        print(
            "ERROR: Source connection string not provided in config or "
            "AZURE_SQL_CONNECTION_STRING env var",
            file=sys.stderr,
        )
        sys.exit(1)

    if not target:
        print(
            "ERROR: Target connection string not provided in config or "
            "RDS_SQL_CONNECTION_STRING env var",
            file=sys.stderr,
        )
        sys.exit(1)

    config.connections.source_connection_string = source
    config.connections.target_connection_string = target
    return config
