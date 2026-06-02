"""Config loading utilities (dict/JSON/YAML) for entity metadata."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from .models import ColumnMappingConfig, DeleteSemantic, EntityConfig, SourceConfig


def _parse_column_mapping(raw: dict[str, Any]) -> ColumnMappingConfig:
    return ColumnMappingConfig(
        source_column=raw["source_column"],
        target_column=raw["target_column"],
        is_business_key=raw.get("is_business_key", False),
        default_value=raw.get("default_value"),
    )


def _parse_source(raw: dict[str, Any]) -> SourceConfig:
    return SourceConfig(
        name=raw["name"],
        bronze_table=raw["bronze_table"],
        source_priority=int(raw["source_priority"]),
        record_source=raw.get("record_source", raw["name"]),
        delete_semantic=DeleteSemantic(raw["delete_semantic"]),
        operation_column=raw.get("operation_column", "op"),
        event_time_column=raw.get("event_time_column", "event_time"),
        source_event_id_column=raw.get("source_event_id_column", "event_id"),
        source_position_column=raw.get("source_position_column", "source_position"),
        ingest_ts_column=raw.get("ingest_ts_column", "ingest_ts"),
        ordering_fields=raw.get("ordering_fields", ["event_time", "source_priority", "source_position", "ingest_ts"]),
        column_mappings=[_parse_column_mapping(m) for m in raw.get("column_mappings", [])],
        component_columns=raw.get("component_columns", []),
    )


def load_entity_config(raw: dict[str, Any]) -> EntityConfig:
    """Load typed `EntityConfig` from a Python dictionary."""
    return EntityConfig(
        name=raw["name"],
        silver_table=raw["silver_table"],
        business_key=raw["business_key"],
        hash_columns=raw["hash_columns"],
        ordering_fields=raw.get("ordering_fields", ["event_time", "source_priority", "source_position", "ingest_ts"]),
        sources=[_parse_source(source) for source in raw.get("sources", [])],
    )


def load_entity_config_from_path(path: str | Path) -> EntityConfig:
    """
    Load entity config from JSON/YAML.

    This is intentionally file-based for bootstrap simplicity and can be replaced later
    with Delta metadata tables without changing framework contracts.
    """

    path = Path(path)
    suffix = path.suffix.lower()
    content = path.read_text(encoding="utf-8")

    if suffix == ".json":
        raw = json.loads(content)
        return load_entity_config(raw)

    if suffix in {".yaml", ".yml"}:
        try:
            import yaml  # type: ignore
        except ImportError as exc:  # pragma: no cover
            raise ImportError("PyYAML is required to load YAML configuration files.") from exc
        raw = yaml.safe_load(content)
        return load_entity_config(raw)

    raise ValueError(f"Unsupported config format: {path}")

