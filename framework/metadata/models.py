"""Typed metadata models for metadata-driven Silver pipelines."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any


class DeleteSemantic(str, Enum):
    """How source delete events should affect the canonical entity."""

    ENTITY_DELETE = "entity_delete"
    NULLIFY_COMPONENT = "nullify_component"
    IGNORE_DELETE = "ignore_delete"


@dataclass(frozen=True)
class ColumnMappingConfig:
    """
    Maps one source column to one canonical Silver attribute.

    `is_business_key=True` indicates this mapped target column participates in entity key.
    """

    source_column: str
    target_column: str
    is_business_key: bool = False
    default_value: Any | None = None


@dataclass(frozen=True)
class SourceConfig:
    """Source-level CDC configuration."""

    name: str
    bronze_table: str
    source_priority: int
    record_source: str
    delete_semantic: DeleteSemantic
    operation_column: str = "op"
    event_time_column: str = "event_time"
    source_event_id_column: str = "event_id"
    source_position_column: str = "source_position"
    ingest_ts_column: str = "ingest_ts"
    ordering_fields: list[str] = field(
        default_factory=lambda: ["event_time", "source_priority", "source_position", "ingest_ts"]
    )
    column_mappings: list[ColumnMappingConfig] = field(default_factory=list)
    component_columns: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class EntityConfig:
    """Entity-level Silver SCD2 configuration."""

    name: str
    silver_table: str
    business_key: list[str]
    hash_columns: list[str]
    ordering_fields: list[str] = field(
        default_factory=lambda: ["event_time", "source_priority", "source_position", "ingest_ts"]
    )
    sources: list[SourceConfig] = field(default_factory=list)

