"""Reusable source normalization from Bronze CDC to canonical change records."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime
from typing import Any, Iterable

from framework.metadata.models import DeleteSemantic, EntityConfig, SourceConfig


@dataclass(frozen=True)
class CanonicalChangeRecord:
    """Canonical handoff schema between normalization and SCD2 assembly."""

    entity_name: str
    source_name: str
    business_key: dict[str, Any]
    payload: dict[str, Any]
    operation: str
    delete_semantic: str
    event_time: Any
    source_event_id: Any
    source_position: Any
    source_priority: int
    ingest_ts: Any
    ordering_fields: list[str]
    record_source: str
    component_columns: list[str]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


class SourceNormalizer:
    """Entity-agnostic normalizer for CDC sources configured through metadata."""

    def __init__(self, entity_config: EntityConfig) -> None:
        self.entity_config = entity_config
        self._sources = {s.name: s for s in entity_config.sources}

    def normalize_source_records(
        self, source_name: str, records: Iterable[dict[str, Any]]
    ) -> list[CanonicalChangeRecord]:
        source = self._sources[source_name]
        normalized: list[CanonicalChangeRecord] = []
        for record in records:
            normalized.append(self._normalize_record(source, record))
        return normalized

    def normalize_all_sources(
        self, source_payloads: dict[str, Iterable[dict[str, Any]]]
    ) -> list[CanonicalChangeRecord]:
        out: list[CanonicalChangeRecord] = []
        for source_name, records in source_payloads.items():
            out.extend(self.normalize_source_records(source_name, records))
        return out

    def _normalize_record(self, source: SourceConfig, record: dict[str, Any]) -> CanonicalChangeRecord:
        operation = self._normalize_operation(record.get(source.operation_column))
        payload, key = self._map_payload_and_key(source, record)
        event_time = record.get(source.event_time_column) or datetime.utcnow().isoformat()
        return CanonicalChangeRecord(
            entity_name=self.entity_config.name,
            source_name=source.name,
            business_key=key,
            payload=payload,
            operation=operation,
            delete_semantic=source.delete_semantic.value,
            event_time=event_time,
            source_event_id=record.get(source.source_event_id_column),
            source_position=record.get(source.source_position_column),
            source_priority=source.source_priority,
            ingest_ts=record.get(source.ingest_ts_column),
            ordering_fields=self.entity_config.ordering_fields,
            record_source=source.record_source,
            component_columns=source.component_columns,
        )

    @staticmethod
    def _normalize_operation(raw_op: Any) -> str:
        op = str(raw_op or "u").strip().lower()
        if op in {"c", "r", "u", "upsert", "insert", "update"}:
            return "upsert"
        if op in {"d", "delete"}:
            return "delete"
        return "upsert"

    def _map_payload_and_key(
        self, source: SourceConfig, record: dict[str, Any]
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        payload: dict[str, Any] = {}
        key: dict[str, Any] = {}
        for mapping in source.column_mappings:
            value = record.get(mapping.source_column, mapping.default_value)
            payload[mapping.target_column] = value
            if mapping.is_business_key or mapping.target_column in self.entity_config.business_key:
                key[mapping.target_column] = value
        missing_keys = [k for k in self.entity_config.business_key if k not in key]
        if missing_keys:
            raise ValueError(
                f"Missing business key values {missing_keys} in source '{source.name}' record: {record}"
            )
        return payload, key

    def get_source_config(self, source_name: str) -> SourceConfig:
        return self._sources[source_name]

    @staticmethod
    def to_dicts(records: Iterable[CanonicalChangeRecord]) -> list[dict[str, Any]]:
        return [r.to_dict() for r in records]
