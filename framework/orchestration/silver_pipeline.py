"""Orchestration/wiring for metadata-driven Silver entity pipelines."""

from __future__ import annotations

from typing import Any, Iterable

from framework.metadata.models import EntityConfig
from framework.normalization import SourceNormalizer
from framework.quality.validator import SilverQualityValidator
from framework.scd2 import SilverSCD2Engine


class SilverEntityPipeline:
    """
    Coordinates normalization + SCD2 for one configured entity.

    This class is intentionally thin so execution modes can later be bound to
    Databricks Jobs/Workflows, DLT, streaming micro-batches, and replay controls.
    """

    def __init__(
        self,
        entity_config: EntityConfig,
        normalizer: SourceNormalizer | None = None,
        scd2_engine: SilverSCD2Engine | None = None,
        validator: SilverQualityValidator | None = None,
    ) -> None:
        self.entity_config = entity_config
        self.normalizer = normalizer or SourceNormalizer(entity_config)
        self.scd2_engine = scd2_engine or SilverSCD2Engine(entity_config)
        self.validator = validator or SilverQualityValidator(entity_config)

    def run_once(
        self,
        source_payloads: dict[str, Iterable[dict[str, Any]]],
        existing_history: list[dict[str, Any]] | None = None,
        run_id: str = "manual_run",
    ) -> list[dict[str, Any]]:
        normalized = self.normalizer.normalize_all_sources(source_payloads)
        history = self.scd2_engine.build_history(
            change_records=normalized,
            existing_history=existing_history,
            run_id=run_id,
        )
        self.validator.validate_target(history)
        return history

    # Replay/backfill hooks (interfaces for future implementation)
    def run_streaming(self) -> None:
        self.scd2_engine.run_streaming()

    def run_backfill(self, start_time: Any, end_time: Any) -> None:
        self.scd2_engine.run_backfill(start_time=start_time, end_time=end_time)

    def run_full_rebuild(self) -> None:
        self.scd2_engine.run_full_rebuild()

    def run_key_replay(self, keys: list[dict[str, Any]]) -> None:
        self.scd2_engine.run_key_replay(keys=keys)

