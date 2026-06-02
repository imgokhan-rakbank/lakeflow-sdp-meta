"""Orchestration/wiring for metadata-driven Silver entity pipelines."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Callable, Iterable

from framework.metadata.models import EntityConfig
from framework.normalization import SourceNormalizer
from framework.orchestration.run_registry import InMemoryRunRegistry, RunMode
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
        run_registry: InMemoryRunRegistry | None = None,
    ) -> None:
        self.entity_config = entity_config
        self.normalizer = normalizer or SourceNormalizer(entity_config)
        self.scd2_engine = scd2_engine or SilverSCD2Engine(entity_config)
        self.validator = validator or SilverQualityValidator(entity_config)
        self.run_registry = run_registry or InMemoryRunRegistry()

    def run_once(
        self,
        source_payloads: dict[str, Iterable[dict[str, Any]]],
        existing_history: list[dict[str, Any]] | None = None,
        run_id: str | None = None,
    ) -> list[dict[str, Any]]:
        normalized = self.normalizer.normalize_all_sources(source_payloads)
        run_id = run_id or self._make_run_id(RunMode.INCREMENTAL)
        return self._execute_run(
            mode=RunMode.INCREMENTAL,
            run_id=run_id,
            run_details={"source_count": len(source_payloads)},
            run_checkpoint={"mode": RunMode.INCREMENTAL.value},
            processor=lambda: self.scd2_engine.build_history(
                change_records=normalized,
                existing_history=existing_history,
                run_id=run_id,
            ),
        )

    def run_streaming(
        self,
        batch_source: Iterable[dict[str, Iterable[dict[str, Any]]]],
        existing_history: list[dict[str, Any]] | None = None,
        run_id: str | None = None,
    ) -> list[dict[str, Any]]:
        """
        Process source-payload batches as a streaming micro-batch loop.

        Each batch is normalized and applied on top of the accumulated SCD2 history.
        Every micro-batch is recorded in the run registry under ``RunMode.STREAMING``.
        """
        history: list[dict[str, Any]] = list(existing_history or [])
        run_id = run_id or self._make_run_id(RunMode.STREAMING)
        for batch_index, source_payloads in enumerate(batch_source):
            normalized = self.normalizer.normalize_all_sources(source_payloads)
            batch_run_id = f"{run_id}_batch_{batch_index}"
            snapshot = list(history)
            history = self._execute_run(
                mode=RunMode.STREAMING,
                run_id=batch_run_id,
                run_details={"batch_index": batch_index, "source_count": len(source_payloads)},
                run_checkpoint={"mode": RunMode.STREAMING.value, "batch_index": batch_index},
                processor=lambda norm=normalized, h=snapshot, rid=batch_run_id: (
                    self.scd2_engine.build_history(
                        change_records=norm,
                        existing_history=h,
                        run_id=rid,
                    )
                ),
            )
        return history

    def run_backfill(
        self,
        source_payloads: dict[str, Iterable[dict[str, Any]]],
        start_time: Any,
        end_time: Any,
        existing_history: list[dict[str, Any]] | None = None,
        run_id: str | None = None,
    ) -> list[dict[str, Any]]:
        normalized = self.normalizer.normalize_all_sources(source_payloads)
        run_id = run_id or self._make_run_id(RunMode.BACKFILL)
        return self._execute_run(
            mode=RunMode.BACKFILL,
            run_id=run_id,
            run_details={
                "window_start": str(start_time),
                "window_end": str(end_time),
                "candidate_changes": len(normalized),
            },
            run_checkpoint={
                "mode": RunMode.BACKFILL.value,
                "window_start": str(start_time),
                "window_end": str(end_time),
            },
            processor=lambda: self.scd2_engine.run_backfill(
                change_records=normalized,
                existing_history=list(existing_history or []),
                start_time=start_time,
                end_time=end_time,
                run_id=run_id,
            ),
        )

    def run_full_rebuild(
        self,
        source_payloads: dict[str, Iterable[dict[str, Any]]],
        run_id: str | None = None,
    ) -> list[dict[str, Any]]:
        normalized = self.normalizer.normalize_all_sources(source_payloads)
        run_id = run_id or self._make_run_id(RunMode.FULL_REBUILD)
        return self._execute_run(
            mode=RunMode.FULL_REBUILD,
            run_id=run_id,
            run_details={"candidate_changes": len(normalized)},
            run_checkpoint={"mode": RunMode.FULL_REBUILD.value},
            processor=lambda: self.scd2_engine.run_full_rebuild(
                change_records=normalized,
                run_id=run_id,
            ),
        )

    def run_key_replay(
        self,
        source_payloads: dict[str, Iterable[dict[str, Any]]],
        keys: list[dict[str, Any]],
        existing_history: list[dict[str, Any]] | None = None,
        run_id: str | None = None,
    ) -> list[dict[str, Any]]:
        normalized = self.normalizer.normalize_all_sources(source_payloads)
        run_id = run_id or self._make_run_id(RunMode.KEY_REPLAY)
        return self._execute_run(
            mode=RunMode.KEY_REPLAY,
            run_id=run_id,
            run_details={"replayed_keys": keys, "candidate_changes": len(normalized)},
            run_checkpoint={"mode": RunMode.KEY_REPLAY.value, "replayed_keys": keys},
            processor=lambda: self.scd2_engine.run_key_replay(
                change_records=normalized,
                existing_history=list(existing_history or []),
                keys=keys,
                run_id=run_id,
            ),
        )

    def get_latest_checkpoint(self) -> dict[str, Any] | None:
        return self.run_registry.get_latest_checkpoint(self.entity_config.name)

    def get_run_history(self) -> list[dict[str, Any]]:
        return [record.__dict__ for record in self.run_registry.get_runs(self.entity_config.name)]

    def _execute_run(
        self,
        mode: RunMode,
        run_id: str,
        run_details: dict[str, Any],
        run_checkpoint: dict[str, Any],
        processor: Callable[[], list[dict[str, Any]]],
    ) -> list[dict[str, Any]]:
        record = self.run_registry.start_run(
            entity_name=self.entity_config.name,
            mode=mode,
            run_id=run_id,
            details=run_details,
        )
        try:
            history = processor()
            self.validator.validate_target(history)
            run_checkpoint = {
                **run_checkpoint,
                "row_count": len(history),
                "last_event_time": self._max_event_time_from_history(history),
                "run_id": run_id,
            }
            self.run_registry.complete_run(record=record, checkpoint=run_checkpoint)
            return history
        except Exception as exc:
            self.run_registry.fail_run(record=record, error=exc)
            raise

    @staticmethod
    def _max_event_time_from_history(history: list[dict[str, Any]]) -> Any:
        if not history:
            return None
        return max(row.get("event_time") for row in history if row.get("event_time") is not None)

    def _make_run_id(self, mode: RunMode) -> str:
        timestamp = datetime.utcnow().strftime("%Y%m%d%H%M%S%f")
        return f"{self.entity_config.name}_{mode.value}_{timestamp}"
