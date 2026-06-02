"""Run registry and checkpointing contracts for Silver orchestration."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any


class RunMode(str, Enum):
    """Supported Silver processing execution modes."""

    INCREMENTAL = "incremental"
    BACKFILL = "backfill"
    KEY_REPLAY = "key_replay"
    FULL_REBUILD = "full_rebuild"


class RunStatus(str, Enum):
    """Lifecycle status of a run."""

    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"


@dataclass
class RunRecord:
    """Simple run ledger record."""

    run_id: str
    entity_name: str
    mode: RunMode
    status: RunStatus
    started_at: str
    ended_at: str | None = None
    checkpoint: dict[str, Any] = field(default_factory=dict)
    details: dict[str, Any] = field(default_factory=dict)
    error: str | None = None


class InMemoryRunRegistry:
    """
    In-memory run registry.

    Keeps run history and latest checkpoint per entity so replay-aware orchestration
    can be tested without infrastructure dependencies.
    """

    def __init__(self) -> None:
        self._runs_by_entity: dict[str, list[RunRecord]] = {}
        self._latest_checkpoint: dict[str, dict[str, Any]] = {}

    def start_run(
        self,
        entity_name: str,
        mode: RunMode,
        run_id: str,
        details: dict[str, Any] | None = None,
    ) -> RunRecord:
        record = RunRecord(
            run_id=run_id,
            entity_name=entity_name,
            mode=mode,
            status=RunStatus.RUNNING,
            started_at=datetime.utcnow().isoformat(),
            details=details or {},
        )
        self._runs_by_entity.setdefault(entity_name, []).append(record)
        return record

    def update_checkpoint(self, entity_name: str, checkpoint: dict[str, Any]) -> None:
        self._latest_checkpoint[entity_name] = dict(checkpoint)

    def complete_run(
        self,
        record: RunRecord,
        checkpoint: dict[str, Any] | None = None,
        details: dict[str, Any] | None = None,
    ) -> None:
        record.status = RunStatus.SUCCEEDED
        record.ended_at = datetime.utcnow().isoformat()
        if checkpoint is not None:
            record.checkpoint = dict(checkpoint)
            self.update_checkpoint(record.entity_name, checkpoint)
        if details:
            record.details.update(details)

    def fail_run(self, record: RunRecord, error: Exception) -> None:
        record.status = RunStatus.FAILED
        record.ended_at = datetime.utcnow().isoformat()
        record.error = str(error)

    def get_latest_checkpoint(self, entity_name: str) -> dict[str, Any] | None:
        checkpoint = self._latest_checkpoint.get(entity_name)
        return dict(checkpoint) if checkpoint else None

    def get_runs(self, entity_name: str) -> list[RunRecord]:
        return list(self._runs_by_entity.get(entity_name, []))

