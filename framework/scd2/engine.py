"""Generic Silver SCD2 engine consuming canonical normalized change records."""

from __future__ import annotations

import hashlib
import json
from collections import defaultdict
from datetime import datetime
from typing import Any, Iterable

from framework.metadata.models import DeleteSemantic, EntityConfig
from framework.normalization.source_normalizer import CanonicalChangeRecord


class SilverSCD2Engine:
    """
    Entity-agnostic SCD2 engine skeleton.

    It operates on canonical change records so source-specific logic stays in normalization.
    """

    def __init__(self, entity_config: EntityConfig) -> None:
        self.entity_config = entity_config

    def build_history(
        self,
        change_records: Iterable[CanonicalChangeRecord],
        existing_history: list[dict[str, Any]] | None = None,
        run_id: str | None = None,
        processed_time: str | None = None,
    ) -> list[dict[str, Any]]:
        """Build/maintain canonical Silver SCD2 rows in-memory."""

        run_id = run_id or "manual_run"
        processed_time = processed_time or datetime.utcnow().isoformat()
        history = list(existing_history or [])
        by_key = defaultdict(list)
        for row in history:
            by_key[self._key_tuple(row)].append(row)

        for change in sorted(change_records, key=self._sort_key):
            key_tuple = tuple(change.business_key[k] for k in self.entity_config.business_key)
            key_history = by_key[key_tuple]
            current_row = self._get_current(key_history)
            next_state = self._apply_change(current_row, change)
            if next_state is None:
                continue
            next_hash = self._compute_hash(next_state)
            if current_row and current_row["change_hash"] == next_hash:
                continue
            if current_row:
                current_row["is_current"] = False
                current_row["effective_to"] = change.event_time
            new_row = self._build_scd2_row(
                key=change.business_key,
                state=next_state,
                change=change,
                change_hash=next_hash,
                run_id=run_id,
                processed_time=processed_time,
            )
            key_history.append(new_row)
            history.append(new_row)

        return history

    # Future orchestration hooks
    def run_streaming(self) -> None:
        raise NotImplementedError("Streaming runner is not implemented in the skeleton yet.")

    def run_backfill(
        self,
        change_records: Iterable[CanonicalChangeRecord],
        existing_history: list[dict[str, Any]],
        start_time: Any,
        end_time: Any,
        run_id: str,
    ) -> list[dict[str, Any]]:
        """
        Apply only the change records inside a backfill window on top of existing history.
        """
        windowed = [
            change
            for change in change_records
            if self._event_time_in_window(change.event_time, start_time=start_time, end_time=end_time)
        ]
        return self.build_history(change_records=windowed, existing_history=existing_history, run_id=run_id)

    def run_full_rebuild(
        self, change_records: Iterable[CanonicalChangeRecord], run_id: str
    ) -> list[dict[str, Any]]:
        """Rebuild complete history from scratch using full change stream."""
        return self.build_history(change_records=change_records, existing_history=[], run_id=run_id)

    def run_key_replay(
        self,
        change_records: Iterable[CanonicalChangeRecord],
        existing_history: list[dict[str, Any]],
        keys: list[dict[str, Any]],
        run_id: str,
    ) -> list[dict[str, Any]]:
        """
        Replay selected keys by rebuilding their full timelines from supplied change records.
        """
        replay_key_set = {self._key_dict_to_tuple(key) for key in keys}
        base_history = [row for row in existing_history if self._key_tuple(row) not in replay_key_set]
        replay_changes = [change for change in change_records if self._change_key_tuple(change) in replay_key_set]
        return self.build_history(change_records=replay_changes, existing_history=base_history, run_id=run_id)

    def _apply_change(
        self, current_row: dict[str, Any] | None, change: CanonicalChangeRecord
    ) -> dict[str, Any] | None:
        current_state = {
            col: current_row.get(col) for col in self.entity_config.hash_columns
        } if current_row else {col: None for col in self.entity_config.hash_columns}

        if change.operation == "delete":
            semantic = DeleteSemantic(change.delete_semantic)
            if semantic == DeleteSemantic.IGNORE_DELETE:
                return None
            if semantic == DeleteSemantic.ENTITY_DELETE:
                return current_state
            if semantic == DeleteSemantic.NULLIFY_COMPONENT:
                next_state = dict(current_state)
                for col in change.component_columns:
                    next_state[col] = None
                return next_state

        next_state = dict(current_state)
        for col, val in change.payload.items():
            next_state[col] = val
        return next_state

    def _build_scd2_row(
        self,
        key: dict[str, Any],
        state: dict[str, Any],
        change: CanonicalChangeRecord,
        change_hash: str,
        run_id: str,
        processed_time: str,
    ) -> dict[str, Any]:
        is_deleted = change.operation == "delete" and change.delete_semantic == DeleteSemantic.ENTITY_DELETE.value
        row: dict[str, Any] = {
            **key,
            **state,
            "effective_from": change.event_time,
            "effective_to": None,
            "is_current": True,
            "is_deleted": is_deleted,
            "change_hash": change_hash,
            "event_time": change.event_time,
            "processed_time": processed_time,
            "record_source": change.record_source,
            "source_event_id": change.source_event_id,
            "source_position": change.source_position,
            "source_priority": change.source_priority,
            "run_id": run_id,
        }
        return row

    def _compute_hash(self, state: dict[str, Any]) -> str:
        data = {col: state.get(col) for col in self.entity_config.hash_columns}
        serialized = json.dumps(data, sort_keys=True, default=str)
        return hashlib.sha256(serialized.encode("utf-8")).hexdigest()

    def _key_tuple(self, row: dict[str, Any]) -> tuple[Any, ...]:
        return tuple(row[k] for k in self.entity_config.business_key)

    @staticmethod
    def _get_current(history_for_key: list[dict[str, Any]]) -> dict[str, Any] | None:
        for row in reversed(history_for_key):
            if row.get("is_current"):
                return row
        return None

    def _sort_key(self, change: CanonicalChangeRecord) -> tuple[Any, ...]:
        values: list[Any] = []
        for field in self.entity_config.ordering_fields:
            values.append(getattr(change, field, None))
        values.append(change.source_name)
        return tuple(values)

    def _change_key_tuple(self, change: CanonicalChangeRecord) -> tuple[Any, ...]:
        return tuple(change.business_key[k] for k in self.entity_config.business_key)

    def _key_dict_to_tuple(self, key: dict[str, Any]) -> tuple[Any, ...]:
        return tuple(key[k] for k in self.entity_config.business_key)

    @staticmethod
    def _normalize_event_time(value: Any) -> datetime:
        if isinstance(value, datetime):
            return value
        text = str(value)
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        return datetime.fromisoformat(text)

    def _event_time_in_window(self, value: Any, start_time: Any, end_time: Any) -> bool:
        event_time = self._normalize_event_time(value)
        start = self._normalize_event_time(start_time)
        end = self._normalize_event_time(end_time)
        return start <= event_time <= end
