"""Quality/validation interfaces for Silver targets."""

from __future__ import annotations

from typing import Any

from framework.metadata.models import EntityConfig


class SilverQualityValidator:
    """Default validator with baseline contract checks; extend for project-specific DQ."""

    def __init__(self, entity_config: EntityConfig) -> None:
        self.entity_config = entity_config

    def validate_target(self, history_rows: list[dict[str, Any]]) -> None:
        """
        Validate target output.

        This method is intentionally lightweight and acts as a hook interface for
        richer constraints, SLA checks, and scorecard publishing later.
        """
        required_columns = {
            "effective_from",
            "effective_to",
            "is_current",
            "is_deleted",
            "change_hash",
            "event_time",
            "processed_time",
            "record_source",
            "source_event_id",
            "source_position",
            "source_priority",
            "run_id",
        }
        for row in history_rows:
            missing = required_columns.difference(row.keys())
            if missing:
                raise ValueError(
                    f"Row is missing required Silver SCD2 columns {sorted(missing)} for entity {self.entity_config.name}"
                )
            for key in self.entity_config.business_key:
                if row.get(key) is None:
                    raise ValueError(f"Business key '{key}' cannot be null in entity {self.entity_config.name}")

    # Placeholder contract for richer quality gates
    def validate_batch_metrics(self, _: dict[str, Any]) -> None:
        raise NotImplementedError("Batch metrics validation is not implemented in the skeleton yet.")

