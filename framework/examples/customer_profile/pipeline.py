"""Sample entity pipeline: two Bronze sources -> one Silver SCD2 entity."""

from __future__ import annotations

from pathlib import Path

from framework.metadata import load_entity_config_from_path
from framework.orchestration import SilverEntityPipeline


def _sample_payloads() -> dict[str, list[dict]]:
    return {
        "customer_core": [
            {
                "op": "c",
                "event_id": "core-1",
                "source_position": 1,
                "event_time": "2026-01-01T10:00:00Z",
                "ingest_ts": "2026-01-01T10:00:05Z",
                "customer_id": "C001",
                "first_name": "Aisha",
                "last_name": "Rahman",
                "email": "aisha.rahman@example.com",
                "phone": "+971500000001",
            },
            {
                "op": "u",
                "event_id": "core-2",
                "source_position": 2,
                "event_time": "2026-01-01T10:15:00Z",
                "ingest_ts": "2026-01-01T10:15:03Z",
                "customer_id": "C001",
                "email": "aisha.r@example.com",
            },
        ],
        "customer_address": [
            {
                "op": "c",
                "event_id": "addr-1",
                "source_position": 10,
                "event_time": "2026-01-01T10:05:00Z",
                "ingest_ts": "2026-01-01T10:05:05Z",
                "customer_id": "C001",
                "address_line_1": "123 Palm Street",
                "address_line_2": "Apt 5",
                "city": "Dubai",
                "state": "DU",
                "postal_code": "00001",
            },
            {
                "op": "d",
                "event_id": "addr-2",
                "source_position": 11,
                "event_time": "2026-01-01T10:30:00Z",
                "ingest_ts": "2026-01-01T10:30:02Z",
                "customer_id": "C001",
                "address_line_1": "123 Palm Street",
                "address_line_2": "Apt 5",
                "city": "Dubai",
                "state": "DU",
                "postal_code": "00001",
            },
        ],
    }


def run_customer_profile_example() -> list[dict]:
    config_path = Path(__file__).with_name("config.yaml")
    entity_config = load_entity_config_from_path(config_path)
    pipeline = SilverEntityPipeline(entity_config)
    return pipeline.run_once(source_payloads=_sample_payloads(), run_id="customer_profile_example_run")


def run_customer_profile_replay_example() -> dict:
    config_path = Path(__file__).with_name("config.yaml")
    entity_config = load_entity_config_from_path(config_path)
    pipeline = SilverEntityPipeline(entity_config)
    source_payloads = _sample_payloads()

    full_history = pipeline.run_full_rebuild(source_payloads=source_payloads, run_id="customer_profile_full_rebuild")
    backfill_history = pipeline.run_backfill(
        source_payloads=source_payloads,
        existing_history=[],
        start_time="2026-01-01T10:05:00Z",
        end_time="2026-01-01T10:20:00Z",
        run_id="customer_profile_backfill",
    )
    key_replay_history = pipeline.run_key_replay(
        source_payloads=source_payloads,
        existing_history=full_history,
        keys=[{"customer_id": "C001"}],
        run_id="customer_profile_key_replay",
    )
    return {
        "full_rebuild_rows": len(full_history),
        "backfill_rows": len(backfill_history),
        "key_replay_rows": len(key_replay_history),
        "latest_checkpoint": pipeline.get_latest_checkpoint(),
        "run_history_count": len(pipeline.get_run_history()),
    }


if __name__ == "__main__":
    rows = run_customer_profile_example()
    for row in rows:
        print(row)
    print(run_customer_profile_replay_example())
