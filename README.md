# lakeflow-sdp-meta

Metadata-driven medallion framework skeleton for Databricks, starting with Silver-layer CDC entity assembly and SCD2 history management.

## What is included

- **Framework-first package layout** with clear extension points for Bronze/Silver/Gold and orchestration/replay
- **Typed metadata models** for entity/source/mapping configuration
- **Config loader** for dict/JSON/YAML inputs (easy to swap for Delta metadata tables later)
- **Reusable source normalization** that converts source-specific CDC events into canonical Silver-stage change records
- **Reusable entity-agnostic SCD2 engine skeleton** with deterministic ordering and configurable delete semantics
- **Pipeline/orchestration skeleton** with hooks for:
  - `run_streaming()`
  - `run_backfill(...)`
  - `run_full_rebuild()`
  - `run_key_replay(...)`
- **Quality/validation hook** with `validate_target(...)`
- **Sample entity** (`customer_profile`) showing two Bronze sources (`customer_core`, `customer_address`) feeding one Silver SCD2 entity

## Repository structure

```text
framework/
  metadata/
    models.py
    loader.py
  normalization/
    source_normalizer.py
  scd2/
    engine.py
  orchestration/
    silver_pipeline.py
  quality/
    validator.py
  examples/
    customer_profile/
      config.yaml
      pipeline.py
```

## Canonical change record contract

Normalization emits canonical change records consumed by the SCD2 engine. This boundary keeps source-specific parsing separate from entity-state assembly:

- `entity_name`
- `source_name`
- `business_key` (dict)
- `payload` (dict of mapped business attributes; partial updates allowed)
- `operation` (`upsert` / `delete`)
- `delete_semantic` (`entity_delete` / `nullify_component` / `ignore_delete`)
- `event_time`
- `source_event_id`
- `source_position`
- `source_priority`
- `ingest_ts`
- `ordering_fields`
- `record_source`

## Silver SCD2 output columns

The engine emits canonical SCD2 rows that include at least:

- `effective_from`
- `effective_to`
- `is_current`
- `is_deleted`
- `change_hash`
- `event_time`
- `processed_time`
- `record_source`
- `source_event_id`
- `source_position`
- `source_priority`
- `run_id`

`is_current = true` can be used later to expose a current snapshot view.

## Sample usage

```python
from framework.examples.customer_profile.pipeline import run_customer_profile_example

history = run_customer_profile_example()
for row in history:
    print(row)
```

## Extension path

1. Replace file-based metadata loading with Delta metadata tables.
2. Plug Spark DataFrame readers/writers into normalization and SCD2 persistence steps.
3. Add Bronze ingestion metadata + processors under `framework/bronze`.
4. Add Gold semantic modeling under `framework/gold`.
5. Add orchestration policies (checkpointing, run registry, replay policies, data quality gates).

## Notes

- Current implementation is intentionally practical and readable.
- Replay/backfill orchestration methods are scaffolded for future completion.