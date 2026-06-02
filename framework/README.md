# Framework architecture (Silver-first)

## Goals

- Keep business semantics explicit in metadata.
- Keep source-specific CDC parsing outside SCD2 state assembly.
- Keep Silver reproducible from Bronze/stage inputs for replay/backfill.
- Keep contracts generic so Bronze/Gold can be added with minimal redesign.

## Layered separation

1. **metadata/**  
   Typed config contracts and loaders.
2. **normalization/**  
   Source CDC -> canonical change records.
3. **scd2/**  
   Canonical change records -> Silver canonical SCD2 history.
4. **orchestration/**  
   Batch/stream/replay execution wiring.
5. **quality/**  
   Validation hooks and quality gate interfaces.
6. **examples/**  
   Entity-specific metadata and lightweight wiring.

## Replay/backfill design intent

- `SilverEntityPipeline` exposes `run_backfill`, `run_full_rebuild`, `run_key_replay`, `run_streaming`.
- `SilverSCD2Engine` implements in-memory replay helpers for:
  - historical window replay (`run_backfill`)
  - key-targeted replay (`run_key_replay`)
  - full table rebuild (`run_full_rebuild`)
- `orchestration/run_registry.py` provides:
  - run ledger records (mode/status/start/end)
  - latest checkpoint per entity
  - run completion/failure tracking

## Current implementation boundaries

- Includes in-memory reference implementation contracts.
- Checkpointing exists in-memory; Delta-backed checkpoint storage is not yet implemented.
- Does not yet include Bronze ingestion or Gold transforms.
