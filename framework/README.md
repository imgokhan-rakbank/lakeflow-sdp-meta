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
- `SilverSCD2Engine` contains matching method stubs.
- These hooks are intentionally scaffolded so a Databricks orchestration layer can later map them to:
  - historical window replay
  - key-targeted replay
  - full table rebuild
  - incremental streaming micro-batches

## Current implementation boundaries

- Includes in-memory reference implementation contracts.
- Does not yet include Delta writes/checkpoints/watermarks.
- Does not yet include Bronze ingestion or Gold transforms.
