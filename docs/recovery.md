# Retry and rebuild

A job run ID identifies one orchestration attempt. A stream identity identifies
one persistent ingestion history. Do not change the stream identity on a retry.

## Retry after interruption

Keep the same catalog/schema, generation, input, and both checkpoint directories.
Run the job again with a new job run ID. Silver MERGEs are idempotent, and its
batch-completion audit is written last. A new run recomputes gold after silver
succeeds. There is no transaction spanning all silver or gold tables.

If deliberately testing an injected failure, remove `fail_after_sinks` before
retrying in a new process or cloud task. The test injection flag is not a durable
"fail once" switch. For forced gold failure, set
`force_gold_assertion_failure=false` on the recovery run.

`pipeline_stream_state` records the effective stream identity, generation,
checkpoint location, and Spark checkpoint UUID. Startup refuses a changed,
missing, replaced, or unregistered checkpoint against existing target state.
This checks directory/UUID continuity, not the integrity of every offset or commit log file. Partial checkpoint corruption also requires an isolated rebuild.

This prevents Delta transaction versions from silently suppressing new bronze
writes after a checkpoint reset. It also prevents a fresh target from adopting a
checkpoint whose files have already been processed elsewhere.

## Full rebuild without deleting existing data

Create **all three** afresh: output namespace, stream generation, and checkpoint
directories. Keep the immutable source files. Example from the repository root:

```bash
python -m telemetry.orchestrator \
  --events data/events.jsonl \
  --customers data/customers.jsonl \
  --warehouse warehouse_rebuild_v2 \
  --namespace security_telemetry_rebuild_v2 \
  --stream-generation v2 \
  --bronze-checkpoint checkpoints/rebuild_v2/bronze \
  --silver-checkpoint checkpoints/rebuild_v2/silver
```

Compare counts, canonical state, and all three gold totals against the expected
fixture before directing any consumer to the rebuilt schema. The previous
schema and checkpoints remain available. The new input paths can point to the
same immutable source files; output tables and checkpoints must be separate.

For Databricks, use a fresh dedicated schema and generation with corresponding
volume checkpoint paths, then deploy/run the bundle with those settings. Do not
reset a checkpoint while retaining the same populated output tables or choose a
new generation against an old schema. See [Databricks setup](databricks.md).

## Upgrading the original repository

The original implementation had no stream registry. This version deliberately
refuses automatic adoption of its populated tables/checkpoints. Preserve that
state and perform an isolated rebuild using the recipe above. The repository's
synthetic inputs are small enough to rebuild and compare explicitly. No migration
or automatic data deletion is performed.

## Publication boundary

`successful_publication` advances only after reconciliation passes. It is an
audit marker, not a multi-table transaction or a versioned snapshot. A failure
during gold can leave some live gold tables changed while the marker remains at
the previous successful run. Consumers needing an atomic, stable snapshot would
require an additional publication design, outside this small portfolio project.
