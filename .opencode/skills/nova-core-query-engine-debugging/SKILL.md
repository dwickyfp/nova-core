---
name: nova-core-query-engine-debugging
description: Use when debugging SQL execution, MySQL protocol, analyzer/planner/optimizer, scheduler, workers, DataFusion operators, or query result mismatches in nova-core.
---

# Nova Core Query Engine Debugging

Use this skill for failures in SQL parsing, planning, optimization, scheduling, MySQL protocol handling, worker execution, DataFusion integration, or query results.

## Debugging Loop

1. Load `nova-core-orientation` and `systematic-debugging` from Superpowers when available.
2. Reproduce with the smallest query or test that fails.
3. Classify the layer:
   - MySQL protocol: connection, auth, packet/result formatting.
   - Parser/analyzer: SQL AST, names, types, catalog lookup.
   - Optimizer/planner: logical/physical plan, pruning, join/order/limit behavior.
   - Scheduler/distributed: fragments, workers, gRPC dispatch, retries.
   - Worker/DataFusion: execution plan, streams, Arrow batches.
   - Storage: micro-partition reads, object store, FDB metadata.
4. Use CodeGraph to trace the exact symbol path before editing.
5. Add regression tests at the lowest layer that proves the bug.

## Useful CodeGraph Prompts

```bash
codegraph explore "mysql protocol query execution path"
codegraph explore "analyzer planner optimizer scheduler execute query"
codegraph explore "MicroPartitionScanExec DataFusion execution"
codegraph explore "FragmentDispatcher worker grpc dispatch"
```

## Common Failure Patterns

- SQL text protocol paths bypass auth or session context.
- Analyzer accepts invalid names/types that later panic in planning.
- Optimizer rewrites change semantics for NULL, LIMIT, ORDER BY, or joins.
- DataFusion streams are consumed incorrectly or produce mismatched schemas.
- Micro-partition pruning skips MPs that might match due to inclusive/exclusive bound mistakes.
- Distributed execution has a single-node fast path that diverges from gRPC behavior.

## Verification Targets

Run the narrowest test first, then broaden:

```bash
cargo test -p nova-coordinator mysql
cargo test -p nova-coordinator analyzer
cargo test -p nova-coordinator optimizer
cargo test -p nova-worker operators
cargo test --all query
cargo test --all integration
```

For performance-sensitive fixes, run the relevant benchmark only after correctness tests pass.

## Completion Checklist

- Include the failing layer and root cause.
- Include the exact query/test used to reproduce.
- Include why the fix preserves SQL semantics and architecture invariants.
- Include regression tests or explain why they could not run locally.
