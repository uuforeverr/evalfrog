# Run Operations and Diagnostics

Both Draft Test and Production Run require user confirmation because they may
execute sandboxed Python, HTTP, or RPC operations. Supply a JSON **object**
input file and an RFC3339 deadline.

## Test the current Draft

```bash
evalfrog run test \
  --server "$EF_SERVER" --token "$EF_TOKEN" --project "$EF_PROJECT" \
  --workflow "$WORKFLOW_ID" --input ./run-input.json \
  --deadline "2026-08-15T16:30:00Z" \
  --idempotency-key "test-order-normalizer-001"
```

The test command reads the current local Workspace Revision and executes an
immutable Draft snapshot. It does not publish or activate a Version.

## Create a Production Run

```bash
evalfrog run create \
  --server "$EF_SERVER" --token "$EF_TOKEN" --project "$EF_PROJECT" \
  --workflow "$WORKFLOW_ID" --input ./run-input.json \
  --deadline "2026-08-15T16:45:00Z" \
  --idempotency-key "run-order-normalizer-001"
```

Production Run uses the active immutable Published Version. The command is
rejected if none exists; clients cannot select an unpublished Draft or Version.
Record the returned Run UUID.

## Observe and diagnose

```bash
evalfrog run status \
  --server "$EF_SERVER" --token "$EF_TOKEN" --project "$EF_PROJECT" \
  --run "$RUN_ID"

evalfrog run diagnose \
  --server "$EF_SERVER" --token "$EF_TOKEN" --project "$EF_PROJECT" \
  --run "$RUN_ID"
```

Use bounded polling with a user-agreed timeout. Diagnostics map the immutable
snapshot's runtime failure back to logical IR Node ID, Edge ID, and field path;
repair IR at that location, then Push/Validate/Test again.

## Cancel and recovery replay

```bash
evalfrog run cancel \
  --server "$EF_SERVER" --token "$EF_TOKEN" --project "$EF_PROJECT" \
  --run "$RUN_ID"
```

Cancel persists termination intent; Engine convergence is asynchronous. Do not
claim all Workers stopped merely because the request was accepted.

Use `run replay --run ... --event-type ... --aggregate-id ...` only after a
Project Admin explicitly requests recovery. It rechecks one current actionable
runtime fact; it cannot inject an arbitrary Kafka payload and should not be
used as a general retry button.
