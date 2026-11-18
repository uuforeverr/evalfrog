# Bootstrap and Discovery

## Required context

Require all three values before an API operation:

```text
EF_SERVER=https://<control-plane>
EF_PROJECT=<project-uuid>
EF_TOKEN=<bearer-token>
```

Use the installed `evalfrog` binary. The CLI has no `auth login`, default
project, or Token-issuance command. `EF_SERVER`, `EF_PROJECT`, and `EF_TOKEN`
are the command defaults; explicit `--server`, `--project`, and `--token`
flags override them for a single command. If a value is missing or a request
returns 401/403, stop and ask for the correct authorized context.

## First commands

```bash
evalfrog version
evalfrog node-type list \
  --server "$EF_SERVER" --token "$EF_TOKEN" --project "$EF_PROJECT"
```

`node-type list` returns the authoring description: allowed Inputs, types,
literal/ref restrictions, Output rules, and examples. Use that response rather
than inventing Node fields.

For HTTP Workflows, discover only safe Connection summaries:

```bash
evalfrog connection list \
  --server "$EF_SERVER" --token "$EF_TOKEN" --project "$EF_PROJECT"
```

The first phase has no CLI command to list Workflows, inspect a Workflow by ID,
list RPC Services, set a default Project, or upload DSL. `workflow builder
preview --out` can export the current local Builder Session IR, but does not
expose DSL. Ask the user for the missing Workflow ID / Published Version / RPC
reference; do not use private database access or undocumented HTTP endpoints.

## Local development checks

When working from the EvalFrog repository, use these configuration-only checks:

```bash
evalfrog config validate --profile local --config-dir configs
evalfrog doctor --profile local --config-dir configs
```

`doctor` verifies the configured local Control Plane readiness. It does not
prove that the supplied API Token has access to the Project.

## CLI response conventions

- `workflow builder ...` emits a JSON envelope. Read `ok`, then retain the
  Session, Workflow ID, Revision and `dirty` state from `data`; on failure,
  read `error.code` and `error.message`.
- `workflow create|copy`, `draft push`, and `publish` emit concise text. Parse
  the Workflow UUID / Revision / Version from the success line and retain it.
- `node-type list`, `connection list`, `run status`, and `run diagnose` emit
  JSON.
- Exit code `0` is success; `1` is local/API failure; `2` is invalid command
  use or arguments.
