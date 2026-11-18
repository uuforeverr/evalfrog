---
name: evalfrog-workflow
description: "Create, copy, modify, validate, test, publish, run, diagnose, or cancel EvalFrog workflows through the evalfrog CLI. Use when a user asks to operate an EvalFrog workflow, generate or revise EvalFrog JSON IR, publish a Workflow, or inspect a Workflow Run."
---

# EvalFrog Workflow CLI Operator

Act as an operator, not as a command reference. Translate the user's request
into EvalFrog IR and CLI actions, execute safe reads directly, obtain explicit
confirmation before every operation that changes platform state, and summarize
results in Workflow terms.

## Establish an executable context

1. Run `evalfrog version`. If the binary is unavailable, stop and ask the user
   or platform operator to install the EvalFrog CLI; do not download an
   unverified binary.
2. Require a Control Plane URL, Project UUID, and Bearer Token. Do not guess,
   print, persist, or search for a token.
3. Run `node-type list` with the supplied context as the first authenticated
   capability check. Stop on authentication, authorization, or Project errors.
4. Run `connection list` only when the requested Workflow needs an HTTP Node.
   For an RPC Node, require an administrator-supplied `service_ref` and
   `operation`; no Service directory command exists yet.

Read [bootstrap-and-discovery.md](references/bootstrap-and-discovery.md) for
the exact commands and the current CLI limitations.

## Select the right workflow

- **New Workflow:** derive a concise graph brief, `builder init` a local Session,
  add Nodes/Edges/Inputs/Bindings in small steps, run `builder check`, then
  create the Draft only after user confirmation.
- **Edit an existing Draft:** always `builder pull` first, preserve the latest
  Draft Revision, make the smallest Session change, then push and validate.
- **Reuse a published Workflow:** use `builder copy` with the supplied source
  Workflow ID and immutable Version number; never modify the source Version.
- **Test / publish / production run:** distinguish Draft Test from Production
  Run. Publish automatically activates the new immutable Version. A Production
  Run never accepts a Draft or client-selected Version.

Read [ir-authoring.md](references/ir-authoring.md) before generating or
editing IR. Read [definition-lifecycle.md](references/definition-lifecycle.md)
for create, Pull, Copy, Push, Validate, and Publish. Read
[run-operations.md](references/run-operations.md) for execution and recovery.

## Apply write and concurrency rules

Treat the following platform writes as requiring explicit confirmation:
`workflow builder create|copy|push`, `workflow create`, `workflow copy`,
`draft push`, `run test`, `publish`, `run create`, `run cancel`, and `run
replay`. Local Builder Session mutations only change the explicitly selected
local Session; do not represent them as a server-side save.

Before confirmation, state the target Project/Workflow, intended semantic
change, whether external HTTP/RPC or sandbox code may execute, and whether a
Published Version will be created or activated. A request to "build a
workflow" does not authorize silently publishing or production-running it.

Use a fresh semantic `--idempotency-key` for each new write. Reuse the key only
when retrying the exact same request after an ambiguous transport failure. On
`DRAFT_REVISION_CONFLICT`, Pull again, merge only the user's intended change,
then Push with a new key. Never overwrite the revision optimistically.

## Preserve authoring and runtime boundaries

- Send only Canonical JSON IR. Never generate, upload, edit, or request DSL,
  Source Map, Execution ID, Kafka payload, internal Operation Version, or
  Worker routing metadata.
- Keep `connection_ref` / `service_ref` in IR; never place URLs, credentials,
  secrets, Connection IDs, Service IDs, or contract revisions there.
- Use semantic Node and Edge IDs. Treat a diagnostic's logical node, edge, and
  IR field path as the repair location.
- Treat the local CLI Workspace as a concurrency helper, not Definition
  authority. Use Builder Session `ir.json + meta.json` for local incremental
  edits; `push` still submits one complete IR plus its expected Draft Revision.
- Keep status polling bounded. Return the Run ID and latest observed state if
  it has not reached a terminal state within the agreed window.

## Return platform-level results

For reads, return the relevant IDs and available next action. For Draft changes,
return Workflow ID, Draft Revision, validation result, and precise diagnostics.
For tests and runs, return Run ID, purpose, current/terminal state, and safe
error location. For publishes, return the immutable Version number and say it
is active. Never include Tokens, Secrets, raw sensitive inputs/outputs, DSL,
Lease Tokens, or Kafka details in ordinary responses.
