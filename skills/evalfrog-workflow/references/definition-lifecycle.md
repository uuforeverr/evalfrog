# Definition Lifecycle with Builder Sessions

Use `workflow builder` for normal Agent authoring. A Session has two local
files: `ir.json` is the complete canonical authoring document and `meta.json`
tracks the server/project/workflow/revision plus hashes. It never stores the
Bearer Token. Every local edit rewrites only these files; `create` and `push`
are still complete-IR API commands guarded by the Draft Revision.

Every remote write needs explicit user confirmation and a unique semantic
idempotency key. Reuse the key only for a retry of the exact same request.

## Build a new Workflow in small steps

```bash
SESSION=./order-normalizer.session
evalfrog workflow builder init --session "$SESSION"
evalfrog workflow builder add-node --session "$SESSION" --id start --type start --title "Start"
evalfrog workflow builder set-output --session "$SESSION" --node start \
  --name workflow_input --data-type object
evalfrog workflow builder add-node --session "$SESSION" --id normalize --type code --title "Normalize"
evalfrog workflow builder set-input --session "$SESSION" --node normalize \
  --name source_code --data-type string --literal-file ./source-code.json
evalfrog workflow builder bind --session "$SESSION" --node normalize --name request \
  --data-type object --source-node start --source-output workflow_input
evalfrog workflow builder set-output --session "$SESSION" --node normalize \
  --name result --data-type object
```

Add `end`, bind its `workflow_output`, add the control Edges, then run:

```bash
evalfrog workflow builder check --session "$SESSION"
evalfrog workflow builder preview --session "$SESSION" --out ./workflow.ir.json
```

`check` is local structural validation and deliberately allows an Agent to see
diagnostics while it is constructing the graph. It does not replace server
Catalog, resource, Control Graph, or compiler validation.

## Create, edit, and Push

```bash
evalfrog workflow builder create --session "$SESSION" \
  --server "$EF_SERVER" --token "$EF_TOKEN" --project "$EF_PROJECT" \
  --name "Order normalizer" --idempotency-key "create-order-normalizer-001"

evalfrog workflow builder set-title --session "$SESSION" --node normalize --title "Normalize order request"
evalfrog workflow builder push --session "$SESSION" --token "$EF_TOKEN" \
  --idempotency-key "update-order-normalizer-001"
evalfrog workflow builder validate --session "$SESSION" --token "$EF_TOKEN"
```

`create` and `push` refresh the Session Revision and the regular CLI Workspace.
`validate` refuses to validate a dirty Session because the server only knows the
last pushed immutable Draft Revision.

## Pull and Copy

```bash
evalfrog workflow builder pull --session "$SESSION" \
  --server "$EF_SERVER" --token "$EF_TOKEN" --project "$EF_PROJECT" \
  --workflow "$WORKFLOW_ID"

evalfrog workflow builder copy --session ./copy.session \
  --server "$EF_SERVER" --token "$EF_TOKEN" --project "$EF_PROJECT" \
  --source-workflow "$SOURCE_WORKFLOW_ID" --version 3 --name "Order normalizer copy" \
  --idempotency-key "copy-order-normalizer-v3-001"
```

Pull refuses to discard unsaved local mutations unless `--discard-local` is
explicit. Copy creates a new Workflow and Draft from a Published Version, then
binds the new Session to that Workflow; it never modifies the source Version.

## Local edit commands

Use `add-node`, `remove-node`, `add-edge`, `remove-edge`, `set-title`,
`set-input`, `bind`, `remove-input`, `set-output`, `remove-output`, and
`set-layout`. Removing a Node removes its incident Edges and Layout entry but
does not silently rewrite unrelated downstream Input references; run `check`
and fix any resulting diagnostic explicitly.
