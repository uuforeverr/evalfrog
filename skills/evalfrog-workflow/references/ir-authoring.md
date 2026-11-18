# IR Authoring

## Authoring contract

Only write JSON IR with this top-level shape:

```json
{
  "ir_version": "1",
  "nodes": [],
  "edges": [],
  "layout": {}
}
```

Use semantic, stable logical IDs such as `normalize_request` and
`normalize_to_end`. Node IDs and Edge IDs are authored offline; do not create
DSL execution IDs. `layout` is shared Canvas metadata and still creates a new
Draft Revision, but is never compiled into DSL.

Each Node has `id`, `type`, `title`, `inputs`, and `outputs`. Each Input is one
of these mutually exclusive forms:

```json
{ "name": "message", "data_type": "string", "source": "literal", "value": "hello" }
{ "name": "request", "data_type": "object", "source": "ref", "ref_node": "start", "ref_output": "workflow_input" }
```

The only primitive data types are `string`, `integer`, `number`, `boolean`,
`object`, and `array`. An `integer` may feed `number`; no other implicit type
conversion exists. Do not invent `params`, `ports`, `bindings`, Node versions,
or execution policy fields.

## Current Node types

Use the live `node-type list` response as the authority. The current built-ins
are `start`, `end`, `branch`, `code`, `http`, and `rpc`:

- `start` exposes `workflow_input` as an object.
- `end` consumes the reachable object Input `workflow_output`.
- `branch` uses `value`, `cases`, and `default_route`; each outgoing branch edge
  carries a matching `route`.
- `code` requires literal `source_code`. Its Python entry point is
  `main(inputs)` and must return a JSON object matching declared Outputs.
- `http` uses an authorized literal `connection_ref`, a literal method, and a
  relative `relative_path`; optional runtime Inputs include query, headers, and
  body. Never use an absolute URL or credential.
- `rpc` uses authorized literal `service_ref` plus literal `operation`, then a
  `request` Input. The service contract is resolved server-side.

## Control and data invariants

- Build exactly one Start and one End in a reachable DAG.
- Add an Edge for every execution/control transition. Use `route` only for a
  Branch outgoing edge.
- A `ref_node` must be an upstream node that can reach the target node through
  the control graph. A correctly named Output on an unreachable node is still
  invalid.
- Declare every Output used by a downstream Input, and make the data types
  compatible.
- Do not give Code Nodes network/database/platform credentials. HTTP and RPC
  must use managed resources.

## Minimal Code Workflow

```json
{
  "ir_version": "1",
  "nodes": [
    {
      "id": "start",
      "type": "start",
      "title": "Start",
      "inputs": [],
      "outputs": [{ "name": "workflow_input", "data_type": "object" }]
    },
    {
      "id": "normalize_request",
      "type": "code",
      "title": "Normalize request",
      "inputs": [
        { "name": "source_code", "data_type": "string", "source": "literal", "value": "def main(inputs):\n    return {'result': {'message': inputs['request'].get('message', '')}}" },
        { "name": "request", "data_type": "object", "source": "ref", "ref_node": "start", "ref_output": "workflow_input" }
      ],
      "outputs": [{ "name": "result", "data_type": "object" }]
    },
    {
      "id": "end",
      "type": "end",
      "title": "End",
      "inputs": [{ "name": "workflow_output", "data_type": "object", "source": "ref", "ref_node": "normalize_request", "ref_output": "result" }],
      "outputs": []
    }
  ],
  "edges": [
    { "id": "start_to_normalize", "source": "start", "target": "normalize_request" },
    { "id": "normalize_to_end", "source": "normalize_request", "target": "end" }
  ],
  "layout": {
    "start": { "x": 80, "y": 160 },
    "normalize_request": { "x": 360, "y": 160 },
    "end": { "x": 640, "y": 160 }
  }
}
```
