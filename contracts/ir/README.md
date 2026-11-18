# IR Contract

`v1/schema.json` is the strict Test/Publish JSON Schema for the shared Human Web and Agent CLI authoring format. Draft Save uses the bounded Go parser and may persist structurally valid but semantically incomplete content; it does not weaken this strict contract.

`v1/fixtures/manifest.json` classifies versioned positive and negative examples used by both JSON Schema and Go contract tests. The schema, model, built-in Node Catalog descriptions, canonicalizer, and fixtures are checked together in CI.

IR deliberately contains no node contract version, operation version, Kafka topic, Worker routing, Lease, Retry, or Timeout author settings.

`v2/schema.json` keeps the same bounded structural envelope and adds `ir_version: "2"`. Its semantic contract is `catalog-v2`: Start declares named invocation parameters through outputs; End binds each named output to a same-named literal or upstream input; Branch supports ordered IF/ELSE IF groups with AND/OR conditions. Existing v1 schemas, catalog revisions, and published snapshots remain available.
