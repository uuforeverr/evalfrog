import test from 'node:test';
import assert from 'node:assert/strict';
import { referenceOptions, updateNodeConfiguration } from '../src/node-editor.js';

const ir = { ir_version: '1', nodes: [
  { id: 'start', type: 'start', title: 'Start', inputs: [], outputs: [{ name: 'workflow_input', data_type: 'object' }] },
  { id: 'code', type: 'code', title: 'Code', inputs: [], outputs: [{ name: 'score', data_type: 'integer' }] },
  { id: 'branch', type: 'branch', title: 'Branch', inputs: [
    { name: 'value', data_type: 'integer', source: 'literal', value: 85 },
    { name: 'cases', data_type: 'array', source: 'literal', value: [{ route: 'approved', operator: 'gte', value: 80 }] },
    { name: 'default_route', data_type: 'string', source: 'literal', value: 'rejected' },
  ], outputs: [] },
], edges: [{ id: 'one', source: 'start', target: 'code' }, { id: 'two', source: 'code', target: 'branch' }], layout: {} };
const description = { type: 'branch', allowed_inputs: [
  { name: 'value', required: true, data_types: ['integer', 'number', 'string', 'object'], sources: ['literal', 'ref'] },
  { name: 'cases', required: true, data_types: ['array'], sources: ['literal'] },
  { name: 'default_route', required: true, data_types: ['string'], sources: ['literal'] },
], outputs: { mode: 'fixed', fields: [] } };

test('reference picker only includes reachable outputs with compatible types', () => {
  assert.deepEqual(referenceOptions(ir, 'branch', 'integer').map(({ node, output }) => `${node}.${output}`), ['code.score']);
  assert.deepEqual(referenceOptions(ir, 'branch', 'number').map(({ node, output }) => `${node}.${output}`), ['code.score']);
  assert.deepEqual(referenceOptions(ir, 'code', 'integer'), []);
});

test('branch value can reference an upstream output without changing the source IR', () => {
  const inputs = structuredClone(ir.nodes[2].inputs);
  inputs[0] = { name: 'value', data_type: 'integer', source: 'ref', ref_node: 'code', ref_output: 'score' };
  const updated = updateNodeConfiguration(ir, 'branch', 'Check score', inputs, [], description);
  assert.equal(updated.nodes[2].inputs[0].ref_output, 'score');
  assert.equal(updated.nodes[2].title, 'Check score');
  assert.equal(ir.nodes[2].inputs[0].source, 'literal');
  assert.throws(() => updateNodeConfiguration(ir, 'branch', 'Check', [{ ...inputs[0], ref_node: 'branch' }, ...inputs.slice(1)], [], description), /前序输出/);
});
