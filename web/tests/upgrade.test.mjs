import test from 'node:test';
import assert from 'node:assert/strict';
import { upgradeIRV1 } from '../src/upgrade.js';

test('v1 upgrade preserves node and edge identities and translates Branch cases', () => {
  const original = { ir_version: '1', nodes: [
    { id: 'start', type: 'start', title: 'Start', inputs: [], outputs: [{ name: 'workflow_input', data_type: 'object' }] },
    { id: 'branch', type: 'branch', title: 'Branch', inputs: [
      { name: 'value', data_type: 'object', source: 'ref', ref_node: 'start', ref_output: 'workflow_input' },
      { name: 'cases', data_type: 'array', source: 'literal', value: [{ route: 'pass', path: 'score', operator: 'gte', value: 60 }] },
      { name: 'default_route', data_type: 'string', source: 'literal', value: 'else' },
    ], outputs: [] },
    { id: 'end', type: 'end', title: 'End', inputs: [{ name: 'workflow_output', data_type: 'object', source: 'literal', value: {} }], outputs: [] },
  ], edges: [{ id: 'a', source: 'start', target: 'branch' }, { id: 'b', source: 'branch', target: 'end', route: 'pass' }, { id: 'c', source: 'branch', target: 'end', route: 'else' }], layout: { start: { x: 0, y: 0 }, branch: { x: 100, y: 0 }, end: { x: 200, y: 0 } } };
  const upgraded = upgradeIRV1(original);
  assert.equal(upgraded.ir_version, '2');
  assert.deepEqual(upgraded.nodes.map(({ id }) => id), original.nodes.map(({ id }) => id));
  assert.deepEqual(upgraded.edges, original.edges);
  assert.deepEqual(upgraded.nodes.find(({ id }) => id === 'end').outputs, [{ name: 'workflow_output', data_type: 'object' }]);
  assert.deepEqual(upgraded.nodes.find(({ id }) => id === 'branch').inputs.find(({ name }) => name === 'cases').value,
    [{ route: 'pass', logic: 'and', conditions: [{ input: 'value', path: 'score', operator: 'gte', value: 60 }] }]);
  assert.equal(original.ir_version, '1');
  assert.equal(original.nodes.find(({ id }) => id === 'end').outputs.length, 0);
});

test('incomplete v1 nodes cannot be silently upgraded', () => {
  assert.throws(() => upgradeIRV1({ ir_version: '1', nodes: [{ type: 'start', inputs: [], outputs: [] }, { type: 'end', inputs: [], outputs: [] }] }), /Start/);
});
