import test from 'node:test';
import assert from 'node:assert/strict';
import { branchRoutes, insertAfter, placeNode, nodeRemovalImpact, removeNode, connect, disconnect, setEdgeRoute } from '../src/graph-editor.js';

const base = { ir_version: '1', nodes: [
  { id: 'start', type: 'start', title: 'Start', inputs: [], outputs: [] },
  { id: 'end', type: 'end', title: 'End', inputs: [], outputs: [] },
], edges: [{ id: 'path', source: 'start', target: 'end' }], layout: { start: { x: 80, y: 80 }, end: { x: 600, y: 80 } } };
const template = { id: 'example', type: 'code', title: 'Code', inputs: [], outputs: [] };

test('insert after a node splits its chosen edge and preserves the original IR', () => {
  const result = insertAfter(base, 'start', template, 'path', 'new', 'first', 'second');
  assert.deepEqual(result.edges.map((edge) => [edge.id, edge.source, edge.target]), [['first', 'start', 'new'], ['second', 'new', 'end']]);
  assert.equal(result.layout.new.x, 340);
  assert.equal(base.nodes.length, 2);
  assert.equal(base.edges[0].id, 'path');
});

test('multiple outgoing edges require an explicit insertion location', () => {
  const branched = { ...base, edges: [...base.edges, { id: 'other', source: 'start', target: 'end' }] };
  assert.throws(() => insertAfter(branched, 'start', template, '', 'new', 'a', 'b'), /选择插入位置/);
});

test('delete node reports and removes incident edges and references', () => {
  const graph = { ...base, nodes: [...base.nodes, { id: 'reader', type: 'code', inputs: [{ name: 'input', source: 'ref', ref_node: 'start', ref_output: 'value' }], outputs: [] }] };
  assert.deepEqual(nodeRemovalImpact(graph, 'start'), { edges: ['path'], inputs: ['reader.inputs.input'] });
  const result = removeNode(graph, 'start');
  assert.equal(result.edges.length, 0);
  assert.equal(result.nodes.find((node) => node.id === 'reader').inputs.length, 0);
  assert.equal(result.layout.start, undefined);
});

test('branch connections may remain unfinished in a draft and receive a declared route later', () => {
  const graph = { ...base, edges: [] };
  const connected = connect(graph, 'start', 'end', 'edge_new', '');
  assert.equal(connected.edges[0].id, 'edge_new');
  assert.equal(disconnect(connected, 'edge_new').edges.length, 0);
  assert.throws(() => connect(base, 'start', 'end', 'duplicate', ''), /已存在/);
  const branch = { ...graph, nodes: [{ ...graph.nodes[0], type: 'branch' }, graph.nodes[1] ] };
  branch.nodes[0].inputs = [
    { name: 'cases', value: [{ route: 'approved' }] },
    { name: 'default_route', value: 'rejected' },
  ];
  assert.deepEqual(branchRoutes(branch.nodes[0]), ['approved', 'rejected']);
  const pending = connect(branch, 'start', 'end', 'edge_new', '');
  assert.equal(pending.edges[0].route, undefined);
  assert.equal(setEdgeRoute(pending, 'edge_new', 'approved').edges[0].route, 'approved');
  assert.throws(() => setEdgeRoute(pending, 'edge_new', 'other'), /配置/);
});

test('inserting Branch keeps the prior continuation on its default route', () => {
  const branch = { type: 'branch', title: 'Branch', inputs: [
    { name: 'cases', value: [{ route: 'approved' }] }, { name: 'default_route', value: 'rejected' },
  ], outputs: [] };
  const result = insertAfter(base, 'start', branch, 'path', 'branch', 'first', 'second');
  assert.deepEqual(result.edges.map(({ source, target, route }) => [source, target, route]), [['start', 'branch', undefined], ['branch', 'end', 'rejected']]);
});

test('placing a detached node preserves existing connections for later manual wiring', () => {
  const result = placeNode(base, 'start', template, 'new');
  assert.equal(result.nodes.length, 3);
  assert.deepEqual(result.edges, base.edges);
  assert.ok(result.layout.new.x > base.layout.start.x);
});
