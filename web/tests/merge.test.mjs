import test from 'node:test';
import assert from 'node:assert/strict';
import { mergeChoices, selectedIR } from '../src/merge.js';

const node = (id, title = id) => ({ id, type: 'code', title, inputs: [], outputs: [] });
const ir = (nodes, edges = [], layout = {}) => ({ ir_version: '1', nodes, edges, layout });

test('every changed node and edge requires an explicit choice', () => {
  const base = ir([node('start'), node('end')], [{ id: 'path', source: 'start', target: 'end' }]);
  const latest = ir([node('start', 'A'), node('end')], base.edges);
  const local = ir([node('start'), node('end', 'B')], [...base.edges, { id: 'extra', source: 'start', target: 'end' }]);
  const preview = mergeChoices(base, latest, local);
  assert.deepEqual(preview.choices.map((item) => `${item.kind}:${item.id}`), ['nodes:end', 'nodes:start', 'edges:extra']);
  assert.throws(() => selectedIR(preview, {}), /请选择/);
  const merged = selectedIR(preview, { 'nodes:end': 'local', 'nodes:start': 'latest', 'edges:extra': 'remove' });
  assert.deepEqual(merged.nodes.map((item) => item.title), ['B', 'A']);
  assert.deepEqual(merged.edges, base.edges);
});

test('deleted dependencies cannot be accepted', () => {
  const base = ir([node('start'), node('end')], [{ id: 'path', source: 'start', target: 'end' }]);
  const preview = mergeChoices(base, ir([node('start')]), base);
  assert.throws(() => selectedIR(preview, { 'nodes:end': 'remove', 'edges:path': 'base' }), /端点不存在/);
});

test('a second conflict can use the chosen result as its local version', () => {
  const base = ir([node('one')]);
  const first = mergeChoices(base, ir([node('one', 'A')]), ir([node('one', 'B')]));
  const chosen = selectedIR(first, { 'nodes:one': 'local' });
  const second = mergeChoices(ir([node('one', 'A')]), ir([node('one', 'C')]), chosen);
  assert.equal(second.choices[0].versions.local.title, 'B');
});

test('a concurrent edit preserves a local IR version upgrade', () => {
  const base = ir([node('one')]);
  const latest = ir([node('one', 'remote')]);
  const local = { ...ir([node('one', 'local')]), ir_version: '2' };
  const preview = mergeChoices(base, latest, local);
  const merged = selectedIR(preview, { 'nodes:one': 'local' });
  assert.equal(merged.ir_version, '2');
});

test('field order and unchanged connections do not require choices', () => {
  const base = ir([node('a'), node('b')], [{ id: 'path', source: 'a', target: 'b' }]);
  const latest = ir([{ outputs: [], inputs: [], title: 'a', type: 'code', id: 'a' }, node('b')], [{ target: 'b', id: 'path', source: 'a' }]);
  const local = ir(base.nodes, [{ id: 'path', source: 'a', target: 'b', display_color: 'blue' }]);
  const preview = mergeChoices(base, latest, local);
  assert.deepEqual(preview.choices, []);
  assert.deepEqual(selectedIR(preview, {}).edges[0].source, 'a');
});

test('node parameter changes require one node choice without repeating its unchanged edge', () => {
  const base = ir([node('a'), node('b')], [{ id: 'path', source: 'a', target: 'b' }]);
  const latest = ir([{ ...node('a'), inputs: [{ name: 'x', data_type: 'integer', source: 'literal', value: 1 }] }, node('b')], base.edges);
  const preview = mergeChoices(base, latest, base);
  assert.deepEqual(preview.choices.map((choice) => `${choice.kind}:${choice.id}`), ['nodes:a']);
});

test('rewired edge is a choice while one-sided layout movement is automatic', () => {
  const base = ir([node('a'), node('b'), node('c')], [{ id: 'path', source: 'a', target: 'b' }], { a: { x: 0, y: 0 } });
  const latest = ir(base.nodes, [{ id: 'path', source: 'a', target: 'c' }], { a: { x: 50, y: 0 } });
  const preview = mergeChoices(base, latest, base);
  assert.deepEqual(preview.choices.map((choice) => `${choice.kind}:${choice.id}`), ['edges:path']);
  assert.deepEqual(preview.result.layout.a, { x: 50, y: 0 });
});

test('dragged merge position is saved for a kept node and removed with a deleted node', () => {
  const base = ir([node('a')], [], { a: { x: 0, y: 0 } });
  const preview = mergeChoices(base, ir([node('a', 'remote')], [], { a: { x: 30, y: 0 } }), ir([node('a', 'local')], [], { a: { x: 0, y: 40 } }));
  const keep = selectedIR(preview, { 'nodes:a': 'local', 'layout:a': 'custom' }, { a: { x: 90, y: 75 } });
  assert.deepEqual(keep.layout.a, { x: 90, y: 75 });
  const remove = selectedIR(preview, { 'nodes:a': 'remove', 'layout:a': 'custom' }, { a: { x: 90, y: 75 } });
  assert.deepEqual(remove.layout, {});
});
