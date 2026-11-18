import { test, expect } from '@playwright/test';
import { createServer } from 'node:http';
import { readFile } from 'node:fs/promises';
import { join } from 'node:path';

const base = { ir_version: '1', nodes: [{ id: 'task', type: 'code', title: 'Base', inputs: [], outputs: [] }], edges: [], layout: {} };
const latest = { ...base, nodes: [{ ...base.nodes[0], title: 'A' }] };
const local = { ...base, nodes: [{ ...base.nodes[0], title: 'B' }] };
const visualBase = { ir_version: '1', nodes: [
  { id: 'start', type: 'start', title: 'Start', inputs: [], outputs: [] },
  { id: 'end', type: 'end', title: 'End', inputs: [], outputs: [] },
], edges: [{ id: 'path', source: 'start', target: 'end' }], layout: { start: { x: 80, y: 100 }, end: { x: 600, y: 100 } } };
const branchBase = { ir_version: '1', nodes: [
  { id: 'start', type: 'start', title: 'Start', inputs: [], outputs: [{ name: 'workflow_input', data_type: 'object' }] },
  { id: 'code', type: 'code', title: 'Code', inputs: [], outputs: [{ name: 'score', data_type: 'integer' }] },
  { id: 'branch', type: 'branch', title: 'Branch', inputs: [
    { name: 'value', data_type: 'integer', source: 'literal', value: 85 },
    { name: 'cases', data_type: 'array', source: 'literal', value: [{ route: 'approved', operator: 'gte', value: 80 }] },
    { name: 'default_route', data_type: 'string', source: 'literal', value: 'rejected' },
  ], outputs: [] },
  { id: 'end', type: 'end', title: 'End', inputs: [], outputs: [] },
], edges: [{ id: 's_c', source: 'start', target: 'code' }, { id: 'c_b', source: 'code', target: 'branch' }, { id: 'b_e', source: 'branch', target: 'end', route: 'rejected' }], layout: { start: { x: 20, y: 100 }, code: { x: 200, y: 100 }, branch: { x: 380, y: 100 }, end: { x: 580, y: 100 } } };
const catalog = [
  { type: 'start', kind: 'control', allowed_inputs: [], outputs: { mode: 'fixed', fields: [{ name: 'workflow_input', data_type: 'object' }] }, examples: [] },
  { type: 'end', kind: 'control', allowed_inputs: [], outputs: { mode: 'fixed', fields: [] }, examples: [] },
  { type: 'code', kind: 'task', allowed_inputs: [], additional_inputs: { data_types: ['string', 'integer', 'number', 'boolean', 'object', 'array'], sources: ['literal', 'ref'] }, outputs: { mode: 'author_declared', allowed_data_types: ['string', 'integer', 'number', 'boolean', 'object', 'array'], min_fields: 0, max_fields: 128 }, examples: [{ id: 'example', type: 'code', title: 'Code', inputs: [], outputs: [] }] },
  { type: 'branch', kind: 'control', allowed_inputs: [
    { name: 'value', required: true, data_types: ['string', 'integer', 'number', 'boolean', 'object', 'array'], sources: ['literal', 'ref'] },
    { name: 'cases', required: true, data_types: ['array'], sources: ['literal'], literal_constraints: { branch_cases: { operators: { integer: ['eq', 'gte'], string: ['eq'] } } } },
    { name: 'default_route', required: true, data_types: ['string'], sources: ['literal'] },
  ], outputs: { mode: 'fixed', fields: [] }, examples: [{ id: 'check', type: 'branch', title: 'Check', inputs: [
    { name: 'value', data_type: 'integer', source: 'literal', value: 85 },
    { name: 'cases', data_type: 'array', source: 'literal', value: [{ route: 'approved', operator: 'gte', value: 80 }] },
    { name: 'default_route', data_type: 'string', source: 'literal', value: 'rejected' },
  ], outputs: [] }] },
];
let server, url, saves;

test.beforeAll(async () => {
  saves = [];
  server = createServer(async (request, response) => {
    if (request.url?.startsWith('/api/')) {
      response.setHeader('Content-Type', 'application/json');
      if (request.method === 'GET' && request.url.endsWith('/draft')) {
        response.end(JSON.stringify({ current: { revision_number: 1, ir: request.url.includes('/visual/') || request.url.includes('/edge-conflict/') ? visualBase : request.url.includes('/branch/') ? branchBase : base } }));
        return;
      }
      if (request.method === 'PUT' && request.url.endsWith('/draft')) {
        const chunks = []; for await (const chunk of request) chunks.push(chunk);
        const submitted = JSON.parse(Buffer.concat(chunks).toString()); saves.push({ ...submitted, key: request.headers['idempotency-key'] });
        if (submitted.expected_revision === 1) {
          response.writeHead(409);
          const edgeConflict = request.url.includes('/edge-conflict/');
          response.end(JSON.stringify({ error: { code: 'DRAFT_REVISION_CONFLICT', message: 'changed', details: {
            base_revision: 1, current_revision: 2, base_ir: edgeConflict ? visualBase : base, current_ir: edgeConflict ? { ...visualBase, edges: [] } : latest, submitted_ir: submitted.ir,
          } } }));
        } else response.end(JSON.stringify({ revision_number: 3, ir: submitted.ir }));
        return;
      }
      response.end(JSON.stringify({ node_types: catalog })); return;
    }
    const name = request.url === '/' ? 'index.html' : request.url?.slice(1);
    if (!['index.html', 'app.js', 'merge.js', 'graph-editor.js', 'node-editor.js', 'upgrade.js', 'styles.css', 'merge.css', 'editor.css'].includes(name)) { response.writeHead(404); response.end(); return; }
    response.setHeader('Content-Type', name.endsWith('.js') ? 'text/javascript' : name.endsWith('.css') ? 'text/css' : 'text/html');
    response.end(await readFile(join('src', name)));
  });
  await new Promise((resolve) => server.listen(0, '127.0.0.1', resolve));
  url = `http://127.0.0.1:${server.address().port}`;
});

test('canvas plus, ports, Backspace and context menu edit the workflow', async ({ page }) => {
  await page.goto(url);
  await page.locator('#project-id').fill('project');
  await page.locator('#workflow-id').fill('visual');
  await page.locator('#token').fill('test-token');
  await page.locator('#load').click();
  await expect(page.locator('.node')).toHaveCount(2);
  await page.locator('.node[data-node-id="start"] .node-add').click();
  await expect(page.locator('#node-picker')).toBeVisible();
  await page.locator('#picker-type').selectOption('code');
  await page.locator('#picker-confirm').click();
  await expect(page.locator('.node')).toHaveCount(3);
  let ir = JSON.parse(await page.locator('#ir').inputValue());
  const inserted = ir.nodes.find((node) => node.type === 'code');
  expect(ir.edges).toHaveLength(2);
  expect(ir.edges.some((edge) => edge.source === 'start' && edge.target === inserted.id)).toBe(true);
  expect(ir.edges.some((edge) => edge.source === inserted.id && edge.target === 'end')).toBe(true);
  const node = page.locator(`.node[data-node-id="${inserted.id}"]`);
  const before = ir.layout[inserted.id];
  const box = await node.boundingBox();
  await page.mouse.move(box.x + 50, box.y + 30);
  await page.mouse.down();
  await page.mouse.move(box.x + 110, box.y + 70, { steps: 5 });
  await page.mouse.up();
  ir = JSON.parse(await page.locator('#ir').inputValue());
  expect(ir.layout[inserted.id].x).toBeGreaterThan(before.x);
  expect(ir.layout[inserted.id].y).toBeGreaterThan(before.y);
  const output = await page.locator('.node[data-node-id="start"] .port-out').boundingBox();
  const input = await page.locator('.node[data-node-id="end"] .port-in').boundingBox();
  await page.mouse.move(output.x + output.width / 2, output.y + output.height / 2);
  await page.mouse.down();
  await page.mouse.move(input.x + input.width / 2, input.y + input.height / 2, { steps: 8 });
  await page.mouse.up();
  ir = JSON.parse(await page.locator('#ir').inputValue());
  expect(ir.edges).toHaveLength(3);
  const canvas = await page.locator('#canvas').boundingBox();
  await page.mouse.click(canvas.x + 425, canvas.y + 136);
  await page.keyboard.press('Backspace');
  expect(JSON.parse(await page.locator('#ir').inputValue()).edges).toHaveLength(2);
  await node.click({ button: 'right' });
  await expect(page.locator('#context-menu')).toBeVisible();
  page.once('dialog', (dialog) => dialog.accept());
  await page.locator('#context-delete').click();
  await expect(page.locator('.node')).toHaveCount(2);
  ir = JSON.parse(await page.locator('#ir').inputValue());
  expect(ir.edges).toHaveLength(0);
});

test('right click removes an edge, while Backspace respects text input focus', async ({ page }) => {
  await page.goto(url);
  await page.locator('#project-id').fill('project');
  await page.locator('#workflow-id').fill('visual');
  await page.locator('#token').fill('test-token');
  await page.locator('#load').click();
  await expect(page.locator('.node')).toHaveCount(2);
  await page.locator('.node[data-node-id="start"]').click();
  await page.locator('#node-title').fill('Start X');
  await page.keyboard.press('Backspace');
  await expect(page.locator('.node')).toHaveCount(2);
  await expect(page.locator('#node-title')).toHaveValue('Start ');
  const canvas = await page.locator('#canvas').boundingBox();
  await page.mouse.click(canvas.x + 425, canvas.y + 136, { button: 'right' });
  await expect(page.locator('#context-menu')).toBeVisible();
  await expect(page.locator('#context-delete')).toHaveText('删除连接');
  await page.locator('#context-delete').click();
  expect(JSON.parse(await page.locator('#ir').inputValue()).edges).toHaveLength(0);
  await page.locator('.node[data-node-id="end"]').click();
  page.once('dialog', (dialog) => dialog.accept());
  await page.keyboard.press('Backspace');
  await expect(page.locator('.node')).toHaveCount(1);
});
test('Branch creation uses default continuation and node editor binds previous output', async ({ page }) => {
  await page.goto(url);
  await page.locator('#project-id').fill('project');
  await page.locator('#workflow-id').fill('visual');
  await page.locator('#token').fill('test-token');
  await page.locator('#load').click();
  await page.locator('.node[data-node-id="start"] .node-add').click();
  await page.locator('#picker-type').selectOption('branch');
  await page.locator('#picker-confirm').click();
  let ir = JSON.parse(await page.locator('#ir').inputValue());
  const branch = ir.nodes.find((node) => node.type === 'branch');
  expect(ir.edges.find((edge) => edge.source === branch.id)?.route).toBe('rejected');
  await page.locator(`.node[data-node-id="${branch.id}"] .node-add`).click();
  await page.locator('#picker-type').selectOption('code');
  await page.locator('#picker-edge').selectOption('__detached');
  await page.locator('#picker-confirm').click();
  ir = JSON.parse(await page.locator('#ir').inputValue());
  const detached = ir.nodes.find((node) => node.type === 'code');
  expect(ir.edges.some((edge) => edge.target === detached.id)).toBe(false);
  const detachedOutput = await page.locator(`.node[data-node-id="${branch.id}"] .port-out`).boundingBox();
  const detachedInput = await page.locator(`.node[data-node-id="${detached.id}"] .port-in`).boundingBox();
  await page.mouse.move(detachedOutput.x + detachedOutput.width / 2, detachedOutput.y + detachedOutput.height / 2);
  await page.mouse.down();
  await page.mouse.move(detachedInput.x + detachedInput.width / 2, detachedInput.y + detachedInput.height / 2, { steps: 8 });
  await page.mouse.up();
  ir = JSON.parse(await page.locator('#ir').inputValue());
  const detachedEdge = ir.edges.find((edge) => edge.source === branch.id && edge.target === detached.id);
  expect(detachedEdge).toBeDefined();
  expect(detachedEdge.route).toBeUndefined();
  await page.locator('#inspector-edge-route').selectOption('approved');
  await page.locator('#apply-edge-route').click();
  expect(JSON.parse(await page.locator('#ir').inputValue()).edges.find((edge) => edge.id === detachedEdge.id).route).toBe('approved');

  await page.locator('#workflow-id').fill('branch');
  await page.locator('#load').click();
  await page.locator('.node[data-node-id="code"]').click();
  await page.locator('#available-input').selectOption('__additional');
  await page.locator('#inspector-content button:has-text("添加输入")').click();
  await page.locator('#inspector-inputs .input-name').fill('order');
  await page.locator('#inspector-inputs .input-type').selectOption('object');
  await page.locator('#inspector-inputs .input-source').selectOption('ref');
  await page.locator('#inspector-inputs .input-ref').selectOption('start::workflow_input');
  await page.locator('#inspector-content button:has-text("添加输出")').click();
  await page.locator('#inspector-outputs .inspector-output').last().locator('.output-name').fill('confidence');
  await page.locator('#inspector-outputs .inspector-output').last().locator('.output-type').selectOption('number');
  await page.locator('#apply-node').click();
  ir = JSON.parse(await page.locator('#ir').inputValue());
  expect(ir.nodes.find((node) => node.id === 'code').inputs[0]).toMatchObject({ name: 'order', source: 'ref', ref_node: 'start', ref_output: 'workflow_input' });
  expect(ir.nodes.find((node) => node.id === 'code').outputs).toContainEqual({ name: 'confidence', data_type: 'number' });
  await page.locator('.node[data-node-id="branch"]').click();
  await page.locator('#inspector-inputs .input-source').selectOption('ref');
  await page.locator('#inspector-inputs .input-ref').selectOption('code::score');
  await page.locator('#branch-settings button:has-text("添加条件")').click();
  await page.locator('#branch-cases .branch-case').last().locator('.case-route').fill('manual');
  await page.locator('#branch-cases .branch-case').last().locator('.case-operator').selectOption('eq');
  await page.locator('#branch-cases .branch-case').last().locator('.case-value').fill('85');
  await page.locator('#apply-node').click();
  ir = JSON.parse(await page.locator('#ir').inputValue());
  expect(ir.nodes.find((node) => node.id === 'branch').inputs.find((input) => input.name === 'value')).toMatchObject({ source: 'ref', ref_node: 'code', ref_output: 'score' });
  expect(ir.nodes.find((node) => node.id === 'branch').inputs.find((input) => input.name === 'cases').value).toContainEqual({ route: 'manual', operator: 'eq', value: 85 });

  const output = await page.locator('.node[data-node-id="branch"] .port-out').boundingBox();
  const input = await page.locator('.node[data-node-id="end"] .port-in').boundingBox();
  await page.mouse.move(output.x + output.width / 2, output.y + output.height / 2);
  await page.mouse.down();
  await page.mouse.move(input.x + input.width / 2, input.y + input.height / 2, { steps: 8 });
  await page.mouse.up();
  ir = JSON.parse(await page.locator('#ir').inputValue());
  const pending = ir.edges.find((edge) => edge.source === 'branch' && !edge.route);
  expect(pending).toBeDefined();
  await page.locator(`.edge-route-label[data-edge-id="${pending.id}"]`).click();
  await page.locator('#inspector-edge-route').selectOption('manual');
  await page.locator('#apply-edge-route').click();
  ir = JSON.parse(await page.locator('#ir').inputValue());
  expect(ir.edges.find((edge) => edge.id === pending.id).route).toBe('manual');
});
test.afterAll(async () => { await new Promise((resolve) => server.close(resolve)); });

test('v2 canvas creates workflow and binds named Start and End parameters', async ({ page }) => {
  const types = ['string', 'integer', 'number', 'boolean', 'object', 'array'];
  const v2Catalog = [
    { type: 'start', kind: 'control', allowed_inputs: [], outputs: { mode: 'author_declared', allowed_data_types: types, min_fields: 0, max_fields: 128 }, examples: [] },
    { type: 'end', kind: 'control', allowed_inputs: [], additional_inputs: { data_types: types, sources: ['literal', 'ref'] }, outputs: { mode: 'author_declared', allowed_data_types: types, min_fields: 0, max_fields: 128 }, examples: [] },
  ];
  const initial = { ir_version: '2', nodes: [
    { id: 'start', type: 'start', title: '开始', inputs: [], outputs: [] },
    { id: 'end', type: 'end', title: '结束', inputs: [], outputs: [] },
  ], edges: [{ id: 'flow', source: 'start', target: 'end' }], layout: { start: { x: 80, y: 120 }, end: { x: 420, y: 120 } } };
  await page.route('**/api/v1/projects/project/workflows', async (route) => {
    expect(route.request().postDataJSON().ir.ir_version).toBe('2');
    await route.fulfill({ json: { workflow: { workflow_id: 'created-v2' }, draft_revision: { revision_number: 1, ir: initial } } });
  });
  await page.route('**/api/v1/projects/project/workflows/created-v2/draft', async (route) => route.fulfill({ json: { current: { revision_number: 1, ir: initial } } }));
  await page.route('**/api/v1/node-types*', async (route) => route.fulfill({ json: { node_types: v2Catalog } }));
  await page.goto(url);
  await page.locator('#project-id').fill('project'); await page.locator('#token').fill('test-token');
  await page.locator('#create').click(); await page.locator('#create-name').fill('参数测试'); await page.locator('#create-confirm').click();
  await expect(page.locator('#workflow-id')).toHaveValue('created-v2');
  await page.locator('.node[data-node-id="start"]').click();
  await page.locator('#inspector-content button:has-text("添加输出")').click();
  await page.locator('.output-name').fill('amount'); await page.locator('.output-type').selectOption('integer');
  await page.locator('#apply-node').click();
  await expect(page.locator('#run-inputs textarea[data-name="amount"]')).toBeVisible();
  await page.locator('.node[data-node-id="end"]').click();
  await page.locator('#available-input').selectOption('__additional'); await page.locator('#inspector-content button:has-text("添加输入")').click();
  await page.locator('.input-name').fill('total'); await page.locator('.input-type').selectOption('integer');
  await page.locator('.input-source').selectOption('ref'); await page.locator('.input-ref').selectOption('start::amount');
  await page.locator('#apply-node').click();
  const ir = JSON.parse(await page.locator('#ir').inputValue());
  expect(ir.nodes.find((node) => node.type === 'end').outputs).toEqual([{ name: 'total', data_type: 'integer' }]);
  expect(ir.nodes.find((node) => node.type === 'end').inputs[0].ref_output).toBe('amount');
});

test('v2 Branch panel edits ordered OR conditions on the canvas', async ({ page }) => {
  const ir = { ir_version: '2', nodes: [
    { id: 'start', type: 'start', title: '开始', inputs: [], outputs: [{ name: 'score', data_type: 'integer' }] },
    { id: 'branch', type: 'branch', title: '判断', inputs: [
      { name: 'score', data_type: 'integer', source: 'ref', ref_node: 'start', ref_output: 'score' },
      { name: 'cases', data_type: 'array', source: 'literal', value: [{ route: 'pass', logic: 'and', conditions: [{ input: 'score', operator: 'gte', value: 60 }] }] },
      { name: 'default_route', data_type: 'string', source: 'literal', value: 'else' },
    ], outputs: [] },
    { id: 'end', type: 'end', title: '结束', inputs: [], outputs: [] },
  ], edges: [{ id: 'first', source: 'start', target: 'branch' }, { id: 'pass', source: 'branch', target: 'end', route: 'pass' }, { id: 'else', source: 'branch', target: 'end', route: 'else' }], layout: { start: { x: 20, y: 100 }, branch: { x: 250, y: 100 }, end: { x: 500, y: 100 } } };
  const types = ['string', 'integer', 'number', 'boolean', 'object', 'array'];
  await page.route('**/api/v1/projects/project/workflows/v2-branch/draft', async (route) => route.fulfill({ json: { current: { revision_number: 1, ir } } }));
  await page.route('**/api/v1/node-types*', async (route) => route.fulfill({ json: { node_types: [
    { type: 'start', kind: 'control', allowed_inputs: [], outputs: { mode: 'author_declared', allowed_data_types: types, min_fields: 0, max_fields: 128 }, examples: [] },
    { type: 'end', kind: 'control', allowed_inputs: [], additional_inputs: { data_types: types, sources: ['literal', 'ref'] }, outputs: { mode: 'author_declared', allowed_data_types: types, min_fields: 0, max_fields: 128 }, examples: [] },
    { type: 'branch', kind: 'control', allowed_inputs: [{ name: 'cases', required: true, data_types: ['array'], sources: ['literal'] }, { name: 'default_route', required: true, data_types: ['string'], sources: ['literal'] }], additional_inputs: { data_types: types, sources: ['literal', 'ref'] }, outputs: { mode: 'fixed', fields: [] }, examples: [] },
  ] } }));
  await page.goto(url); await page.locator('#project-id').fill('project'); await page.locator('#workflow-id').fill('v2-branch'); await page.locator('#token').fill('test-token'); await page.locator('#load').click();
  await page.locator('.node[data-node-id="branch"]').click();
  await page.locator('.case-logic').selectOption('or');
  await page.locator('.branch-case button:has-text("添加 AND/OR 判断")').click();
  const second = page.locator('.case-condition').last();
  await second.locator('.condition-input').selectOption('score'); await second.locator('.condition-operator').selectOption('eq'); await second.locator('.condition-value').fill('75');
  await page.locator('#apply-node').click();
  const edited = JSON.parse(await page.locator('#ir').inputValue());
  const cases = edited.nodes.find((node) => node.type === 'branch').inputs.find((input) => input.name === 'cases').value;
  expect(cases[0].logic).toBe('or'); expect(cases[0].conditions).toHaveLength(2); expect(cases[0].conditions[1].value).toBe(75);
});

test('v1 Draft upgrade is explicit and saves v2 against the same revision', async ({ page }) => {
  const legacy = { ir_version: '1', nodes: [
    { id: 'start', type: 'start', title: '开始', inputs: [], outputs: [{ name: 'workflow_input', data_type: 'object' }] },
    { id: 'end', type: 'end', title: '结束', inputs: [{ name: 'workflow_output', data_type: 'object', source: 'ref', ref_node: 'start', ref_output: 'workflow_input' }], outputs: [] },
  ], edges: [{ id: 'flow', source: 'start', target: 'end' }], layout: { start: { x: 80, y: 100 }, end: { x: 400, y: 100 } } };
  await page.route('**/api/v1/projects/project/workflows/upgrade/draft', async (route) => {
    if (route.request().method() === 'PUT') {
      const body = route.request().postDataJSON();
      expect(body.expected_revision).toBe(1);
      expect(body.ir.ir_version).toBe('2');
      await route.fulfill({ json: { revision_number: 2, ir: body.ir } });
    } else await route.fulfill({ json: { current: { revision_number: 1, ir: legacy } } });
  });
  await page.goto(url); await page.locator('#project-id').fill('project'); await page.locator('#workflow-id').fill('upgrade'); await page.locator('#token').fill('test-token'); await page.locator('#load').click();
  await expect(page.locator('#upgrade-ir')).toBeVisible();
  page.once('dialog', (dialog) => dialog.accept()); await page.locator('#upgrade-ir').click();
  const upgraded = JSON.parse(await page.locator('#ir').inputValue());
  expect(upgraded.ir_version).toBe('2');
  expect(upgraded.nodes.find((node) => node.type === 'end').outputs).toEqual([{ name: 'workflow_output', data_type: 'object' }]);
  await page.locator('#save').click();
  await expect(page.locator('#upgrade-ir')).toBeHidden();
  await expect(page.locator('#save-status')).toHaveText('保存成功 · Draft Revision 2');
});

test('save failure is visible beside the save button', async ({ page }) => {
  await page.route('**/api/v1/projects/project/workflows/failsave/draft', async (route) => {
    if (route.request().method() === 'PUT') await route.fulfill({ status: 500, json: { error: { code: 'SAVE_FAILED', message: 'database unavailable' } } });
    else await route.fulfill({ json: { current: { revision_number: 1, ir: base } } });
  });
  await page.goto(url);
  await page.locator('#project-id').fill('project'); await page.locator('#workflow-id').fill('failsave'); await page.locator('#token').fill('test-token');
  await page.locator('#load').click();
  await page.locator('#save').click();
  await expect(page.locator('#save-status')).toHaveText('保存失败：database unavailable');
  await expect(page.locator('#save')).toBeEnabled();
});

test('conflict preserves local edit, cancel keeps it, and explicit choice saves a new revision', async ({ page }) => {
  await page.goto(url);
  await page.locator('#project-id').fill('project');
  await page.locator('#workflow-id').fill('workflow');
  await page.locator('#token').fill('test-token');
  await page.locator('#load').click();
  await expect(page.locator('#ir')).toHaveValue(/Base/);
  await page.locator('.node[data-node-id="task"]').click();
  await page.locator('#node-title').fill('B');
  await page.locator('#apply-node').click();
  await page.locator('#save').click();
  await expect(page.locator('#merge-dialog')).toBeVisible();
  await expect(page.locator('#save-status')).toContainText('保存冲突');
  await expect(page.locator('#merge-canvas .merge-node[data-node-id="task"]')).toBeVisible();
  await page.locator('#merge-canvas .merge-node[data-node-id="task"]').click();
  await expect(page.locator('#merge-choices')).toContainText('已保存修改 · A');
  await expect(page.locator('#merge-choices')).toContainText('本地修改 · B');
  await expect(page.locator('#merge-choices')).not.toContainText('"title"');
  await page.locator('#merge-cancel').click();
  await expect(page.locator('#merge-dialog')).not.toBeVisible();
  await expect(page.locator('#ir')).toHaveValue(/"title": "B"/);
  await page.locator('#save').click();
  await page.locator('#merge-canvas .merge-node[data-node-id="task"]').click();
  await page.locator('#merge-choices .merge-option[data-origin="local"]').click();
  await page.locator('#merge-submit').click();
  await expect(page.locator('#merge-dialog')).not.toBeVisible();
  await expect(page.locator('#save-status')).toHaveText('保存成功 · Draft Revision 3');
  await expect(page.locator('#ir')).toHaveValue(/"title": "B"/);
  expect(saves.at(-1).expected_revision).toBe(2);
  expect(saves.at(-1).ir.nodes[0].title).toBe('B');
  expect(saves.at(-1).key).not.toBe(saves.at(-2).key);
});

test('edge conflict is chosen directly on the merge canvas', async ({ page }) => {
  await page.goto(url);
  await page.locator('#project-id').fill('project');
  await page.locator('#workflow-id').fill('edge-conflict');
  await page.locator('#token').fill('test-token');
  await page.locator('#load').click();
  const canvas = await page.locator('#canvas').boundingBox();
  await page.mouse.click(canvas.x + 425, canvas.y + 136, { button: 'right' });
  await page.locator('#context-delete').click();
  await page.locator('#save').click();
  await expect(page.locator('#merge-dialog')).toBeVisible();
  await page.locator('#merge-canvas .merge-edge[data-edge-id="path"]').click();
  await page.locator('#merge-choices .merge-option[data-origin="remove"]').click();
  await expect(page.locator('#merge-progress')).toContainText('1 / 1');
  await page.locator('#merge-submit').click();
  await expect(page.locator('#merge-dialog')).not.toBeVisible();
  expect(saves.at(-1).ir.edges).toHaveLength(0);
});

test('merge canvas node can be dragged before keeping a version', async ({ page }) => {
  await page.goto(url);
  await page.locator('#project-id').fill('project'); await page.locator('#workflow-id').fill('workflow'); await page.locator('#token').fill('test-token');
  await page.locator('#load').click();
  await page.locator('.node[data-node-id="task"]').click(); await page.locator('#node-title').fill('B'); await page.locator('#apply-node').click();
  await page.locator('#save').click(); await expect(page.locator('#merge-dialog')).toBeVisible();
  const item = page.locator('#merge-canvas .merge-node[data-node-id="task"]');
  const before = await item.boundingBox();
  await page.mouse.move(before.x + 50, before.y + 30); await page.mouse.down(); await page.mouse.move(before.x + 125, before.y + 75, { steps: 8 }); await page.mouse.up();
  await expect(page.locator('#merge-choices')).toContainText('已手动调整节点位置');
  await page.locator('#merge-choices .merge-option[data-origin="local"]').click();
  await page.locator('#merge-submit').click(); await expect(page.locator('#merge-dialog')).not.toBeVisible();
  expect(saves.at(-1).ir.layout.task.x).toBeGreaterThan(50);
});
