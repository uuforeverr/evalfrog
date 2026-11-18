import { mergeChoices, selectedIR } from './merge.js';
import { nodeByID, branchRoutes, insertAfter, placeNode, nodeRemovalImpact, removeNode, connect, disconnect, setEdgeRoute } from './graph-editor.js';
import { referenceOptions, updateNodeConfiguration } from './node-editor.js';
import { upgradeIRV1 } from './upgrade.js';
const state = { ir: null, revision: null, catalogRevision: null, run: null, stream: null, merge: null, selectedNode: null, selectedEdge: null, catalog: [], inputSchema: '' };
const $ = (id) => document.getElementById(id);
const apiBase = () => $('api-base').value.replace(/\/$/, '');
const project = () => $('project-id').value.trim();
const workflow = () => $('workflow-id').value.trim();
const auth = () => ({ Authorization: `Bearer ${$('token').value.trim()}` });
function descriptionFor(type) {
  return state.catalog.find((item) => item.type === type);
}

async function api(path, options = {}) {
  const response = await fetch(`${apiBase()}${path}`, { ...options, headers: { ...auth(), 'Content-Type': 'application/json', ...(options.headers || {}) } });
  if (!response.ok) { const body = await response.json().catch(() => ({})); throw body.error || { code: 'HTTP_ERROR', message: response.statusText }; }
  return response.json();
}

function parseIR() { state.ir = JSON.parse($('ir').value); return state.ir; }
function displayDiagnostics(value) { $('diagnostics').textContent = value ? JSON.stringify(value, null, 2) : ''; }
function saveStatus(message, kind = '') { const element = $('save-status'); element.textContent = message; element.dataset.kind = kind; }
function syncText() { $('ir').value = JSON.stringify(state.ir, null, 2); }

function draw() {
  $('upgrade-ir').hidden = state.ir?.ir_version !== '1';
  const nodes = $('nodes'); const edges = $('edges'); nodes.replaceChildren(); edges.replaceChildren();
  if (!state.ir) { refreshEditorControls(); return; }
  for (const edge of state.ir.edges || []) {
    const line = document.createElementNS('http://www.w3.org/2000/svg', 'line'); line.classList.add('edge');
    line.dataset.edgeId = edge.id; line.dataset.selected = String(state.selectedEdge === edge.id);
    line.dataset.pending = String(nodeByID(state.ir, edge.source)?.type === 'branch' && !edge.route);
    if (state.run?.failure_location?.logical_edge_id === edge.id) line.classList.add('edge-error');
    const hit = document.createElementNS('http://www.w3.org/2000/svg', 'line'); hit.classList.add('edge-hit'); hit.dataset.edgeId = edge.id;
    hit.addEventListener('click', () => selectEdge(edge.id));
    hit.addEventListener('contextmenu', (event) => { event.preventDefault(); selectEdge(edge.id); showContextMenu(event.clientX, event.clientY); });
    edges.append(line, hit);
    if (nodeByID(state.ir, edge.source)?.type === 'branch') {
      const label = document.createElementNS('http://www.w3.org/2000/svg', 'text');
      label.classList.add('edge-route-label'); label.dataset.edgeId = edge.id;
      label.dataset.pending = String(!edge.route);
      label.textContent = edge.route || '选择条件';
      label.addEventListener('click', () => selectEdge(edge.id));
      label.addEventListener('contextmenu', (event) => { event.preventDefault(); selectEdge(edge.id); showContextMenu(event.clientX, event.clientY); });
      edges.append(label);
    }
  }
  for (const node of state.ir.nodes || []) {
    const position = state.ir.layout?.[node.id] || { x: 32, y: 32 }; const element = document.createElement('article'); element.className = 'node'; element.dataset.nodeId = node.id; element.dataset.selected = String(state.selectedNode === node.id); element.style.left = `${position.x}px`; element.style.top = `${position.y}px`;
    const location = state.run?.failure_location?.logical_node_id === node.id ? state.run.failure_location : state.run?.nodes?.find((candidate) => candidate.location?.logical_node_id === node.id)?.location;
    const field = location?.ir_field ? `<small class="field-error" data-error-field="${escapeHTML(location.ir_field)}">${escapeHTML(location.ir_field)}</small>` : '';
    element.innerHTML = `<strong>${escapeHTML(node.title || node.id)}</strong><small>${escapeHTML(node.type)}</small>${field}`;
    element.dataset.error = String(Boolean(location));
    const inputPort = document.createElement('button'); inputPort.type = 'button'; inputPort.className = 'port port-in'; inputPort.dataset.nodeId = node.id; inputPort.title = '输入连接点'; inputPort.setAttribute('aria-label', `${node.title || node.id} 输入连接点`);
    inputPort.addEventListener('pointerdown', (event) => event.stopPropagation());
    const outputPort = document.createElement('button'); outputPort.type = 'button'; outputPort.className = 'port port-out'; outputPort.dataset.nodeId = node.id; outputPort.title = '从此处拖线连接'; outputPort.setAttribute('aria-label', `${node.title || node.id} 输出连接点`);
    outputPort.addEventListener('pointerdown', (event) => startConnectionDrag(event, node, outputPort));
    const add = document.createElement('button'); add.type = 'button'; add.className = 'node-add'; add.textContent = '+'; add.title = '在此节点后添加'; add.setAttribute('aria-label', `在 ${node.title || node.id} 后添加节点`);
    add.addEventListener('pointerdown', (event) => event.stopPropagation());
    add.addEventListener('click', (event) => { event.stopPropagation(); openNodePicker(node.id); });
    element.append(inputPort, outputPort, add);
    element.addEventListener('contextmenu', (event) => { event.preventDefault(); selectNode(node.id); showContextMenu(event.clientX, event.clientY); });
    makeDraggable(element, node); nodes.append(element);
  }
  updateEdgePositions(); refreshEditorControls(); renderRunInputs();
}

function renderRunInputs() {
  const start = state.ir?.nodes?.find((node) => node.type === 'start');
  const parameters = state.ir?.ir_version === '2' ? start?.outputs || [] : [{ name: 'workflow_input', data_type: 'object' }];
  const signature = JSON.stringify([state.ir?.ir_version, parameters]);
  if (signature === state.inputSchema) return;
  state.inputSchema = signature;
  const container = $('run-inputs'); container.replaceChildren();
  for (const parameter of parameters) {
    const textarea = document.createElement('textarea'); textarea.rows = 2; textarea.dataset.name = parameter.name;
    textarea.value = JSON.stringify(({ string: '', integer: 0, number: 0, boolean: false, object: {}, array: [] })[parameter.data_type]);
    container.append(field(`${parameter.name} (${parameter.data_type})`, textarea));
  }
}

function invocationInput() {
  const values = {};
  for (const textarea of $('run-inputs').querySelectorAll('textarea[data-name]')) {
    try { values[textarea.dataset.name] = JSON.parse(textarea.value); }
    catch { throw new Error(`调用参数 ${textarea.dataset.name} 不是有效 JSON`); }
  }
  return state.ir?.ir_version === '2' ? values : values.workflow_input || {};
}

function makeDraggable(element, node) {
  element.addEventListener('pointerdown', (event) => {
    if (event.button !== 0 || event.target.closest('.port, .node-add')) return;
    markNodeSelection(node.id);
    const startX = event.clientX, startY = event.clientY;
    const origin = state.ir.layout?.[node.id] || { x: 32, y: 32 };
    let moved = false;
    element.setPointerCapture(event.pointerId);
    const move = (next) => {
      if (next.pointerId !== event.pointerId) return;
      const x = Math.max(0, Math.round(origin.x + next.clientX - startX));
      const y = Math.max(0, Math.round(origin.y + next.clientY - startY));
      moved ||= x !== origin.x || y !== origin.y;
      state.ir.layout ||= {}; state.ir.layout[node.id] = { x, y };
      element.style.left = `${x}px`; element.style.top = `${y}px`;
      updateEdgePositions();
    };
    const finish = (next) => {
      if (next.pointerId !== event.pointerId) return;
      element.removeEventListener('pointermove', move);
      element.removeEventListener('pointerup', finish);
      element.removeEventListener('pointercancel', finish);
      if (moved) syncText();
    };
    element.addEventListener('pointermove', move);
    element.addEventListener('pointerup', finish);
    element.addEventListener('pointercancel', finish);
  });
}

function updateEdgePositions() {
  for (const line of $('edges').querySelectorAll('line[data-edge-id]')) {
    const edge = state.ir?.edges?.find((item) => item.id === line.dataset.edgeId);
    if (!edge) continue;
    const source = [...$('nodes').children].find((item) => item.dataset.nodeId === edge.source);
    const target = [...$('nodes').children].find((item) => item.dataset.nodeId === edge.target);
    if (!source || !target) continue;
    line.setAttribute('x1', source.offsetLeft + source.offsetWidth); line.setAttribute('y1', source.offsetTop + source.offsetHeight / 2);
    line.setAttribute('x2', target.offsetLeft); line.setAttribute('y2', target.offsetTop + target.offsetHeight / 2);
  }
  for (const label of $('edges').querySelectorAll('.edge-route-label')) {
    const line = $('edges').querySelector(`.edge[data-edge-id="${CSS.escape(label.dataset.edgeId)}"]`);
    if (!line) continue;
    label.setAttribute('x', (Number(line.getAttribute('x1')) + Number(line.getAttribute('x2'))) / 2);
    label.setAttribute('y', (Number(line.getAttribute('y1')) + Number(line.getAttribute('y2'))) / 2 - 8);
  }
}

function markNodeSelection(id) {
  state.selectedNode = id; state.selectedEdge = null;
  for (const item of $('nodes').children) item.dataset.selected = String(item.dataset.nodeId === id);
  for (const line of $('edges').querySelectorAll('.edge')) line.dataset.selected = 'false';
  refreshEditorControls();
}
function selectNode(id) { markNodeSelection(id); }
function canvasPoint(clientX, clientY) {
  const bounds = $('canvas').getBoundingClientRect();
  return { x: clientX - bounds.left + $('canvas').scrollLeft, y: clientY - bounds.top + $('canvas').scrollTop };
}
function startConnectionDrag(event, node, port) {
  if (event.button !== 0 || node.type === 'end') return;
  event.preventDefault(); event.stopPropagation(); markNodeSelection(node.id);
  const element = port.closest('.node');
  const startX = element.offsetLeft + element.offsetWidth, startY = element.offsetTop + element.offsetHeight / 2;
  const preview = document.createElementNS('http://www.w3.org/2000/svg', 'line'); preview.classList.add('edge-preview');
  preview.setAttribute('x1', startX); preview.setAttribute('y1', startY); preview.setAttribute('x2', startX); preview.setAttribute('y2', startY);
  $('edges').append(preview); port.dataset.dragging = 'true'; port.setPointerCapture(event.pointerId);
  const move = (next) => {
    if (next.pointerId !== event.pointerId) return;
    const point = canvasPoint(next.clientX, next.clientY);
    preview.setAttribute('x2', point.x); preview.setAttribute('y2', point.y);
  };
  const finish = (next) => {
    if (next.pointerId !== event.pointerId) return;
    const target = document.elementFromPoint(next.clientX, next.clientY)?.closest('.port-in');
    port.removeEventListener('pointermove', move); port.removeEventListener('pointerup', finish); port.removeEventListener('pointercancel', finish);
    delete port.dataset.dragging; preview.remove();
    if (next.type === 'pointercancel' || !target) return;
    try { const edgeID = newID('e'); applyEdit(connect(editorIR(), node.id, target.dataset.nodeId, edgeID, ''), null, edgeID); }
    catch (error) { displayDiagnostics(error.message); }
  };
  port.addEventListener('pointermove', move); port.addEventListener('pointerup', finish); port.addEventListener('pointercancel', finish);
}

function options(select, values, placeholder, preferred) {
  const previous = preferred ?? select.value;
  select.replaceChildren();
  select.add(new Option(placeholder, ''));
  for (const [value, label] of values) select.add(new Option(label, value));
  select.value = values.some(([value]) => value === previous) ? previous : '';
}
function field(labelText, control) {
  const label = document.createElement('label'); label.textContent = labelText; label.append(control); return label;
}
function makeInputRow(node, description, input) {
  const spec = description.allowed_inputs?.find((item) => item.name === input.name) || description.additional_inputs;
  const row = document.createElement('fieldset'); row.className = 'inspector-input';
  const name = document.createElement('input'); name.className = 'input-name'; name.value = input.name;
  name.readOnly = Boolean(description.allowed_inputs?.some((item) => item.name === input.name));
  row.append(field('输入名称', name));
  const type = document.createElement('select'); type.className = 'input-type';
  for (const candidate of spec?.data_types || [input.data_type]) type.add(new Option(candidate, candidate));
  type.value = input.data_type; row.append(field('类型', type));
  const source = document.createElement('select'); source.className = 'input-source';
  for (const candidate of spec?.sources || [input.source]) source.add(new Option(candidate === 'ref' ? '引用前序输出' : '直接填写', candidate));
  source.value = input.source; row.append(field('数据来源', source));
  const literal = document.createElement('textarea'); literal.className = 'input-literal'; literal.rows = 3;
  literal.value = input.source === 'literal' ? JSON.stringify(input.value, null, 2) : '';
  row.append(field('JSON 值', literal));
  const ref = document.createElement('select'); ref.className = 'input-ref'; row.append(field('前序输出', ref));
  const refreshReferences = () => {
    const selected = ref.value || (input.source === 'ref' ? `${input.ref_node}::${input.ref_output}` : '');
    options(ref, referenceOptions(state.ir, node.id, type.value).map((item) => [`${item.node}::${item.output}`, item.label]), '选择类型兼容的前序输出', selected);
    literal.closest('label').hidden = source.value !== 'literal'; ref.closest('label').hidden = source.value !== 'ref';
  };
  type.addEventListener('change', refreshReferences); source.addEventListener('change', refreshReferences); refreshReferences();
  if (!name.readOnly) {
    const remove = document.createElement('button'); remove.type = 'button'; remove.className = 'danger'; remove.textContent = '移除输入';
    remove.addEventListener('click', () => row.remove()); row.append(remove);
  } else if (!description.allowed_inputs?.find((item) => item.name === input.name)?.required) {
    const remove = document.createElement('button'); remove.type = 'button'; remove.className = 'danger'; remove.textContent = '移除可选输入';
    remove.addEventListener('click', () => row.remove()); row.append(remove);
  }
  return row;
}
function makeOutputRow(output, editable, types) {
  const row = document.createElement('fieldset'); row.className = 'inspector-output';
  const name = document.createElement('input'); name.className = 'output-name'; name.value = output.name; name.readOnly = !editable;
  row.append(field('输出名称', name));
  const type = document.createElement('select'); type.className = 'output-type';
  for (const candidate of types || [output.data_type]) type.add(new Option(candidate, candidate));
  type.value = output.data_type; type.disabled = !editable; row.append(field('类型', type));
  if (editable) { const remove = document.createElement('button'); remove.type = 'button'; remove.className = 'danger'; remove.textContent = '移除输出'; remove.addEventListener('click', () => row.remove()); row.append(remove); }
  return row;
}
function makeCaseRow(branchCase, operators) {
  const row = document.createElement('fieldset'); row.className = 'branch-case';
  const route = document.createElement('input'); route.className = 'case-route'; route.value = branchCase.route || ''; row.append(field('分支名称', route));
  const path = document.createElement('input'); path.className = 'case-path'; path.value = branchCase.path || ''; path.placeholder = '可选，如 status'; row.append(field('对象字段路径', path));
  const operator = document.createElement('select'); operator.className = 'case-operator';
  for (const item of [...new Set([...operators, branchCase.operator].filter(Boolean))]) operator.add(new Option(item, item));
  operator.value = branchCase.operator || ''; row.append(field('判断方式', operator));
  const value = document.createElement('textarea'); value.className = 'case-value'; value.rows = 2; value.value = branchCase.value === undefined ? '' : JSON.stringify(branchCase.value, null, 2); row.append(field('比较值（JSON）', value));
  const remove = document.createElement('button'); remove.type = 'button'; remove.className = 'danger'; remove.textContent = '移除条件'; remove.addEventListener('click', () => row.remove()); row.append(remove);
  return row;
}
function makeCaseRowV2(branchCase, operators, inputRows) {
  const row = document.createElement('fieldset'); row.className = 'branch-case';
  const route = document.createElement('input'); route.className = 'case-route'; route.value = branchCase.route || ''; row.append(field('IF / ELSE IF 分支名称', route));
  const logic = document.createElement('select'); logic.className = 'case-logic'; logic.add(new Option('全部满足（AND）', 'and')); logic.add(new Option('任一满足（OR）', 'or')); logic.value = branchCase.logic || 'and'; row.append(field('条件组合', logic));
  const conditions = document.createElement('div'); conditions.className = 'case-conditions'; row.append(conditions);
  const addCondition = () => {
    const condition = document.createElement('fieldset'); condition.className = 'case-condition';
    const name = document.createElement('select'); name.className = 'condition-input';
    const candidates = [...inputRows.children].map((item) => item.querySelector('.input-name')?.value.trim()).filter(Boolean);
    options(name, candidates.map((candidate) => [candidate, candidate]), '选择 Branch 输入', '');
    condition.append(field('判断参数', name));
    const path = document.createElement('input'); path.className = 'condition-path'; path.placeholder = '对象字段路径（可选）'; condition.append(field('字段路径', path));
    const operator = document.createElement('select'); operator.className = 'condition-operator';
    for (const value of operators) operator.add(new Option(value, value)); condition.append(field('判断方式', operator));
    const value = document.createElement('textarea'); value.className = 'condition-value'; value.rows = 2; condition.append(field('比较值（JSON）', value));
    const remove = document.createElement('button'); remove.type = 'button'; remove.className = 'danger'; remove.textContent = '移除判断'; remove.addEventListener('click', () => condition.remove()); condition.append(remove);
    conditions.append(condition); return { name, path, operator, value };
  };
  for (const item of branchCase.conditions || []) {
    const controls = addCondition(); controls.name.value = item.input || ''; controls.path.value = item.path || ''; controls.operator.value = item.operator || ''; controls.value.value = item.value === undefined ? '' : JSON.stringify(item.value, null, 2);
  }
  const add = document.createElement('button'); add.type = 'button'; add.textContent = '添加 AND/OR 判断'; add.addEventListener('click', addCondition); row.append(add);
  const remove = document.createElement('button'); remove.type = 'button'; remove.className = 'danger'; remove.textContent = '移除分支'; remove.addEventListener('click', () => row.remove()); row.append(remove);
  return row;
}
function renderInspector(node, edge) {
  const content = $('inspector-content'); content.replaceChildren(); $('inspector-error').textContent = '';
  if (!node && !edge) { content.textContent = '选择节点或连接后编辑'; return; }
  if (edge) {
    const source = nodeByID(state.ir, edge.source);
    const heading = document.createElement('strong'); heading.textContent = `${edge.source} → ${edge.target}`; content.append(heading);
    if (source?.type !== 'branch') { content.append('普通连接会在上游节点成功后激活。'); return; }
    const route = document.createElement('select'); route.id = 'inspector-edge-route';
    options(route, branchRoutes(source).map((item) => [item, item]), '选择分支条件', edge.route);
    content.append(field('此连接对应的条件', route));
    const help = document.createElement('p'); help.className = 'hint'; help.textContent = '条件在 Branch 节点中配置；未指定的连接可以保存 Draft，但无法通过校验或发布。'; content.append(help);
    const apply = document.createElement('button'); apply.type = 'button'; apply.id = 'apply-edge-route'; apply.textContent = '应用连接条件';
    apply.addEventListener('click', () => { try { applyEdit(setEdgeRoute(editorIR(), edge.id, route.value), null, edge.id); } catch (error) { $('inspector-error').textContent = error.message; } });
    content.append(apply); return;
  }
  const description = descriptionFor(node.type);
  if (!description) { content.textContent = '正在加载节点目录，加载后即可编辑配置。'; return; }
  const heading = document.createElement('strong'); heading.textContent = `${node.type} · ${node.id}`; content.append(heading);
  const title = document.createElement('input'); title.id = 'node-title'; title.value = node.title || ''; content.append(field('节点标题', title));
  const inputHeading = document.createElement('h3'); inputHeading.textContent = node.type === 'end' && state.ir?.ir_version === '2' ? '返回值（选择来源）' : '输入参数'; content.append(inputHeading);
  const inputRows = document.createElement('div'); inputRows.id = 'inspector-inputs'; content.append(inputRows);
  for (const input of node.inputs || []) if (node.type !== 'branch' || !['cases', 'default_route'].includes(input.name)) inputRows.append(makeInputRow(node, description, input));
  const available = document.createElement('select'); available.id = 'available-input';
  options(available, [
    ...(description.allowed_inputs || []).filter((item) => !item.required && !(node.inputs || []).some((input) => input.name === item.name)).map((item) => [item.name, item.name]),
    ...(description.additional_inputs ? [['__additional', '自定义输入']] : []),
  ], '添加输入');
  const addInput = document.createElement('button'); addInput.type = 'button'; addInput.textContent = '添加输入';
  addInput.addEventListener('click', () => {
    if (!available.value) return;
    const spec = description.allowed_inputs?.find((item) => item.name === available.value) || description.additional_inputs;
    const type = spec.data_types[0]; const value = ({ string: '', integer: 0, number: 0, boolean: false, object: {}, array: [] })[type];
    inputRows.append(makeInputRow(node, description, { name: available.value === '__additional' ? '' : available.value, data_type: type, source: spec.sources[0], value }));
    if (available.value !== '__additional') available.remove(available.selectedIndex);
  });
  if (node.type !== 'start' || state.ir?.ir_version !== '2') content.append(available, addInput);
  if (node.type === 'branch') {
    const section = document.createElement('section'); section.id = 'branch-settings';
    const heading = document.createElement('h3'); heading.textContent = '分支条件（从上到下，首个命中生效）'; section.append(heading);
    const help = document.createElement('p'); help.className = 'hint'; help.textContent = 'value 可引用前序输出；每个条件和默认分支都可连接到下游节点。'; section.append(help);
    const cases = node.inputs?.find((item) => item.name === 'cases')?.value || [];
    const rows = document.createElement('div'); rows.id = 'branch-cases'; section.append(rows);
    const operators = [...new Set(Object.values(description.allowed_inputs?.find((item) => item.name === 'cases')?.literal_constraints?.branch_cases?.operators || {}).flat())];
    if (state.ir?.ir_version === '2') operators.push(...['eq', 'neq', 'gt', 'gte', 'lt', 'lte', 'contains', 'not_contains', 'starts_with', 'ends_with', 'has_key', 'not_has_key'].filter((item) => !operators.includes(item)));
    for (const item of cases) rows.append(state.ir?.ir_version === '2' ? makeCaseRowV2(item, operators, inputRows) : makeCaseRow(item, operators));
    const add = document.createElement('button'); add.type = 'button'; add.textContent = state.ir?.ir_version === '2' ? '添加分支' : '添加条件'; add.addEventListener('click', () => rows.append(state.ir?.ir_version === '2' ? makeCaseRowV2({ route: '', logic: 'and', conditions: [] }, operators, inputRows) : makeCaseRow({ route: '', operator: operators[0] || 'eq', value: '' }, operators))); section.append(add);
    const fallback = document.createElement('input'); fallback.id = 'branch-default'; fallback.value = node.inputs?.find((item) => item.name === 'default_route')?.value || ''; section.append(field('默认分支名称', fallback));
    content.append(section);
  }
  const outputHeading = document.createElement('h3'); outputHeading.textContent = node.type === 'start' && state.ir?.ir_version === '2' ? '调用时输入的参数' : '输出参数'; content.append(outputHeading);
  const outputRows = document.createElement('div'); outputRows.id = 'inspector-outputs'; content.append(outputRows);
  const editable = description.outputs?.mode === 'author_declared';
  if (node.type !== 'end' || state.ir?.ir_version !== '2') for (const output of node.outputs || []) outputRows.append(makeOutputRow(output, editable, description.outputs?.allowed_data_types));
  if (node.type === 'end' && state.ir?.ir_version === '2') { outputHeading.textContent = '对外输出'; const help = document.createElement('p'); help.className = 'hint'; help.textContent = '返回值名称和类型由上方输入自动同步。'; content.append(help); }
  else if (editable) { const add = document.createElement('button'); add.type = 'button'; add.textContent = '添加输出'; add.addEventListener('click', () => outputRows.append(makeOutputRow({ name: '', data_type: description.outputs.allowed_data_types[0] }, true, description.outputs.allowed_data_types))); content.append(add); }
  else { const fixed = document.createElement('p'); fixed.className = 'hint'; fixed.textContent = '此节点的输出由节点目录固定。'; content.append(fixed); }
  const note = document.createElement('p'); note.className = 'hint'; note.textContent = '仅能引用类型兼容、在控制图上可到达的前序输出。条件分支汇合后的引用安全性由服务端校验。'; content.append(note);
  const apply = document.createElement('button'); apply.type = 'button'; apply.id = 'apply-node'; apply.textContent = '应用节点设置';
  apply.addEventListener('click', () => { try { applyNodeInspector(node.id, description); } catch (error) { $('inspector-error').textContent = error.message; } }); content.append(apply);
}
function applyNodeInspector(nodeID, description) {
  const inputs = [...$('inspector-inputs').children].map((row) => {
    const name = row.querySelector('.input-name').value.trim(), data_type = row.querySelector('.input-type').value, source = row.querySelector('.input-source').value;
    if (source === 'ref') { const [ref_node, ref_output] = row.querySelector('.input-ref').value.split('::'); return { name, data_type, source, ref_node, ref_output }; }
    return { name, data_type, source, value: JSON.parse(row.querySelector('.input-literal').value) };
  });
  if ($('branch-settings')) {
    const cases = [...$('branch-cases').children].map((row) => {
      const route = row.querySelector('.case-route').value.trim();
      if (state.ir?.ir_version === '2') return { route, logic: row.querySelector('.case-logic').value, conditions: [...row.querySelectorAll('.case-condition')].map((condition) => {
        const input = condition.querySelector('.condition-input').value, path = condition.querySelector('.condition-path').value.trim(), operator = condition.querySelector('.condition-operator').value, raw = condition.querySelector('.condition-value').value.trim();
        return { input, ...(path ? { path } : {}), operator, ...(raw ? { value: JSON.parse(raw) } : {}) };
      }) };
      const path = row.querySelector('.case-path').value.trim(), operator = row.querySelector('.case-operator').value;
      const raw = row.querySelector('.case-value').value.trim();
      return { route, ...(path ? { path } : {}), operator, ...(raw ? { value: JSON.parse(raw) } : {}) };
    });
    inputs.push({ name: 'cases', data_type: 'array', source: 'literal', value: cases });
    inputs.push({ name: 'default_route', data_type: 'string', source: 'literal', value: $('branch-default').value.trim() });
  }
  const outputs = description.type === 'end' && state.ir?.ir_version === '2' ? inputs.map(({ name, data_type }) => ({ name, data_type })) : [...$('inspector-outputs').children].map((row) => ({ name: row.querySelector('.output-name').value.trim(), data_type: row.querySelector('.output-type').value }));
  applyEdit(updateNodeConfiguration(editorIR(), nodeID, $('node-title').value, inputs, outputs, description), nodeID);
}
function refreshEditorControls() {
  const ir = state.ir;
  const node = ir && nodeByID(ir, state.selectedNode);
  const edge = ir?.edges?.find((item) => item.id === state.selectedEdge);
  $('selection').textContent = node ? `节点：${node.title || node.id} (${node.id})` : edge ? `连接：${edge.source} → ${edge.target} (${edge.id})` : '未选择';
  const outgoing = (ir?.edges || []).filter((item) => item.source === node?.id);
  options($('insert-edge'), outgoing.map((item) => [item.id, `${item.id}: ${item.source} → ${item.target}${item.route ? ` [${item.route}]` : ''}`]), outgoing.length ? '请选择出边' : '无出边：追加新连接', outgoing.length === 1 ? outgoing[0].id : undefined);
  const nodes = (ir?.nodes || []).map((item) => [item.id, `${item.title || item.id} (${item.id})`]);
  options($('edge-source'), nodes, '选择起点', node?.id || edge?.source);
  options($('edge-target'), nodes, '选择终点', edge?.target);
  const list = $('edge-list'); list.replaceChildren();
  for (const item of ir?.edges || []) {
    const button = document.createElement('button'); button.type = 'button'; button.dataset.edgeId = item.id; button.dataset.selected = String(item.id === state.selectedEdge);
    button.textContent = `${item.source} → ${item.target}${item.route ? ` [${item.route}]` : ''}`;
    button.addEventListener('click', () => selectEdge(item.id)); list.append(button);
  }
  renderInspector(node, edge);
}
function selectEdge(id) { state.selectedEdge = id; state.selectedNode = null; draw(); }
function openNodePicker(sourceID) {
  const source = nodeByID(state.ir, sourceID);
  if (!source || source.type === 'end') { displayDiagnostics('结束节点后不能添加节点'); return; }
  if (!state.catalog.length) { displayDiagnostics('节点目录尚未加载，请先加载 Draft'); return; }
  state.pickerSource = sourceID; markNodeSelection(sourceID);
  $('picker-source').textContent = `从 ${source.title || source.id} 后添加节点`;
  options($('picker-type'), state.catalog.filter((item) => !['start', 'end'].includes(item.type) && item.examples?.length).map((item) => [item.type, item.type]), '选择节点类型', $('insert-type').value);
  const outgoing = (state.ir.edges || []).filter((edge) => edge.source === sourceID);
  options($('picker-edge'), [...outgoing.map((edge) => [edge.id, `插入 ${edge.source} → ${edge.target}${edge.route ? ` [${edge.route}]` : ''}`]), ['__detached', '仅放置节点，稍后拉线']], '选择添加方式', outgoing.length === 1 ? outgoing[0].id : '__detached');
  $('picker-error').textContent = '';
  $('node-picker').showModal();
}
function confirmNodePicker() {
  try {
    const ir = editorIR();
    const template = descriptionFor($('picker-type').value)?.examples?.[0];
    const id = newID('n');
    const result = $('picker-edge').value === '__detached'
      ? placeNode(ir, state.pickerSource, template, id)
      : insertAfter(ir, state.pickerSource, template, $('picker-edge').value, id, newID('e'), newID('e'));
    applyEdit(result, id); $('node-picker').close();
  } catch (error) { $('picker-error').textContent = error.message; }
}
$('picker-confirm').addEventListener('click', confirmNodePicker);
$('picker-cancel').addEventListener('click', () => $('node-picker').close());
function showContextMenu(x, y) {
  const menu = $('context-menu'); menu.hidden = false;
  menu.style.left = `${Math.max(0, Math.min(x, window.innerWidth - 190))}px`;
  menu.style.top = `${Math.max(0, Math.min(y, window.innerHeight - 55))}px`;
  $('context-delete').textContent = state.selectedNode ? '删除节点' : '删除连接';
}
function hideContextMenu() { $('context-menu').hidden = true; }
$('context-delete').addEventListener('click', () => {
  hideContextMenu();
  try { if (state.selectedNode) deleteSelectedNode(); else if (state.selectedEdge) deleteSelectedEdge(); }
  catch (error) { displayDiagnostics(error.message); }
});
document.addEventListener('pointerdown', (event) => { if (!event.target.closest?.('#context-menu')) hideContextMenu(); });
window.addEventListener('keydown', (event) => {
  if (event.key === 'Escape') { hideContextMenu(); return; }
  if (!['Backspace', 'Delete'].includes(event.key) || $('merge-dialog').open || $('node-picker').open) return;
  if (event.target.closest?.('input, textarea, select, [contenteditable="true"]')) return;
  if (!state.selectedNode && !state.selectedEdge) return;
  event.preventDefault(); hideContextMenu();
  try { if (state.selectedNode) deleteSelectedNode(); else deleteSelectedEdge(); }
  catch (error) { displayDiagnostics(error.message); }
});
function applyEdit(ir, selectedNode = null, selectedEdge = null) {
  state.ir = ir; state.selectedNode = selectedNode; state.selectedEdge = selectedEdge;
  syncText(); draw(); displayDiagnostics(null);
}
function editorIR() { state.ir = JSON.parse($('ir').value); return state.ir; }
function newID(prefix) { return `${prefix}_${crypto.randomUUID()}`; }
function insertSelectedNode() {
  const ir = editorIR();
  const template = descriptionFor($('insert-type').value)?.examples?.[0];
  const id = newID('n');
  applyEdit(insertAfter(ir, state.selectedNode, template, $('insert-edge').value, id, newID('e'), newID('e')), id);
}
function deleteSelectedNode() {
  const ir = editorIR();
  const impact = nodeRemovalImpact(ir, state.selectedNode);
  const message = `删除节点 ${state.selectedNode}？\n将删除连接：${impact.edges.join(', ') || '无'}\n将删除数据引用：${impact.inputs.join(', ') || '无'}`;
  if (window.confirm(message)) applyEdit(removeNode(ir, state.selectedNode));
}
function addSelectedEdge() {
  const ir = editorIR();
  const edgeID = newID('e');
  applyEdit(connect(ir, $('edge-source').value, $('edge-target').value, edgeID, $('edge-route').value.trim()), null, edgeID);
}
function deleteSelectedEdge() { applyEdit(disconnect(editorIR(), state.selectedEdge)); }
for (const [id, action] of Object.entries({ 'insert-node': insertSelectedNode, 'delete-node': deleteSelectedNode, 'add-edge': addSelectedEdge, 'delete-edge': deleteSelectedEdge })) {
  $(id).addEventListener('click', () => { try { action(); } catch (error) { displayDiagnostics(error.message); } });
}
$('ir').addEventListener('change', () => { try { const previous = state.catalogRevision; state.ir = JSON.parse($('ir').value); state.catalogRevision = `catalog-v${state.ir.ir_version}`; draw(); if (previous !== state.catalogRevision) loadCatalog().catch(displayDiagnostics); else displayDiagnostics(null); } catch (error) { displayDiagnostics(error.message); } });

async function pull() { const draft = await api(`/v1/projects/${project()}/workflows/${workflow()}/draft`, { method: 'GET', headers: auth() }); state.ir = draft.current.ir; state.revision = draft.current.revision_number; state.catalogRevision = draft.current.catalog_revision || `catalog-v${state.ir.ir_version}`; state.selectedNode = null; state.selectedEdge = null; state.inputSchema = ''; syncText(); draw(); saveStatus(`已加载 Draft Revision ${state.revision}`); }
async function save() {
  if (state.revision == null) { saveStatus('请先加载或新建工作流', 'error'); return; }
  saveStatus('正在保存 Draft…', 'pending'); $('save').disabled = true;
  try { await saveIR(JSON.parse($('ir').value), state.revision); }
  catch (error) { saveStatus(`保存失败：${error.message || error.code || String(error)}`, 'error'); throw error; }
  finally { $('save').disabled = false; }
}
async function saveIR(ir, expectedRevision) {
  try {
    const saved = await api(`/v1/projects/${project()}/workflows/${workflow()}/draft`, { method: 'PUT', headers: { ...auth(), 'Idempotency-Key': crypto.randomUUID() }, body: JSON.stringify({ expected_revision: expectedRevision, ir }) });
    const previous = state.catalogRevision;
    state.revision = saved.revision_number; state.ir = saved.ir; state.catalogRevision = saved.catalog_revision || `catalog-v${state.ir.ir_version}`; syncText(); draw(); state.merge = null; $('merge-dialog').close();
    if (previous !== state.catalogRevision) await loadCatalog();
    saveStatus(`保存成功 · Draft Revision ${state.revision}`, 'success');
  } catch (error) {
    if (error.code === 'DRAFT_REVISION_CONFLICT' && error.details?.base_ir && error.details?.current_ir) { saveStatus('保存冲突：请在合并画布中选择版本', 'pending'); showMerge(error.details); return; }
    throw error;
  }
}
function showMerge(details) {
  const preview = mergeChoices(details.base_ir, details.current_ir, details.submitted_ir);
  state.merge = { preview, revision: details.current_revision, base: details.base_ir, latest: details.current_ir, local: details.submitted_ir, selections: {}, positions: {} };
  $('merge-revisions').textContent = `基线 Revision ${details.base_revision} → 当前 Revision ${details.current_revision}。蓝色是已保存修改，橙色是本地修改；点击节点或连线选择，拖动节点调整最终位置。`;
  $('merge-error').textContent = '';
  $('merge-choices').textContent = '点击画布中的元素，选择保留哪一版。';
  renderMergeCanvas();
  if (!$('merge-dialog').open) $('merge-dialog').showModal();
}
function mergeChoice(kind, id) { return state.merge?.preview.choices.find((choice) => choice.kind === kind && choice.id === id); }
function mergeVariant(kind, id, origin) {
  const ir = state.merge[origin];
  if (!ir) return undefined;
  return kind === 'layout' ? ir.layout?.[id] : ir[kind]?.find((item) => item.id === id);
}
function chosenMergeOrigin(kind, id) { return state.merge.selections[`${kind}:${id}`] || 'latest'; }
function mergePosition(id, index) {
  if (state.merge.positions[id]) return state.merge.positions[id];
  const selected = chosenMergeOrigin('layout', id);
  const position = mergeVariant('layout', id, selected) || mergeVariant('layout', id, 'latest') || mergeVariant('layout', id, 'local') || mergeVariant('layout', id, 'base');
  return position ? { x: Math.max(24, position.x), y: Math.max(24, position.y) } : { x: 50 + index * 210, y: 90 };
}
function renderMergeCanvas() {
  const merge = state.merge; if (!merge) return;
  const canvas = $('merge-canvas'); canvas.replaceChildren();
  const nodeIDs = [...new Set(['base', 'latest', 'local'].flatMap((origin) => merge[origin].nodes.map((node) => node.id)))];
  const edgeIDs = [...new Set(['base', 'latest', 'local'].flatMap((origin) => merge[origin].edges.map((edge) => edge.id)))];
  const positions = Object.fromEntries(nodeIDs.map((id, index) => [id, mergePosition(id, index)]));
  const width = Math.max(900, ...Object.values(positions).map((position) => position.x + 220));
  const height = Math.max(460, ...Object.values(positions).map((position) => position.y + 150));
  canvas.style.width = `${width}px`; canvas.style.height = `${height}px`;
  const svg = document.createElementNS('http://www.w3.org/2000/svg', 'svg'); svg.setAttribute('width', width); svg.setAttribute('height', height); svg.classList.add('merge-lines'); canvas.append(svg);
  for (const id of edgeIDs) {
    const choice = mergeChoice('edges', id);
    const origin = chosenMergeOrigin('edges', id);
    const edge = mergeVariant('edges', id, origin) || mergeVariant('edges', id, 'latest') || mergeVariant('edges', id, 'local') || mergeVariant('edges', id, 'base');
    if (!edge || !positions[edge.source] || !positions[edge.target]) continue;
    const x1 = positions[edge.source].x + 170, y1 = positions[edge.source].y + 42;
    const x2 = positions[edge.target].x, y2 = positions[edge.target].y + 42;
    const line = document.createElementNS('http://www.w3.org/2000/svg', 'line'); line.setAttribute('x1', x1); line.setAttribute('y1', y1); line.setAttribute('x2', x2); line.setAttribute('y2', y2);
    line.classList.add('merge-line'); line.dataset.edgeId = id; if (choice) line.dataset.changed = choice.aChanged && choice.lChanged ? 'both' : choice.aChanged ? 'latest' : 'local';
    if (merge.selections[`edges:${id}`] === 'remove') line.dataset.removed = 'true'; svg.append(line);
    const button = document.createElement('button'); button.type = 'button'; button.className = 'merge-edge'; button.dataset.edgeId = id;
    button.style.left = `${(x1 + x2) / 2}px`; button.style.top = `${(y1 + y2) / 2}px`;
    button.dataset.changed = line.dataset.changed || 'none'; button.dataset.resolved = String(Boolean(merge.selections[`edges:${id}`]));
    button.textContent = edge.route ? `→ ${edge.route}` : '→'; button.title = `${edge.source} → ${edge.target}`;
    button.addEventListener('click', () => inspectMergeChoice('edges', id)); canvas.append(button);
  }
  for (const id of nodeIDs) {
    const choice = mergeChoice('nodes', id) || mergeChoice('layout', id);
    const origin = chosenMergeOrigin('nodes', id);
    const node = mergeVariant('nodes', id, origin) || mergeVariant('nodes', id, 'latest') || mergeVariant('nodes', id, 'local') || mergeVariant('nodes', id, 'base');
    const button = document.createElement('button'); button.type = 'button'; button.className = 'merge-node'; button.dataset.nodeId = id;
    button.style.left = `${positions[id].x}px`; button.style.top = `${positions[id].y}px`;
    button.dataset.changed = choice ? choice.aChanged && choice.lChanged ? 'both' : choice.aChanged ? 'latest' : 'local' : 'none';
    button.dataset.resolved = String(Boolean(merge.selections[`nodes:${id}`] || merge.selections[`layout:${id}`]));
    button.dataset.removed = String(merge.selections[`nodes:${id}`] === 'remove');
    const title = document.createElement('strong'); title.textContent = node?.title || id; button.append(title);
    const subtitle = document.createElement('small'); subtitle.textContent = node?.type || '已删除'; button.append(subtitle);
    if (choice) { const badge = document.createElement('small'); badge.textContent = button.dataset.removed === 'true' ? '选择删除' : button.dataset.resolved === 'true' ? '已选择' : '待选择'; button.append(badge); }
    button.addEventListener('click', () => { if (button.dataset.dragged === 'true') { button.dataset.dragged = 'false'; return; } inspectMergeChoice('nodes', id); });
    enableMergeDrag(button, id, positions[id]); canvas.append(button);
  }
  const resolved = merge.preview.choices.filter((choice) => merge.selections[`${choice.kind}:${choice.id}`]).length;
  $('merge-progress').textContent = `已选择 ${resolved} / ${merge.preview.choices.length} 项`;
}
function updateMergeConnections() {
  const canvas = $('merge-canvas');
  for (const line of canvas.querySelectorAll('.merge-line')) {
    const edge = mergeVariant('edges', line.dataset.edgeId, chosenMergeOrigin('edges', line.dataset.edgeId)) || mergeVariant('edges', line.dataset.edgeId, 'latest') || mergeVariant('edges', line.dataset.edgeId, 'local') || mergeVariant('edges', line.dataset.edgeId, 'base');
    const source = [...canvas.querySelectorAll('.merge-node')].find((node) => node.dataset.nodeId === edge?.source);
    const target = [...canvas.querySelectorAll('.merge-node')].find((node) => node.dataset.nodeId === edge?.target);
    if (!source || !target) continue;
    const x1 = source.offsetLeft + source.offsetWidth, y1 = source.offsetTop + 42, x2 = target.offsetLeft, y2 = target.offsetTop + 42;
    line.setAttribute('x1', x1); line.setAttribute('y1', y1); line.setAttribute('x2', x2); line.setAttribute('y2', y2);
    const label = [...canvas.querySelectorAll('.merge-edge')].find((item) => item.dataset.edgeId === line.dataset.edgeId);
    if (label) { label.style.left = `${(x1 + x2) / 2}px`; label.style.top = `${(y1 + y2) / 2}px`; }
  }
}
function enableMergeDrag(button, id, origin) {
  button.addEventListener('pointerdown', (event) => {
    if (event.button !== 0) return;
    const startX = event.clientX, startY = event.clientY; let moved = false;
    button.setPointerCapture(event.pointerId);
    const move = (next) => {
      if (next.pointerId !== event.pointerId) return;
      if (Math.abs(next.clientX - startX) + Math.abs(next.clientY - startY) > 3) moved = true;
      if (!moved) return;
      button.style.left = `${Math.max(24, Math.round(origin.x + next.clientX - startX))}px`;
      button.style.top = `${Math.max(24, Math.round(origin.y + next.clientY - startY))}px`;
      updateMergeConnections();
    };
    const finish = () => {
      button.removeEventListener('pointermove', move); button.removeEventListener('pointerup', finish); button.removeEventListener('pointercancel', finish);
      if (!moved) return;
      button.dataset.dragged = 'true';
      state.merge.positions[id] = { x: button.offsetLeft, y: button.offsetTop };
      if (mergeChoice('layout', id)) state.merge.selections[`layout:${id}`] = 'custom';
      renderMergeCanvas(); inspectMergeChoice('nodes', id);
    };
    button.addEventListener('pointermove', move); button.addEventListener('pointerup', finish); button.addEventListener('pointercancel', finish);
  });
}
function inspectMergeChoice(kind, id) {
  const nodeChoice = kind === 'nodes' && mergeChoice('nodes', id);
  const choices = kind === 'nodes' ? [nodeChoice, mergeChoice('layout', id)].filter(Boolean) : [mergeChoice('edges', id)].filter(Boolean);
  const panel = $('merge-choices'); panel.replaceChildren();
  const title = document.createElement('h3'); title.textContent = kind === 'nodes' ? `节点 ${id}` : `连线 ${id}`; panel.append(title);
  if (kind === 'nodes' && state.merge.positions[id]) { const note = document.createElement('p'); note.textContent = '已手动调整节点位置；保存选择时会使用此位置。'; panel.append(note); }
  if (!choices.length) { panel.append('双方均未修改，已保留。'); return; }
  const descriptions = { base: '保留基线', latest: '保留已保存修改', local: '保留本地修改', remove: '删除' };
  for (const origin of ['base', 'latest', 'local', 'remove']) {
    if (origin === 'remove' && kind === 'nodes' && !nodeChoice) continue;
    if (origin !== 'remove' && choices.some((choice) => choice.versions[origin] === undefined)) continue;
    const button = document.createElement('button'); button.type = 'button'; button.className = 'merge-option'; button.dataset.origin = origin;
    const variant = kind === 'nodes' ? mergeVariant('nodes', id, origin) : mergeVariant('edges', id, origin);
    button.textContent = `${descriptions[origin]}${variant ? ` · ${kind === 'nodes' ? variant.title || variant.id : `${variant.source} → ${variant.target}${variant.route ? ` [${variant.route}]` : ''}`}` : ''}`;
    if (choices.every((choice) => state.merge.selections[`${choice.kind}:${choice.id}`] === origin)) button.dataset.selected = 'true';
    button.addEventListener('click', () => {
      for (const choice of choices) state.merge.selections[`${choice.kind}:${choice.id}`] = origin;
      renderMergeCanvas(); inspectMergeChoice(kind, id);
    }); panel.append(button);
  }
}
async function submitMerge() {
  if (!state.merge) return;
  try {
    const ir = selectedIR(state.merge.preview, state.merge.selections, state.merge.positions);
    $('ir').value = JSON.stringify(ir, null, 2);
    await saveIR(ir, state.merge.revision);
  } catch (error) { $('merge-error').textContent = error.message || String(error); }
}
$('merge-submit').addEventListener('click', submitMerge);
$('merge-cancel').addEventListener('click', () => $('merge-dialog').close());
async function validate() { const result = await api(`/v1/projects/${project()}/workflows/${workflow()}/draft/validate`, { method: 'POST', body: JSON.stringify({ revision: state.revision }) }); displayDiagnostics(result.diagnostics); }
async function test() { const deadline = new Date(Date.now() + 5 * 60_000).toISOString(); const run = await api(`/v1/projects/${project()}/workflows/${workflow()}/draft/test`, { method: 'POST', headers: { ...auth(), 'Idempotency-Key': crypto.randomUUID() }, body: JSON.stringify({ revision: state.revision, input: invocationInput(), deadline_at: deadline }) }); await loadRun(run.run_id); }
async function publish() { const result = await api(`/v1/projects/${project()}/workflows/${workflow()}/publish`, { method: 'POST', headers: { ...auth(), 'Idempotency-Key': crypto.randomUUID() }, body: JSON.stringify({ expected_revision: state.revision, change_log: 'Published from Human Web Canvas' }) }); displayDiagnostics(`Published version ${result.version.version_number}`); }
async function run() { const deadline = new Date(Date.now() + 5 * 60_000).toISOString(); const result = await api(`/v1/projects/${project()}/workflows/${workflow()}/runs`, { method: 'POST', headers: { ...auth(), 'Idempotency-Key': crypto.randomUUID() }, body: JSON.stringify({ input: invocationInput(), deadline_at: deadline }) }); await loadRun(result.run_id); }
async function cancel() { if (!state.run?.run_id) throw new Error('请先选择一个运行实例'); const result = await api(`/v1/projects/${project()}/runs/${state.run.run_id}/cancel`, { method: 'POST', headers: auth() }); displayDiagnostics(result); await loadRun(state.run.run_id); }
async function refreshRun(runID) { state.run = await api(`/v1/projects/${project()}/runs/${runID}`, { method: 'GET', headers: auth() }); $('run-state').textContent = `${state.run.state} (v${state.run.state_version})`; $('run-error').textContent = JSON.stringify(state.run.failure_location || {}, null, 2); draw(); }
async function loadRun(runID) { await refreshRun(runID); subscribe(runID); }
function subscribe(runID) {
  state.stream?.abort();
  if (['succeeded', 'failed', 'canceled', 'timed_out'].includes(state.run?.state)) return;
  const controller = new AbortController(); state.stream = controller;
  readRunEvents(runID, controller.signal).catch(() => {
    if (!controller.signal.aborted) setTimeout(() => loadRun(runID).catch(() => {}), 1500);
  });
}
async function readRunEvents(runID, signal) {
  const response = await fetch(`${apiBase()}/v1/projects/${project()}/runs/${runID}/events`, { headers: { ...auth(), Accept: 'text/event-stream' }, signal });
  if (!response.ok || !response.body) throw new Error('run event stream is unavailable');
  const reader = response.body.getReader(); const decoder = new TextDecoder(); let buffer = '';
  while (!signal.aborted) {
    const { value, done } = await reader.read(); if (done) break; buffer += decoder.decode(value, { stream: true });
    const frames = buffer.split('\n\n'); buffer = frames.pop();
    for (const frame of frames) if (frame.includes('event: ready') || frame.includes('event: update')) await refreshRun(runID);
  }
  if (!signal.aborted && !['succeeded', 'failed', 'canceled', 'timed_out'].includes(state.run?.state)) throw new Error('run event stream ended');
}
function escapeHTML(value) { return String(value).replace(/[&<>"']/g, (char) => ({ '&':'&amp;', '<':'&lt;', '>':'&gt;', '"':'&quot;', "'":'&#039;' })[char]); }

for (const [id, action] of Object.entries({ load: refreshAuthoringContext, save, validate, test, publish, run, cancel })) $(id).addEventListener('click', () => action().catch(displayDiagnostics));
$('create').addEventListener('click', () => { $('create-error').textContent = ''; $('create-dialog').showModal(); });
$('create-cancel').addEventListener('click', () => $('create-dialog').close());
$('upgrade-ir').addEventListener('click', async () => {
  try {
    const upgraded = upgradeIRV1(editorIR());
    if (!window.confirm('升级后调用输入需包装为 {"workflow_input": 原输入}，返回值变为 {"workflow_output": 原输出}。当前只修改本地 Draft；确认后请先校验，再保存和发布。')) return;
    applyEdit(upgraded); state.catalogRevision = 'catalog-v2'; await loadCatalog();
  } catch (error) { displayDiagnostics(error.message); }
});
$('create-confirm').addEventListener('click', async () => {
  try {
    const name = $('create-name').value.trim();
    if (!name || !project() || !$('token').value.trim()) throw new Error('请填写工作流名称、Project ID 和 Token');
    const ir = { ir_version: '2', nodes: [
      { id: 'start', type: 'start', title: '开始', inputs: [], outputs: [] },
      { id: 'end', type: 'end', title: '结束', inputs: [], outputs: [] },
    ], edges: [{ id: 'flow', source: 'start', target: 'end' }], layout: { start: { x: 80, y: 120 }, end: { x: 420, y: 120 } } };
    const result = await api(`/v1/projects/${project()}/workflows`, { method: 'POST', headers: { 'Idempotency-Key': crypto.randomUUID() }, body: JSON.stringify({ name, ir }) });
    $('workflow-id').value = result.workflow.workflow_id;
    $('create-dialog').close();
    await refreshAuthoringContext();
  } catch (error) { $('create-error').textContent = error.message || String(error); }
});
function refreshAuthoringContext() {
  if (!project() || !workflow() || !$('token').value.trim() || !apiBase()) return Promise.resolve();
  return pull().then(loadCatalog);
}
for (const id of ['workflow-id', 'project-id', 'token', 'api-base']) $(id).addEventListener('change', () => refreshAuthoringContext().catch(displayDiagnostics));
function addCatalogNode(description) {
  $('insert-type').value = description.type;
  displayDiagnostics(`已选择 ${description.type}。请点击画布节点，再点击“插入节点”。`);
}
async function loadCatalog() { const revision = state.catalogRevision || `catalog-v${state.ir?.ir_version || '2'}`; const { node_types } = await api(`/v1/node-types?catalog_revision=${encodeURIComponent(revision)}`, { method: 'GET', headers: auth() }); state.catalog = node_types; options($('insert-type'), node_types.filter((node) => !['start', 'end'].includes(node.type) && node.examples?.length).map((node) => [node.type, node.type]), '选择节点类型'); $('catalog').replaceChildren(...node_types.map((node) => { const button = document.createElement('button'); button.className = 'catalog-item'; button.textContent = `${node.type} · ${node.kind}`; button.title = node.natural_language_description; button.addEventListener('click', () => addCatalogNode(node)); return button; })); refreshEditorControls(); }
