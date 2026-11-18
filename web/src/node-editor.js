import { nodeByID } from './graph-editor.js';

const namePattern = /^[A-Za-z][A-Za-z0-9_]{0,63}$/;
const dataTypes = new Set(['string', 'integer', 'number', 'boolean', 'object', 'array']);

function reaches(ir, source, target) {
  const seen = new Set([source]);
  const pending = [source];
  while (pending.length) {
    const current = pending.shift();
    for (const edge of ir.edges || []) {
      if (edge.source !== current) continue;
      if (edge.target === target) return true;
      if (!seen.has(edge.target)) { seen.add(edge.target); pending.push(edge.target); }
    }
  }
  return false;
}

export function referenceOptions(ir, targetID, inputType) {
  return (ir.nodes || []).filter((node) => node.id !== targetID && reaches(ir, node.id, targetID))
    .flatMap((node) => (node.outputs || []).filter((output) => !inputType || output.data_type === inputType || (inputType === 'number' && output.data_type === 'integer'))
      .map((output) => ({ node: node.id, output: output.name, dataType: output.data_type, label: `${node.title || node.id}.${output.name} (${output.data_type})` })));
}

function literalMatches(type, value) {
  if (type === 'integer') return Number.isSafeInteger(value);
  if (type === 'number') return typeof value === 'number' && Number.isFinite(value);
  if (type === 'array') return Array.isArray(value);
  if (type === 'object') return value !== null && typeof value === 'object' && !Array.isArray(value);
  return typeof value === type;
}

export function updateNodeConfiguration(ir, nodeID, title, inputs, outputs, description) {
  if (!nodeByID(ir, nodeID)) throw new Error('节点已不存在');
  if (!title.trim()) throw new Error('节点标题不能为空');
  const allowedInputs = new Map((description?.allowed_inputs || []).map((input) => [input.name, input]));
  const names = new Set();
  for (const input of inputs) {
    if (!namePattern.test(input.name) || names.has(input.name)) throw new Error(`输入名称无效或重复：${input.name}`);
    names.add(input.name);
    const contract = allowedInputs.get(input.name) || description?.additional_inputs;
    if (!contract) throw new Error(`节点不支持输入 ${input.name}`);
    if (!contract.data_types?.includes(input.data_type) || !contract.sources?.includes(input.source)) throw new Error(`输入 ${input.name} 的类型或来源不受支持`);
    if (input.source === 'ref') {
      const valid = referenceOptions(ir, nodeID, input.data_type).some((item) => item.node === input.ref_node && item.output === input.ref_output);
      if (!valid) throw new Error(`输入 ${input.name} 需要选择类型兼容、可到达的前序输出`);
    } else if (!literalMatches(input.data_type, input.value)) throw new Error(`输入 ${input.name} 的 JSON 值与类型不符`);
  }
  for (const contract of description?.allowed_inputs || []) if (contract.required && !names.has(contract.name)) throw new Error(`缺少必填输入 ${contract.name}`);
  const outputNames = new Set();
  for (const output of outputs) {
    if (!namePattern.test(output.name) || outputNames.has(output.name) || !dataTypes.has(output.data_type)) throw new Error(`输出名称或类型无效：${output.name}`);
    outputNames.add(output.name);
  }
  if (description?.outputs?.mode === 'fixed' && JSON.stringify(outputs) !== JSON.stringify((description.outputs.fields || []).map(({ name, data_type }) => ({ name, data_type })))) throw new Error('此节点的输出由目录固定');
  if (description?.outputs?.mode === 'author_declared') {
    if (outputs.length < description.outputs.min_fields || outputs.length > description.outputs.max_fields || outputs.some((output) => !description.outputs.allowed_data_types.includes(output.data_type))) throw new Error('输出声明不符合节点目录约束');
  }
  const result = structuredClone(ir);
  const node = nodeByID(result, nodeID);
  node.title = title.trim(); node.inputs = inputs; node.outputs = outputs;
  return result;
}
