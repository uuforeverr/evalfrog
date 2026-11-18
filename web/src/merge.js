const canonical = (value) => {
  if (Array.isArray(value)) return value.map(canonical);
  if (value && typeof value === 'object') return Object.fromEntries(Object.keys(value).sort().map((key) => [key, canonical(value[key])]));
  return value;
};
const equal = (a, b) => JSON.stringify(canonical(a)) === JSON.stringify(canonical(b));
const edgeConnection = (edge) => edge && { source: edge.source, target: edge.target, route: edge.route || null };

export function mergeChoices(base, latest, local) {
  const choices = [];
  const result = { ...latest, nodes: [], edges: [], layout: {} };
  if (latest.ir_version === base.ir_version && local.ir_version !== base.ir_version) result.ir_version = local.ir_version;
  else if (local.ir_version !== base.ir_version && latest.ir_version !== base.ir_version && latest.ir_version !== local.ir_version) throw new Error('双方升级到不同的 IR 版本，需先统一版本');
  for (const kind of ['nodes', 'edges', 'layout']) {
    const object = kind === 'layout';
    const index = (ir) => object ? (ir.layout || {}) : Object.fromEntries((ir[kind] || []).map((item) => [item.id, item]));
    const b = index(base), a = index(latest), l = index(local);
    for (const id of [...new Set([...Object.keys(b), ...Object.keys(a), ...Object.keys(l)])].sort()) {
      const versions = { base: b[id], latest: a[id], local: l[id] };
      const compare = kind === 'edges' ? edgeConnection : (value) => value;
      const aChanged = !equal(compare(b[id]), compare(a[id])), lChanged = !equal(compare(b[id]), compare(l[id]));
      if (kind === 'layout' && (aChanged !== lChanged || (aChanged && equal(a[id], l[id])))) {
        const position = aChanged ? a[id] : l[id];
        if (position !== undefined) result.layout[id] = position;
      } else if (aChanged || lChanged) choices.push({ kind, id, versions, aChanged, lChanged });
      else if (b[id] !== undefined) {
        if (object) result.layout[id] = b[id]; else result[kind].push(a[id] || l[id] || b[id]);
      }
    }
  }
  return { result, choices };
}

export function selectedIR(preview, selections, positions = {}) {
  const result = structuredClone(preview.result);
  for (const choice of preview.choices) {
    const selection = selections[`${choice.kind}:${choice.id}`];
    if (!selection) throw new Error(`请选择 ${choice.kind} ${choice.id} 的版本`);
    const value = selection === 'remove' ? undefined : selection === 'custom' && choice.kind === 'layout' ? positions[choice.id] : choice.versions[selection];
    if (value === undefined) continue;
    if (choice.kind === 'layout') result.layout[choice.id] = value;
    else result[choice.kind].push(value);
  }
  const ids = new Set(result.nodes.map((node) => node.id));
  for (const id of Object.keys(result.layout)) if (!ids.has(id)) delete result.layout[id];
  for (const [id, position] of Object.entries(positions)) if (ids.has(id)) result.layout[id] = position;
  for (const edge of result.edges) if (!ids.has(edge.source) || !ids.has(edge.target)) throw new Error(`边 ${edge.id} 的端点不存在，请重新选择`);
  for (const node of result.nodes) for (const input of node.inputs || []) {
    if (input.source === 'ref' && !ids.has(input.ref_node)) throw new Error(`节点 ${node.id} 引用了不存在的节点 ${input.ref_node}`);
  }
  return result;
}
