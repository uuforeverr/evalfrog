export function nodeByID(ir, id) { return (ir.nodes || []).find((node) => node.id === id); }

export function branchRoutes(node) {
  if (node?.type !== 'branch') return [];
  const cases = node.inputs?.find((input) => input.name === 'cases')?.value;
  const fallback = node.inputs?.find((input) => input.name === 'default_route')?.value;
  return [...new Set([...(Array.isArray(cases) ? cases.map((item) => item.route) : []), fallback].filter((route) => typeof route === 'string' && route))];
}

export function insertAfter(ir, sourceID, template, edgeID, newNodeID, firstEdgeID, secondEdgeID) {
  const source = nodeByID(ir, sourceID);
  if (!source) throw new Error('请先选择一个节点');
  if (source.type === 'end') throw new Error('结束节点后不能插入节点');
  if (!template || ['start', 'end'].includes(template.type)) throw new Error('请选择可插入的节点类型');
  const outgoing = (ir.edges || []).filter((edge) => edge.source === sourceID);
  if (outgoing.length > 1 && !edgeID) throw new Error('该节点有多条出边，请选择插入位置');
  const oldEdge = outgoing.find((edge) => edge.id === edgeID) || (outgoing.length === 1 ? outgoing[0] : null);
  if (edgeID && !oldEdge) throw new Error('所选连接已不存在');
  const result = structuredClone(ir);
  const fresh = structuredClone(template);
  fresh.id = newNodeID;
  result.nodes.push(fresh);
  result.layout ||= {};
  const from = result.layout[sourceID] || { x: 32, y: 32 };
  const to = oldEdge && result.layout[oldEdge.target];
  result.layout[newNodeID] = { x: to && to.x - from.x > 370 ? Math.round((from.x + to.x) / 2) : from.x + 220,
    y: to ? Math.round((from.y + to.y) / 2) : from.y };
  result.edges ||= [];
  if (oldEdge) result.edges = result.edges.filter((edge) => edge.id !== oldEdge.id);
  const first = { id: firstEdgeID, source: sourceID, target: newNodeID };
  if (oldEdge?.route) first.route = oldEdge.route;
  result.edges.push(first);
  if (oldEdge) {
    const second = { id: secondEdgeID, source: newNodeID, target: oldEdge.target };
    if (template.type === 'branch') {
      const fallback = template.inputs?.find((input) => input.name === 'default_route')?.value;
      if (typeof fallback === 'string' && fallback) second.route = fallback;
    }
    result.edges.push(second);
  }
  return result;
}

export function placeNode(ir, sourceID, template, newNodeID) {
  const source = nodeByID(ir, sourceID);
  if (!source) throw new Error('请先选择一个节点');
  if (source.type === 'end') throw new Error('结束节点后不能添加节点');
  if (!template || ['start', 'end'].includes(template.type)) throw new Error('请选择可添加的节点类型');
  const result = structuredClone(ir);
  const fresh = structuredClone(template); fresh.id = newNodeID;
  result.nodes.push(fresh); result.layout ||= {};
  const from = result.layout[sourceID] || { x: 32, y: 32 };
  const siblings = (result.nodes || []).filter((node) => node.id !== newNodeID && (result.layout[node.id]?.x || 0) >= from.x + 180).length;
  result.layout[newNodeID] = { x: from.x + 220, y: Math.max(0, from.y + 120 + siblings * 90) };
  return result;
}

export function nodeRemovalImpact(ir, nodeID) {
  if (!nodeByID(ir, nodeID)) throw new Error('请选择要删除的节点');
  return {
    edges: (ir.edges || []).filter((edge) => edge.source === nodeID || edge.target === nodeID).map((edge) => edge.id),
    inputs: (ir.nodes || []).flatMap((node) => (node.inputs || []).filter((input) => input.source === 'ref' && input.ref_node === nodeID).map((input) => `${node.id}.inputs.${input.name}`)),
  };
}

export function removeNode(ir, nodeID) {
  nodeRemovalImpact(ir, nodeID);
  const result = structuredClone(ir);
  result.nodes = result.nodes.filter((node) => node.id !== nodeID);
  result.edges = (result.edges || []).filter((edge) => edge.source !== nodeID && edge.target !== nodeID);
  for (const node of result.nodes) node.inputs = (node.inputs || []).filter((input) => input.source !== 'ref' || input.ref_node !== nodeID);
  if (result.layout) delete result.layout[nodeID];
  return result;
}

export function connect(ir, sourceID, targetID, edgeID, route) {
  const source = nodeByID(ir, sourceID), target = nodeByID(ir, targetID);
  if (!source || !target) throw new Error('请选择存在的起点和终点');
  if (sourceID === targetID) throw new Error('不能连接节点自身');
  if (source.type === 'end' || target.type === 'start') throw new Error('开始/结束节点的连接方向无效');
  if ((ir.edges || []).some((edge) => edge.source === sourceID && edge.target === targetID && (edge.route || '') === route)) throw new Error('该连接已存在');
  if (source.type === 'branch' && route && !/^[A-Za-z][A-Za-z0-9_]{0,63}$/.test(route)) throw new Error('Branch 出边需要合法 route');
  if (source.type !== 'branch' && route) throw new Error('只有 Branch 出边可以填写 route');
  const result = structuredClone(ir);
  result.edges ||= [];
  result.edges.push({ id: edgeID, source: sourceID, target: targetID, ...(route ? { route } : {}) });
  return result;
}

export function setEdgeRoute(ir, edgeID, route) {
  const edge = (ir.edges || []).find((item) => item.id === edgeID);
  if (!edge) throw new Error('请选择连接');
  const source = nodeByID(ir, edge.source);
  if (source?.type !== 'branch') throw new Error('只有 Branch 出边可以设置条件');
  if (route && !branchRoutes(source).includes(route)) throw new Error('请先在 Branch 节点中配置这个条件');
  const result = structuredClone(ir);
  const changed = result.edges.find((item) => item.id === edgeID);
  if (route) changed.route = route;
  else delete changed.route;
  return result;
}

export function disconnect(ir, edgeID) {
  if (!(ir.edges || []).some((edge) => edge.id === edgeID)) throw new Error('请选择要删除的连接');
  const result = structuredClone(ir);
  result.edges = result.edges.filter((edge) => edge.id !== edgeID);
  return result;
}
