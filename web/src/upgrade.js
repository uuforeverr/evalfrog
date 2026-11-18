// Upgrade a validated v1 authoring shape without changing stable node or edge IDs.
// The caller must review the changed invocation and return object shapes before saving.
export function upgradeIRV1(ir) {
  if (ir?.ir_version !== '1') throw new Error('只有 IR v1 Draft 可以升级');
  const upgraded = structuredClone(ir);
  const start = upgraded.nodes.find((node) => node.type === 'start');
  const end = upgraded.nodes.find((node) => node.type === 'end');
  if (!start || !end || start.inputs?.length || start.outputs?.length !== 1 || start.outputs[0].name !== 'workflow_input' || start.outputs[0].data_type !== 'object') {
    throw new Error('请先补全 v1 Start 的 workflow_input 输出，再升级');
  }
  const endValue = end.inputs?.find((input) => input.name === 'workflow_output');
  if (!endValue || end.inputs.length !== 1 || endValue.data_type !== 'object' || end.outputs?.length) {
    throw new Error('请先补全 v1 End 的 workflow_output 输入，再升级');
  }
  end.outputs = [{ name: 'workflow_output', data_type: 'object' }];
  for (const node of upgraded.nodes.filter((item) => item.type === 'branch')) {
    const cases = node.inputs?.find((input) => input.name === 'cases');
    if (!Array.isArray(cases?.value) || !cases.value.length || !node.inputs.some((input) => input.name === 'value')) {
      throw new Error(`请先补全 Branch ${node.id} 的 value 和条件，再升级`);
    }
    cases.value = cases.value.map((item) => {
      if (!item.route || !item.operator || !Object.hasOwn(item, 'value')) throw new Error(`Branch ${node.id} 存在未填完的条件`);
      return { route: item.route, logic: 'and', conditions: [{ input: 'value', ...(item.path ? { path: item.path } : {}), operator: item.operator, value: item.value }] };
    });
  }
  upgraded.ir_version = '2';
  return upgraded;
}
