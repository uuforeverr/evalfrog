# Control Graph 语义基线

> 状态：Accepted Baseline
>
> 日期：2026-08-07
>
> 范围：第一阶段 Start、End、Branch、并行路径、汇合与分支输出边界
>
> 前置基线：[00_核心架构决策基线.md](./00_核心架构决策基线.md)、[03_IR编辑态结构基线.md](./03_IR编辑态结构基线.md)

## 1. 目标

第一阶段只定义能够形成可靠核心闭环的最小 Control Graph，不引入循环、Join Node、Optional Reference 或隐式分支数据合并。

控制流和数据流保持分离：

```text
Edge                         → 控制路径是否激活
Input(source = ref)          → 节点需要哪个上游 Output
Compiler 生成的 Dependency  → Runtime 实际等待关系
```

## 2. 图结构不变量

第一阶段 Workflow Control Graph 是单入口、单出口的 DAG：

- 恰好一个 Start；
- 恰好一个 End；
- Start 没有入边；
- End 没有出边；
- 每个其他 Node 至少具有一条入边和一条出边；
- 所有 Node 都必须从 Start 可达；
- 所有 Node 都必须能够到达 End；
- Control Edge 不得形成环；
- 不允许与主图隔离的孤立 Node 或子图。

以上条件均在 Draft Test 和 Publish 的结构校验阶段完成，不能延迟到 Runtime。

## 3. Start 与普通节点的出边

Start 将本次 Run Input 引入控制图。Start 和普通非 Branch Node 成功后，会激活其全部出边：

第一阶段 Start 契约固定为：

```json
{
  "id": "start",
  "type": "start",
  "title": "开始",
  "inputs": [],
  "outputs": [
    { "name": "workflow_input", "data_type": "object" }
  ]
}
```

Workflow Run Input 必须是 JSON Object；没有业务参数时使用 `{}`。

```text
Node A succeeded
├─ Edge A→B = active
└─ Edge A→C = active
```

因此普通 Node 的多出边表示并行控制路径，不表示条件选择。

## 4. Branch

Branch 是 Engine 解释的排他控制节点，不创建 Node Attempt，也不进入 Kafka。

冻结语义：

1. 每次 Branch 求值必须且只能选中一个 Route。
2. Branch 必须声明 Default Route，保证没有条件命中时仍能确定一个 Route。
3. Branch 的每条出边都必须声明 `route`。
4. 出边所引用的 Route 必须由该 Branch 声明。
5. 选中 Route 的全部出边变为 active，其他 Route 的全部出边变为 inactive。
6. 同一 Route 可以对应多条出边，从而在选中后并行激活多个下游。

```text
Branch selected route = approved

route=approved Edge 1 ─ active
route=approved Edge 2 ─ active
route=rejected Edge   ─ inactive
route=default Edge    ─ inactive
```

Branch 固定包含一个业务值 Input，以及两个 Literal 配置 Input：

```json
{
  "id": "check_order",
  "type": "branch",
  "title": "判断订单",
  "inputs": [
    {
      "name": "value",
      "data_type": "object",
      "source": "ref",
      "ref_node": "load_order",
      "ref_output": "result"
    },
    {
      "name": "cases",
      "data_type": "array",
      "source": "literal",
      "value": [
        { "route": "approved", "path": "status", "operator": "eq", "value": "approved" },
        { "route": "manual_review", "path": "risk.score", "operator": "gte", "value": 80 }
      ]
    },
    {
      "name": "default_route",
      "data_type": "string",
      "source": "literal",
      "value": "rejected"
    }
  ],
  "outputs": []
}
```

- `value` 允许六种 `data_type`，可以是 Literal，也可以 Ref 任意满足数据依赖约束的控制流上游 Output；
- `cases` 和 `default_route` 只能是 Literal；
- Case 按数组顺序求值，第一个匹配项胜出；
- 每个 Case 只包含一个判断，不支持 `and/or` 或任意函数调用；复杂判断先使用 Code Node 计算；
- Primitive 或 Array 直接操作整个 `value`，省略 `path`；
- Object 可以直接执行对象 Operator，或者用 `a.b.c` 形式的简单字段路径取得内部值后执行对应类型的 Operator；
- 不支持完整 JSONPath、数组过滤器或表达式脚本；
- 路径不存在或实际类型与 Operator 不匹配时 Branch 失败，不能静默选择 Default Route。

第一阶段 Operator 矩阵为：

| Value Type | Operators |
|---|---|
| `string` | `eq`、`neq`、`contains`、`not_contains`、`starts_with`、`ends_with` |
| `integer` | `eq`、`neq`、`gt`、`gte`、`lt`、`lte` |
| `number` | `eq`、`neq`、`gt`、`gte`、`lt`、`lte` |
| `boolean` | `eq`、`neq` |
| `array` | `eq`、`neq`、`contains`、`not_contains` |
| `object` | `eq`、`neq`、`has_key`、`not_has_key` |

`number` 使用十进制 Decimal 比较；`integer` 可以提升后与 `number` 比较。Array `contains` 使用元素深度相等，Object/Array `eq` 使用 JSON 结构相等且忽略 Object 字段顺序。`has_key` 只判断字段是否存在，不把值为 `null` 当作不存在。其他类型之间不进行隐式转换。

Branch 路径或类型错误至少返回 `BRANCH_PATH_NOT_FOUND` 或 `BRANCH_OPERATOR_TYPE_MISMATCH`，并定位到 `cases` 中对应 Route。

## 5. 隐式汇合

第一阶段不设置 Join Node。任何具有多条入边的 Node 都使用相同的隐式 OR-Join 规则：

```text
等待所有直接 Control Input 完成结算
├─ 全部 inactive    → Node Run = skipped
└─ 至少一条 active  → Node 激活一次
```

如果存在多条 active 入边，节点也只执行一次。节点激活后仍需等待其必填 Data Dependency 成功，才能进入 `ready`。

该汇合语义只合并控制路径，不自动合并不同路径产生的数据。

## 6. Skipped

`skipped` 的唯一含义是没有 Active Control Path 到达该 Node：

- 未被 Branch 选中的路径向下传播 inactive；
- 只有当某 Node 的全部入边都是 inactive 时，该 Node 才进入 `skipped`；
- 如果同时存在其他 active 入边，该 Node 仍被激活；
- 一旦激活，不得再进入 `skipped`。

Run 失败、取消或超时引起的未执行状态仍使用带原因的 `canceled`，不能伪装成 `skipped`。

## 7. 数据引用与分支输出限制

必填 Ref Input 必须满足：

```text
Active(Target) ⇒ Active(Source)
```

单纯存在 `Source → ... → Target` 路径并不足够。以下结构中的 End 不能安全地直接引用 `Task A.output`：

```text
             ┌→ Task A ─┐
Start → Branch           ├→ End
             └→ Task B ─┘
```

选择 Task B 路径时，End 仍会激活，而 Task A 不会激活。因此该引用必须在 Publish 阶段以 `UNSAFE_DATA_BINDING` 拒绝。

第一阶段的明确限制是：

- End 只能引用在其所有可能激活路径上都必然产生的数据；
- 不支持 Optional Reference；
- 不支持分支结果自动 Coalesce；
- 不支持 Runtime 根据“哪个值存在”猜测 Workflow Output；
- 需要分支结果汇合时，未来显式增加 Data Merge/Select 语义。

## 8. End 与 Run 完成

End 是唯一控制出口，不创建 Node Attempt。End 的所有入边完成结算且至少一条 active、其必填输出引用也已成功解析后，Engine 完成 Workflow Output 并将 Run 推进为 `succeeded`。

第一阶段 End 契约固定为：

```json
{
  "id": "end",
  "type": "end",
  "title": "结束",
  "inputs": [
    {
      "name": "workflow_output",
      "data_type": "object",
      "source": "ref",
      "ref_node": "result_node",
      "ref_output": "result"
    }
  ],
  "outputs": []
}
```

`workflow_output` 必填、固定为 `object`，允许 Literal 或 Ref；没有业务输出时使用 `{}`。End 本身没有 Output，Engine 将该 Input 固化为 Workflow Run Output。因此第一阶段 Workflow 对外接口始终是 `JSON Object → Workflow → JSON Object`。

在 fail-fast 策略下，任何已激活 Task Node 的最终失败都会先使 Run 进入失败流程，因此不能通过另一条路径到达 End 来掩盖失败。

## 9. 编译期校验

Draft 可以暂时违反图约束，但 Draft Test 和 Publish 必须至少校验：

- Start/End 数量及入出度；
- DAG 无环；
- 全图从 Start 可达且能到达 End；
- Edge ID 唯一及 Node 引用有效；
- 非 Branch Edge 不包含 `route`；
- Branch 出边 Route 完整且存在 Default Route；
- Branch 每次只能选择一个 Route；
- Ref Source 存在、Output 存在且类型兼容；
- Ref Source 是 Target 的控制上游；
- `Active(Target) ⇒ Active(Source)`。

## 10. M2 静态分析算法

M2 使用有序多值决策图（Multi-valued Decision Diagram，MDD）精确表达 Node Activation：

1. 按确定性的拓扑顺序为每个 Branch 建立一个多值变量，其 Domain 是该 Branch 声明的全部 Route；
2. Start 的激活公式为 `true`；普通 Edge 传播 Source 激活公式；Branch Edge 增加 `branch_route = edge.route` 条件；
3. 多入边 Node 的公式是所有入边公式的逻辑 OR，与隐式 OR-Join 完全一致；
4. 对每个 Ref 检查 `Active(Target) AND NOT Active(Source)` 是否恒为 `false`；非空即返回 `UNSAFE_DATA_BINDING`；
5. MDD 使用唯一表和 Apply Memo 共享等价子图，不枚举所有 Branch Route 组合。

选择 MDD 而不是只做 Dominator，是因为普通 Node 多出边会并行激活：两个并行节点可能始终同时激活，但彼此不构成经典控制流 Dominator。选择 MDD 而不是 Route 组合穷举，是为了避免多个独立 Branch 汇合时出现指数级组合。

分析设置有界节点数并失败关闭；超过实现安全上限返回 `CONTROL_GRAPH_COMPLEXITY_EXCEEDED`，绝不以近似算法放行不安全引用。控制上游可达性仍是独立前置条件，使用 DAG 拓扑与位集合传递闭包检查。

未来 Data Merge/Select 的节点形态仍未冻结。

### v2 控制节点补充

`control.start@2` 在初始化时按具名端口校验 Run Input，将各字段作为 Start Effective Output；字段缺失、多余或类型错误均使初始化失败，并由 Engine 按持久化失败路径结算 Run。`control.end@2` 解析同名 Input 后按输出声明复核类型，组装 JSON Object 并沿用原有 End/Run 原子成功边界。`control.branch@2` 按声明顺序对组内条件执行 AND/OR，首个命中 Route 生效，未命中走 ELSE；路径不存在或类型不匹配仍失败，不能当作条件 false。旧 `@1` 操作语义不变。
