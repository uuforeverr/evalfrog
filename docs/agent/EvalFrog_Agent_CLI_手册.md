# EvalFrog Agent CLI 操作手册

本手册面向会调用终端命令的 Agent。目标是让 Agent 使用 `evalfrog` 完成
**IR 创作 → Draft 校验/测试 → 发布 → 正式运行 → 诊断** 的闭环，而不是让
Agent 手拼 DSL 或绕过平台的版本、权限和运行时边界。

> 当前产品入口是 `evalfrog` CLI 与 Web Canvas 共用的 External API。CLI 只
> 接受 Canonical JSON IR；DSL、Source Map、Execution Snapshot 都由服务端在
> 校验和编译后生成，客户端永远不能上传或修改它们。

## 0. 为本地 Agent 安装 Skill

`skills/evalfrog-workflow/` 是可随仓库分发的 Agent Skill，不是另一套平台
API。它以短小的 `SKILL.md` 作为执行入口，并将发现、IR 创作、Definition
生命周期和 Run 操作拆入 `references/`；因此后续增加节点或运行能力时，只需要
扩展对应场景参考，而不会让每个 Agent 每次都加载一份不断膨胀的总指南。

在包含该目录的仓库根目录执行以下命令，即可把整个 Skill 安装至本机 Codex 默认
发现目录（`$CODEX_HOME/skills`，未设置时为 `~/.codex/skills`）：

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File .\skills\evalfrog-workflow\scripts\install.ps1
```

```bash
./skills/evalfrog-workflow/scripts/install.sh
```

如果同名 Skill 已存在，安装器不会静默覆盖；确认升级后显式附加 `-Force`（PowerShell）
或 `--force`（bash）。安装完成后，本地 Agent 可按 `$evalfrog-workflow` 引用该
Skill，或由其元数据在涉及 EvalFrog Workflow/IR/CLI 操作时自动匹配。

## 1. Agent 的职责与操作边界

Agent 应当作为执行者：理解用户目标、生成或修改 IR、调用 CLI、解释校验与
运行结果。用户不需要知道节点 ID、DSL 或底层 Worker 拓扑。

必须遵守以下规则：

1. 先确认连接上下文，再执行具体任务；没有 `server`、`project` 或 `token`
   时，向用户索取，不能猜测或扫描凭证。
2. 先读后写。编辑已有 Workflow 前必须 `workflow builder pull --session <目录>`；
   需要复用时，已知 Workflow ID 与 Published Version 后优先
   `workflow builder copy --session <新目录>`。旧命令仅作为完整 IR 兼容路径。
3. 创建、复制、保存 Draft、测试运行、发布、正式运行、取消和恢复请求都会
   改变平台状态；执行前必须向用户说明目标与影响并取得确认。
4. 每次写请求使用新的、语义唯一的 `idempotency-key`。网络重试同一个请求时
   复用原 key；请求内容或意图变化时必须换新 key。
5. Builder Session 内每次形成一组修改后先执行 `workflow builder check`；确认要保存
   后再 `workflow builder push` 和 `workflow builder validate`。只有服务端
   `valid=true` 才能测试或发布；本地快速校验不能代替服务端校验。
6. 不把 API Token、HTTP Connection 的凭证、RPC 服务端点或运行输入/输出写进
   对话摘要、日志、IR 或代码。IR 只引用 `connection_ref` / `service_ref`。
7. 不进行无界轮询。查询 Run 时使用有限次数和退避；超过等待窗口后把 Run ID
   与当前状态交还给用户。

## 2. 当前 CLI 能力与非能力

| 目标 | 已实现 CLI | Agent 处理方式 |
|---|---|---|
| Agent 创建 / 小步编辑 Workflow | `workflow builder ...` | 本地 Session 内增删 Node/Edge、配置 Input、Binding、Output 与 Layout；最终完整 IR Push |
| 完整 IR 兼容创建 / 编辑 | `workflow create`、`workflow pull`、`draft push` | 仅用于导入或已持有完整 IR 文件的场景 |
| 复用已发布流程 | `workflow builder copy` | 从指定不可变 Published Version 创建新 Draft，并落入新 Session |
| 结构、Catalog、资源校验 | `draft validate` | 读取结构化诊断并修复 IR |
| Draft 测试 | `run test` | 执行当前 Draft Revision 的不可变测试快照 |
| 发布与正式运行 | `publish`、`run create` | Publish 自动激活新 Version；正式 Run 只执行激活发布版本 |
| 查询、诊断、取消 | `run status|diagnose|cancel` | 用 Run ID 读取权威投影或请求终止 |
| 目录发现 | `node-type list`、`connection list` | 先读取节点能力与当前项目可用 Connection |

以下能力**尚未提供**，Agent 不得假定存在：

- `auth login`、默认项目配置或自助签发 Token；部署方必须提供 Endpoint、
  Project ID 与 Bearer Token。
- `workflow list|get`、`node-type describe`、DSL 上传或 DSL 反编译。
- RPC Service/Operation 的目录查询；使用 `rpc` 前必须由用户或项目管理员提供
  已授权的 `service_ref` 与 `operation`，再由服务端解析其内部契约。
- 客户端选择 Node/Operation Version、直接投递 Kafka 消息、直接修改 Run 状态，
  或上传 Source Map。

CLI 的成功输出混合为简洁文本和 JSON：创建/保存/发布输出摘要，`run status`、
`run diagnose`、目录查询输出 JSON。Agent 应根据命令语义解析所需的 ID，不应
假定存在统一 JSON envelope。

`workflow builder` 是 Agent 的首选 Authoring 路径。它在显式目录中维护
`ir.json + meta.json`：局部命令只改该 Session，`create` / `push` 仍按当前
Revision 向同一 External API 提交完整 IR。它不保存 Token，也不生成 DSL；因此
改善 Agent 的增量编辑体验，但不改变 Draft、Compiler 或 Runtime 的权威边界。

## 3. 启动前检查

### 3.1 获取并保护连接上下文

部署方应提供以下值。不要在最终答复中回显 Token。

```text
EF_SERVER=http://localhost:8080
EF_PROJECT=<project-uuid>
EF_TOKEN=<bearer-api-token>
```

PowerShell 示例：

```powershell
$env:EF_SERVER = 'http://localhost:8080'
$env:EF_PROJECT = '<project-uuid>'
$env:EF_TOKEN = '<bearer-api-token>'
$EF = '.\bin\evalfrog.exe' # 或部署方安装的 evalfrog
```

`EF_SERVER`、`EF_PROJECT` 与 `EF_TOKEN` 是 CLI 的默认连接上下文；配置后可以省略
每条命令中的 `--server`、`--project`、`--token`。显式传入这三个 Flag 时，以 Flag
为准，适合临时切换 Project 或 Control Plane。

仓库本地开发可先构建 CLI：

```powershell
go build -o .\bin\evalfrog.exe .\cmd\evalfrog
& $EF version
& $EF doctor --profile local --config-dir configs
```

`doctor` 只验证本地配置与 Control Plane 就绪状态，不会替代 API 授权检查。

### 3.2 在每个新任务中发现可用能力

```powershell
& $EF node-type list --server $env:EF_SERVER --token $env:EF_TOKEN --project $env:EF_PROJECT
& $EF connection list --server $env:EF_SERVER --token $env:EF_TOKEN --project $env:EF_PROJECT
```

当前内置 Node Type 是 `start`、`end`、`branch`、`code`、`http`、`rpc`。但 Agent
仍应以 `node-type list` 返回的作者态描述为准：它包含允许输入、数据类型、
literal/ref 来源限制、输出约束及示例。不要在 IR 中写 DSL Operation、内部
版本号、Connection ID、Service ID 或 Secret。

`connection list` 只返回当前 Principal 在当前 Project 可见的安全摘要。若目标
Connection 不在列表中，应请用户或管理员完成授权/配置，不能伪造
`connection_ref`。

旧的 `workflow pull` 本地 Workspace 只用于保存最近一次 Pull/Save 的 Draft
Revision 与 IR，以支持乐观锁；它不是服务端 Definition 的权威副本，也没有
`pull --out` 导出参数。需要编辑已有 Draft 时，Agent 应优先使用
`workflow builder pull --session <目录>`：该命令会把当前 Draft 明确写入 Session
中的 `ir.json`，并保存对应 Revision。完整 IR 直传的兼容路径仍应把待编辑 IR
保存在明确任务文件（如 `workflow.ir.json`）中，不能依赖临时内存中的旧 IR 做
覆盖式 Push。

## 4. IR 创作规范

IR 是 Agent 和 Web 的共同编辑格式，顶层固定为：

```json
{
  "ir_version": "1",
  "nodes": [],
  "edges": [],
  "layout": {}
}
```

每个 Node 统一使用 `id`、`type`、`title`、`inputs`、`outputs`；每个 Edge 使用
`id`、`source`、`target`，Branch 出边额外使用 `route`。所有逻辑 ID 由作者离线
生成。Agent 应使用可读、稳定、语义化的 ID，例如 `normalize_request` 和
`normalize_to_end`，而不是 DSL Execution ID。

Input 始终为列表元素；不使用 `params`、`port` 或独立 `binding` 集合：

```json
{ "name": "message", "data_type": "string", "source": "literal", "value": "hello" }
{ "name": "request", "data_type": "object", "source": "ref", "ref_node": "start", "ref_output": "workflow_input" }
```

`ref` 的源节点必须沿控制图能够到达当前节点；否则服务端会在结构校验时拒绝，
不能把这种错误推迟到运行时。`layout` 是共享画布布局，修改它也会产生新的
Draft Revision，但不会被编译进 DSL。

### 4.1 最小可测试 Code Workflow

下面是一个不依赖 Connection 的完整示例。Code Node 的 Python 入口必须是
`main(inputs)`，返回值必须是 JSON Object，且字段与声明的 `outputs` 匹配。

```json
{
  "ir_version": "1",
  "nodes": [
    {
      "id": "start",
      "type": "start",
      "title": "开始",
      "inputs": [],
      "outputs": [{ "name": "workflow_input", "data_type": "object" }]
    },
    {
      "id": "normalize_request",
      "type": "code",
      "title": "整理输入",
      "inputs": [
        {
          "name": "source_code",
          "data_type": "string",
          "source": "literal",
          "value": "def main(inputs):\n    request = inputs['request']\n    return {'result': {'message': request.get('message', '')}}"
        },
        {
          "name": "request",
          "data_type": "object",
          "source": "ref",
          "ref_node": "start",
          "ref_output": "workflow_input"
        }
      ],
      "outputs": [{ "name": "result", "data_type": "object" }]
    },
    {
      "id": "end",
      "type": "end",
      "title": "结束",
      "inputs": [
        {
          "name": "workflow_output",
          "data_type": "object",
          "source": "ref",
          "ref_node": "normalize_request",
          "ref_output": "result"
        }
      ],
      "outputs": []
    }
  ],
  "edges": [
    { "id": "start_to_normalize", "source": "start", "target": "normalize_request" },
    { "id": "normalize_to_end", "source": "normalize_request", "target": "end" }
  ],
  "layout": {
    "start": { "x": 80, "y": 160 },
    "normalize_request": { "x": 360, "y": 160 },
    "end": { "x": 640, "y": 160 }
  }
}
```

Node Type 差异仅出现在各自的 Input 名称与约束中：

- `code`：`source_code` 只能是 literal；其他命名输入会被封装为 `inputs`。它在
  固定 Python 沙箱执行，不应持有 HTTP、数据库或平台凭证。
- `http`：仅以 `connection_ref` 加相对 `relative_path` 调用；不得在 IR 中写
  完整 URL 或 Secret。还需声明 `method`，可选 `query`、`headers`、`body`、
  `accepted_statuses`。
- `rpc`：以 `service_ref` 与 `operation` 选择已授权的服务操作，并以 `request`
  传递数据；契约版本和幂等能力由服务端解析。
- `branch`：用 `value`、`cases`、`default_route` 决定出边 `route`；每条条件边
  必须有对应 Route。
- `end`：`workflow_output` 必须引用可到达的上游 `object` Output。

更详细的格式与字段语义以[IR 编辑态结构基线](../architecture/decisions/03_IR编辑态结构基线.md)为准。

## 5. Agent 的标准执行流程

正常情况下，Agent 应使用 `workflow builder` 进行小步构建、Copy 后修改和现有
Draft 编辑；完整 IR 直传仅保留给已持有完整 IR 文件的兼容/导入场景。Builder 的
Session 命令、`check` 与 `preview` 见安装后 Skill 的
`references/definition-lifecycle.md`，或直接使用：

```powershell
& $EF workflow builder init --session .\order.session
& $EF workflow builder add-node --session .\order.session --id start --type start --title '开始'
& $EF workflow builder check --session .\order.session

# 获得用户确认后，才创建服务端 Draft；响应 data 中带 workflow_id 与 revision。
$createKey = 'create-order-normalizer-20260815-001'
& $EF workflow builder create --session .\order.session `
  --server $env:EF_SERVER --token $env:EF_TOKEN --project $env:EF_PROJECT `
  --name '订单输入整理' --idempotency-key $createKey
```

在创建后的任意本地小步修改使 Session 变为 `dirty=true` 后，才使用新的
Idempotency Key 执行 `builder push`，随后执行 `builder validate`；没有本地变化时
`push` 会明确返回 `NO_LOCAL_CHANGES`，避免无意义地创建 Revision。

以下示例是**完整 IR 兼容模式**：仅当 Agent 已持有完整 `workflow.ir.json`（例如
导入）时使用，并且已取得用户对相应写操作的确认。

### 5.1 创建新的 Workflow

```powershell
$createKey = 'create-order-normalizer-20260815-001'
& $EF workflow create `
  --server $env:EF_SERVER --token $env:EF_TOKEN --project $env:EF_PROJECT `
  --name '订单输入整理' --ir .\workflow.ir.json --idempotency-key $createKey
```

Draft Revision 写到本机 `~/.evalfrog` 下按 Project/Workflow 隔离的 Workspace。
成功后记录输出中的 Workflow UUID，记为 `$workflowID`。CLI 同时把 IR 和当前
Draft Revision 写到本机 `~/.evalfrog` 下按 Project/Workflow 隔离的 Workspace。
Draft Revision 写到本机 `~/.evalfrog` 下按 Project/Workflow 隔离的 Workspace。

### 5.2 编辑已有 Draft：Pull → 修改 → Push → Validate

```powershell
& $EF workflow pull `
  --server $env:EF_SERVER --token $env:EF_TOKEN --project $env:EF_PROJECT `
  --workflow $workflowID

# Agent 读取拉取的 IR、按用户请求生成新的完整 workflow.ir.json 后：
$pushKey = 'update-order-normalizer-20260815-001'
& $EF draft push `
  --server $env:EF_SERVER --token $env:EF_TOKEN --project $env:EF_PROJECT `
  --workflow $workflowID --ir .\workflow.ir.json --idempotency-key $pushKey

& $EF draft validate `
  --server $env:EF_SERVER --token $env:EF_TOKEN --project $env:EF_PROJECT `
  --workflow $workflowID
```

若 Push 返回 `DRAFT_REVISION_CONFLICT`，说明其他作者已经保存了更新。处理顺序
固定为：重新 `pull` → 读取新 IR → 重新应用仍符合用户意图的最小修改 → 使用**新**
idempotency key 再次 `push`。禁止用旧 Revision 强行覆盖。

### 5.3 复用已发布 Workflow

当用户给出来源 Workflow ID 和 Published Version 后，先取得创建副本的确认：

```powershell
$copyKey = 'copy-customer-flow-v3-20260815-001'
& $EF workflow copy `
  --server $env:EF_SERVER --token $env:EF_TOKEN --project $env:EF_PROJECT `
  --source-workflow $sourceWorkflowID --version 3 --name '客户流程副本' `
  --idempotency-key $copyKey
```

Copy 产生独立 Workflow 与 Draft，而不是改写来源 Published Version。随后按照
“Pull → 修改 → Push → Validate”流程继续。

### 5.4 测试、发布与正式运行

测试运行使用当前 Draft Revision 的不可变快照；正式运行只能使用已经发布并自动
激活的版本。两者都可能触发 Code、HTTP 或 RPC 执行，因此必须先说明影响并确认。

```powershell
'{"message":"hello"}' | Set-Content -Encoding utf8 .\run-input.json

$testKey = 'test-order-normalizer-20260815-001'
& $EF run test `
  --server $env:EF_SERVER --token $env:EF_TOKEN --project $env:EF_PROJECT `
  --workflow $workflowID --input .\run-input.json `
  --deadline '2026-08-15T16:30:00Z' --idempotency-key $testKey

$publishKey = 'publish-order-normalizer-r1-20260815-001'
& $EF publish `
  --server $env:EF_SERVER --token $env:EF_TOKEN --project $env:EF_PROJECT `
  --workflow $workflowID --change-log '首次发布：整理订单输入' `
  --idempotency-key $publishKey

$runKey = 'run-order-normalizer-20260815-001'
& $EF run create `
  --server $env:EF_SERVER --token $env:EF_TOKEN --project $env:EF_PROJECT `
  --workflow $workflowID --input .\run-input.json `
  --deadline '2026-08-15T16:45:00Z' --idempotency-key $runKey
```

`publish` 成功即“发布并激活”；不需要、也不能再调用单独的激活命令。正式 Run
不接受客户端指定 Draft 或 Version，因此不会执行未发布的定义。

### 5.5 观察、诊断与终止

```powershell
& $EF run status `
  --server $env:EF_SERVER --token $env:EF_TOKEN --project $env:EF_PROJECT `
  --run $runID

& $EF run diagnose `
  --server $env:EF_SERVER --token $env:EF_TOKEN --project $env:EF_PROJECT `
  --run $runID
```

失败诊断会基于运行快照中的 Source Map 返回逻辑 `node_id`、`edge_id` 与 IR 字段
路径。Agent 应修改对应 IR 字段，不应尝试修改 DSL Execution ID。

取消需要用户确认；它写入持久化终止意图，Engine 会异步收敛状态，CLI 成功返回
不等于所有 Worker 已瞬间停止：

```powershell
& $EF run cancel `
  --server $env:EF_SERVER --token $env:EF_TOKEN --project $env:EF_PROJECT `
  --run $runID
```

`run replay` 是受权限保护的恢复请求，只能让系统重新检查一个当前可执行的持久化
事实，不能重放任意 Kafka 消息。仅在 Project Admin 明确要求故障恢复时使用。

## 6. 与 Web Canvas 协作

CLI 与 Web 编辑的是同一个 Draft Revision。CLI 创建或 Copy 后，将 `Project ID`、
`Workflow ID` 与 Token 提供给用户，即可在 Web Canvas 加载该 Draft。交接前应先
`workflow builder pull --session <目录>`，避免误以为本机文件天然就是服务器最新状态。

当人类在 Canvas 中保存后，Agent 再次修改前必须 Pull；当 Agent Push 后，人类在
Canvas 再操作前应重新加载。Revision Conflict 是保护并发作者而非可忽略错误。

## 7. 故障处理速查

| 现象 | Agent 的处理 |
|---|---|
| CLI 返回参数错误（退出码 2） | 修正命令和必填 flag；不要重试相同错误命令 |
| 本地 IR 校验失败 | 修正 JSON 外壳、ID、Input 或 Edge；不发起 API 写入 |
| `valid=false` | 读取诊断中的 Phase、Code、Node/Edge/IR Path，在 Builder Session 中做最小修改后再次 Push/Validate |
| `DRAFT_REVISION_CONFLICT` | Builder Pull 最新 Draft，合并最小修改，换新 key 后 Push |
| `CONNECTION_BINDING_REQUIRED` / `SERVICE_BINDING_REQUIRED` | 先查目录；要求授予/配置资源，不能以 URL、Secret 或内部 ID 绕过 |
| Run 失败 | `run status` 后 `run diagnose`，按 Source Map 的逻辑位置修复并重新测试/发布 |
| Run 仍在执行 | 有界间隔查询；用户决定取消或继续等待 |

## 8. Agent 交付给用户的结果格式

每一次操作结束，Agent 应按产品概念而不是底层命令总结：

- 读取：找到的 Workflow/Connection/Node Type、关键 ID 和下一步建议；
- Draft 修改：Workflow ID、Draft Revision、校验结论及诊断位置；
- 测试或正式运行：Run ID、Purpose、当前状态、是否已到达终态；
- 发布：Workflow ID、不可变 Version Number、已自动激活的事实；
- 失败：安全错误摘要、映射回的 IR Node/Edge/Field、建议的最小修复。

不要在正常结果中输出 Token、Secret、完整执行输入/输出、Kafka Topic、Lease Token
或内部 DSL。只有用户要求复现或排障时，才附上必要且已脱敏的 CLI 命令。
