# 分布式调度与 Worker 执行边界

> 状态：第 12 节为当前已冻结实现；第 13 节已完成本地实现但待真实环境验收；第 14 节为已确认、待实现的热路径优化
>
> 范围：最近几轮关于分布式部署、项目公平调度、Kafka、Worker、Attempt 恢复的讨论
>
> 前置基线：[00_核心架构决策基线.md](./00_核心架构决策基线.md)

本文只记录最近几轮新增或修正的决策，不重复 Definition、Draft/Published Version、IR/DSL、Source Map、Run/Node Run 基础生命周期等既有结论。第 1～11 节保留历史 Scheduler/Redis 演进，第 12 节废止独立 Scheduler，第 13～14 节记录微批与缓存演进，第 15 节是当前移除 Redis 后的执行架构与验收基线。历史段落中未逐句改写的 Redis 实现不再指导当前代码。

## 1. 分布式范围与第一阶段部署形态

系统中需要支持分布式的部分包括：

- 无状态 API、Definition Service 和 Compiler 的水平扩展；
- 多 Engine 实例共同推进大量 Workflow Run；
- 多 Scheduler 实例进行项目级公平准入；
- Kafka TaskBus 和运行事件的分布式传输；
- 按资源类独立扩缩容的 Worker Pool；
- 多实例 Outbox Relay、Inbox Consumer、Retry Timer 和 Recovery Scanner；
- Redis 调度共享状态和 Active Run Hot Cache；
- MySQL 作为第一阶段的权威持久层；Redis 和 Kafka 均不承担权威持久化。

第一阶段采用：

```text
模块化 Control Plane（可多副本）
+ 独立 Worker Pool（按资源类扩缩容）
+ MySQL / Scheduling Redis / Cache Redis / Kafka
```

“模块化 Control Plane”表示 API、Definition、Compiler、Engine、Scheduler、Attempt Coordinator、Outbox Relay、Retry/Recovery 等是边界清楚的逻辑模块，但第一阶段允许它们位于同一个部署单元中。是否拆成独立微服务是后续部署选择，不改变模块接口和状态所有权。

项目可执行程序边界冻结为：

```text
evalfrog                    Agent/Human 使用的 CLI
evalfrog-control-plane      External API + Control Plane 逻辑模块
evalfrog-worker-builtin     HTTP/RPC Resource Class
evalfrog-worker-sandbox     Code/Sandbox Resource Class
```

`evalfrog-control-plane` 使用同一套代码和一个镜像。默认部署可以启用全部角色；未来可通过启动 Role 分别运行 API、Runtime Consumer、Scheduler、Relay/Recovery 或 Projection，以便按容量独立扩展，但 Role 选择不得改变模块接口、事务边界和状态所有权，也不要求拆仓库。

Scheduling Redis 与 Cache Redis 是不同逻辑接口，生产环境使用不同 Endpoint 隔离淘汰策略、容量和故障域；本地开发可以共用。不同 Endpoint 不等于必须立即建设 Redis Cluster，第一阶段可以分别使用高可用主从部署。Scheduling Redis 当前采用一个同槽逻辑分片；只有容量证据证明它成为瓶颈时，才另行设计分片协议。

Engine 不永久持有某个 Workflow Run。它是无状态、事件驱动、可恢复的状态推进器；任何实例都可以依据 Inbox、数据库权威状态和 CAS 接管同一个 Run 的后续推进。

## 2. 历史调度实现：Max-Min 与 Lane/Credit

> 本节及 2.1、2.2 仅用于解释初始 M6 实现，已由第 11.5～11.7 节的一秒时间桶 FIFO、同桶 Project Load 和 Topic Queue Window 取代。

调度不是全局 FIFO。公平调度的唯一身份是 `project_id`；Node Type、Worker Pool、执行安全边界和 Kafka Topic 均不参与项目公平份额计算。跨 Project 和 Project 内部采用两级规则：

```text
第一级：在有竞争的 Project 之间等权轮转
第二级：在选中的 Project 内选择 Ready Node
```

项目间规则：

- 不设置 `project_priority`、`project_weight` 或固定 30% 配额；
- 每个调度周期根据有 Ready Task 的竞争项目集合，使用等权 Max-Min Fairness 动态计算项目基础额度；
- 当共有 `N` 个有充分需求的竞争项目时，每个项目的基础份额约为总派发容量的 `1/N`；
- 某个项目需求不足时，未使用容量由其他有需求项目等权借用，系统保持 Work-Conserving；
- 新竞争项目出现后，已超出新份额的项目停止获得新额度，但不抢占其正在执行的 Attempt；
- `inflight = queued + claimed + running`；其中 `claimed` 是领取过程，成功 Claim 后持久状态计入 `running`，不增加新的 Node Run 状态；
- `ready` 与 `retry_wait` 不计入 Inflight。

“竞争项目”是当前仍有 Ready Task 需要准入的 Project。只有 Running Task、但没有新 Ready Task 的 Project 不占用新的调度份额，其既有 Inflight 仍计入额度判断。为避免项目集合短暂变化造成额度抖动，Scheduler 使用带 Epoch 版本的周期性快照和可配置 Active TTL；周期长度与 TTL 是容量参数，不是领域语义。

Project 内部选择顺序固定为：

```text
priority DESC
ready_at ASC
node_run_id ASC
```

因此，相同 Priority 下是稳定 FIFO；它只保证派发顺序，不保证节点完成顺序。全局不存在跨 Project FIFO，因为它会让大项目的长队列压制其他项目，违背公平性目标。

多 Scheduler 实例不能各自维护彼此独立的本地项目轮转状态。所有实例共享 Redis 中的项目公平状态，并由数据库 CAS 对 `ready → queued` 作最终裁决。Redis 状态丢失时，从数据库中的 `ready/queued/running` 事实重建后再恢复新准入。

### 2.1 有界 Dispatch Window

Kafka 之前必须设置有界 `dispatch_window`：

```text
admitted = queued + claimed + running
admitted <= dispatch_window
```

`dispatch_window` 只允许接近 Worker 短期消化能力的 Attempt 进入 Kafka，可包含少量传输缓冲；其余 Node Run 保持数据库权威的 `ready` 状态。禁止将某个 Project 的大量 Ready Task 深度预派发到 Kafka，否则后加入的 Project 无法被 Scheduler 重新公平排序，取消和额度调整也会被已经形成的 Kafka 积压架空。

Worker Pool 的可用执行槽仍是派发可行性约束，但不形成第二套公平身份：项目额度跨其全部 Task 统一计算，Task 只有同时具备项目 Credit 和目标 Worker Pool 可用容量时才能准入。

### 2.2 固定 Scheduling Lane 与批量 Credit

Redis 调度状态使用固定数量的逻辑 Scheduling Lane，而不是一个承载所有 Project 的全局有序集合：

```text
lane_id = stable_hash(project_id) % lane_count
```

Lane 是 Redis 调度数据的逻辑分片，不是 Redis 实例，也不是 Kafka Partition。同一个 Project 的 Active 标记、Ready 索引、Inflight Reservation、轮转位置和临时额度始终归入同一个 Lane。`lane_count` 是预先配置且显著多于初始 Redis 主节点数的容量参数；Redis Cluster 通过 Slot 迁移扩容，不改变 Project 到 Lane 的稳定映射。未来确需改变 Lane 数量时必须采用版本化迁移，不能直接取模重映射正在调度的项目。

同一 Lane 的 Key 使用 Redis Cluster Hash Tag 共置，使项目轮转、Credit 扣减和 Reservation 建立可以通过 Lane 内原子操作完成，例如：

```text
sched:{lane-17}:active-projects
sched:{lane-17}:project:{project_id}:ready
sched:{lane-17}:project:{project_id}:inflight
sched:{lane-17}:credits
```

每个 Scheduling Epoch，Control Plane 内的 Credit Balancer 根据各 Lane 汇总的竞争项目数、Ready 需求、既有 Inflight 和全局 `dispatch_window`，以等权 Max-Min Fairness 批量分配 Lane Credit。Lane 内每次准入只访问本 Lane Redis 数据并消耗一个 Credit；跨 Lane 只进行低频汇总、额度发放、回收和再平衡，不在每个 Task 的热路径上访问单个全局 Key。

Credit Balancer 是 Control Plane 内带 Lease 与 Fencing 的逻辑模块，不要求第一阶段拆成独立微服务。未使用 Credit 可在后续 Epoch 回收并分配给仍有需求的 Lane；短周期内允许近似公平，持续竞争时必须不饥饿。

## 3. 四个不得混淆的概念

| 概念 | 回答的问题 | 示例 |
|---|---|---|
| Node Type | 这个节点的业务语义是什么 | Code、HTTP、RPC |
| Resource Class | 需要什么安全边界和运行资源 | `builtin`、`sandbox` |
| Kafka Topic | Task 应路由到哪一类 Worker Pool | Builtin Task Topic、Sandbox Task Topic |
| Kafka Physical Partition | 一个 Topic 如何并行存储和消费 | Partition 0..N-1 |

它们不是一一对应关系：

- 多种 Node Type 可以映射到同一 Resource Class；
- 一个 Resource Class 对应一个逻辑 Task Topic；
- 一个 Topic 可以有多个 Physical Partition；
- 不按 Node Type 或 Project 创建 Topic；
- IR/DSL 不包含 Topic 名称，避免业务定义耦合基础设施拓扑。

平台内部的版本化 Node Catalog 描述 Node Schema 和执行能力要求；Web 与 Agent 只看到无版本号的 Node Description。Runtime Routing Policy 在发布或派发阶段解析 `resource_class` 和 Topic；Task Contract 只携带解析结果及执行身份。第一阶段不建设独立 Manifest 或 Registry 服务。

这里的 Resource Class 只描述 Worker 执行兼容性与安全隔离，不是公平调度维度。无论 Task 最终路由到哪个 Worker Pool，公平身份始终只有 `project_id`。

## 4. 第一阶段 Worker 资源类

第一阶段冻结两个最小资源类：

### 4.1 builtin

用于平台编写、受信任、允许在长生命周期 Worker 进程内运行的 Executor。第一阶段 Builtin Worker 同时支持 `HttpExecutor` 与 `RpcExecutor`。

### 4.2 sandbox

用于 Code Node。Sandbox Worker 的职责是创建、监控和销毁每个 Attempt 的隔离执行环境；用户代码不得直接运行在 Worker 主进程中，也不得继承 Worker 的数据库、网络或 Secret 权限。

资源类依据安全隔离、资源模型、依赖和配额划分，而不是简单依据 Node Type 划分。未来可以增加 `llm`、`io`、`gpu` 等资源类，但这属于 Routing Policy 演进，不修改 IR/DSL 语义。

## 5. Worker Runtime 与 Executor Catalog

Worker 由通用 Runtime 和进程内 Executor Catalog 组成：

```text
Worker Runtime
├─ Task Consumer
├─ Local Concurrency / Backpressure
├─ Attempt Client
├─ Execution Context Client
├─ Timeout / Cancellation
├─ Telemetry
└─ Executor Catalog
   ├─ Builtin Pool: HttpExecutor / RpcExecutor
   └─ Sandbox Pool: CodeExecutor / Sandbox Orchestrator
```

Worker 根据 DSL Operation Type 与 Version 从进程内 Catalog 解析 Executor，不在中心流程中使用不断增长的 Node Type `if/else` 或 `switch-case`。Catalog 只是 Handler Map，不是独立服务或领域实体。

Worker-facing 边界第一阶段使用版本化 HTTP/JSON，至少包含：

```text
ClaimAttempt
HeartbeatAttempt
CompleteAttempt
LoadExecutionContext
```

前三项由 Attempt Coordinator 提供；`LoadExecutionContext` 由 Control Plane 内显式的 Execution Context Gateway 提供。Gateway 统一读取不可变 Snapshot、Run Input 和已经被 `effective_attempt_id` 接受的上游 Output，封装 Cache Redis 优先、MySQL 回源与回填，并根据 Resource Class 裁剪资源信息。入口控制节点没有 Attempt 也没有 Output 行，其 `workflow_input` 有效输出就是 Run Input，因此引用该输出的 Binding 由 Gateway 直接复用 Run Input，而不是走 `effective_attempt_id` 查询。它是内部只读模块，不是独立微服务，不得修改 Run、Node Run 或 Attempt 状态。

HTTP/JSON 只是第一阶段 Transport Adapter。Worker Runtime 依赖 Transport-neutral Client Port；未来增加 gRPC 不得改变 Claim、Lease、Fencing、ACK 或 Execution Context 的语义。

第一阶段同一 Resource Class 的 Worker Pool 必须能力同构：每个副本都支持 Routing Policy 可能路由到该 Pool 的全部 Executor。Worker 在 Claim 前仍需上报并校验自身 Resource Class 与 Capability，防止错误部署导致不兼容 Task 进入 `running`。

M7 实现将这条约束固化为三层防线：Worker Executor Catalog 启动时必须覆盖该 Resource Class 的完整 Routing Capability；Worker Heartbeat 注册必须携带同一完整集合；Scheduling Redis 只把能力指纹匹配当前 Routing Policy 且 TTL 未过期的 Slot 计入 Dispatch Window。Attempt Claim 仍逐任务复核 Operation/Resource Class/Capability，作为最终数据库防线。这样滚动发布期间的旧能力 Worker 不会继续扩大新版本调度容量。

只有本地执行槽可用时 Worker 才领取并 Claim Task；本地等待队列必须有界，不能依靠无限预取制造隐藏积压。

Kafka Consumer 在 `Poll → MySQL Claim/Inbox → ACK` 的短临界区使用 Rebalance Gate，防止分区撤销后旧消费者提交 Offset；ACK、NACK 或进程关闭都会释放 Gate。Task ACK 后的 Executor 运行不阻塞 Kafka Rebalance，节点执行可靠性由 Attempt Lease/Reaper/Fencing 承担，而不是依赖消费者长期拥有分区。

## 6. Kafka Topic 与 Physical Partition

Kafka 被选为 TaskBus 和运行事件的传输实现，但不承担权威 Workflow 状态。

第一阶段只建立三个逻辑 Topic：

```text
workflow-task-builtin-v1  → Builtin Worker Pool
workflow-task-sandbox-v1  → Sandbox Worker Pool
workflow-runtime-event-v1 → Engine / Read Model / Web Notification Projector
```

第一阶段不建立 Definition Event Topic。Worker 与 Engine 都不订阅定义发布事件：Worker 只执行 Attempt，Engine 只读取 Run 已固定绑定的 `snapshot_id`；Execution Snapshot 不可变且按 ID 缓存，不需要发布失效事件。未来只有出现明确外部订阅者时才新增 Definition Event。

Topic 内仍需要多个 Physical Partition。即使 Topic 里的任务最终由同一类 Worker 执行，Partition 仍负责 Broker 存储、Leader 吞吐和并行消费扩展；在普通 Consumer Group 下，活跃 Consumer 数也受 Partition 数限制。因此“Worker 同类”不能推出“Topic 不需要分区”。

Task 使用 `attempt_id` 作为 Partition Key，以分散负载；Runtime Event 使用 `run_id`，使同一 Run 的信号尽量保持 Partition Locality。Task 之间没有业务顺序，Runtime Event 顺序也只作为优化；Engine 的正确性来自数据库状态、Inbox 去重和 CAS，而不是 Kafka 分区顺序。

第一阶段全部使用普通 Consumer Group。Share Group 只保留为 TaskBus Adapter 的未来替换能力，不改变 Claim、ACK、幂等和恢复语义。Partition 数量是容量配置，不进入领域模型。

Kafka Message 使用带 `message_version` 的独立 JSON DTO，不直接序列化数据库实体。Task 与 Runtime Event 只携带身份、版本、时间和 Trace 等轻量定位信息，不携带完整 DSL、Workflow Context、Node Output 或 Secret。可识别 `attempt_id` 的 Poison Task 必须先通过 Coordinator 结算 Attempt；完全无法识别身份的消息才进入对应 DLQ，DLQ 只做运维隔离，不驱动业务 Retry。

## 7. Task 领取、ACK 与完成链路

Worker 不直接访问 Workflow 数据库。Control Plane 内的 Attempt Coordinator 对 Worker 暴露三个逻辑操作：

```text
ClaimAttempt
HeartbeatAttempt
CompleteAttempt
```

完整链路为：

```text
Worker 获得本地执行槽
→ 从 Kafka 领取 Task
→ ClaimAttempt
→ 数据库事务：queued → running + Lease/Fencing
→ Claim 成功后 ACK Kafka Task
→ Worker 执行 Executor
→ CompleteAttempt
→ 数据库事务：Attempt 终态 + 带 attempt_id 的 Output Candidate + Completion Outbox
→ Outbox Relay 发布 Completion Event
→ Engine Inbox 去重 + 校验 Current Attempt
→ CAS 推进 Node Run / Workflow Run
→ 成功时设置 effective_attempt_id，Output 才对下游可见
```

ACK 不等待节点执行完成。若一直持有 Kafka 消息直到长任务结束，会把任务 Lease 与消息消费 Lease 错误绑定，并放大 Rebalance、重复投递和超时问题。ACK 之后 Worker 故障，由数据库 Lease、Heartbeat 和 Recovery Scanner 找回 Lost Attempt。

若 Claim 返回“已由当前或其他合法执行者领取、已终结、已取消”等幂等结果，Worker 按 Coordinator 返回的处置结果 ACK 或忽略，不自行猜测状态。

Poison Task 必须先记录持久化错误。只要能够识别 `attempt_id`，就必须通过 Coordinator 将 Attempt 结算为可恢复或终态，再最终 ACK/Reject；禁止让它永久停留在 `queued`。

## 8. Attempt 身份、重试与恢复

三个计数维度必须分开：

| 字段 | 含义 | 是否消耗业务重试预算 |
|---|---|---|
| `attempt_seq` | 每次物理执行的单调序号，也是 Fencing 维度 | 否，仅作执行身份 |
| `retry_count` | 业务错误触发的重试次数 | 是 |
| `recovery_count` | Worker/Lease/基础设施丢失后的恢复次数 | 否，受独立上限约束 |

Lost Attempt 不自动消耗业务重试预算，否则平台故障会错误减少用户配置的业务容错机会。基础设施恢复仍必须受 `max_recoveries`、Attempt Timeout 和 Workflow Deadline 限制，避免无限恢复。

有效完成结果至少需要匹配：

```text
current_attempt_id
+ attempt_seq
+ lease_token / fencing_token
+ expected running state
```

旧 Worker 的迟到 Heartbeat 或 Result 必须被拒绝。业务 Retry 的到期时间保存在数据库中，由 Retry Timer 扫描并重新置为可调度状态；第一阶段不使用 Kafka Delay Topic 充当权威计时器。

## 9. 数据一致性与故障恢复不变量

1. MySQL 保存 Workflow、Node Run、Attempt、Lease、Output Reference、Outbox 和 Inbox 的权威状态。
2. Redis 保存可重建的调度共享状态、Inflight Reservation、Active Run Hot Cache 和 Read Model，不参与最终语义裁决。
3. Kafka 采用至少一次投递假设；不依赖 Kafka Exactly Once 覆盖外部数据库或节点副作用。
4. Scheduler 创建 Attempt 与 NodeTask Outbox 必须处于同一数据库事务。
5. Attempt Coordinator 写 Attempt 终态、带 `attempt_id` 的 Output Candidate 与 Completion Outbox 必须处于同一数据库事务；Engine 接受 Attempt 并设置 `effective_attempt_id` 后 Output 才成为有效值。
6. Engine 先写 Inbox 去重记录，再以数据库状态和 CAS 推进 Run；重复或乱序 Completion Event 不得重复推进。
7. Outbox Relay、Inbox Consumer、Retry Timer 和 Recovery Scanner 均可多实例运行，但必须使用幂等操作及数据库 Claim/Lease，不能依赖单实例内存所有权。
8. Kafka Task 只携带执行身份和定位信息，不携带完整 Workflow Context；输入通过 Execution Context 接口读取。
9. Worker 不持有 Workflow 数据库凭证；执行层故障不能绕过 Control Plane 状态机写库。
10. 外部副作用使用稳定的逻辑幂等键，Attempt 恢复或业务重试不得生成新的业务副作用身份。
11. Outbox 只携带轻量身份和定位信息，消费者重新读取数据库权威状态；Inbox 至少以 `consumer_name + event_id` 唯一去重。
12. Runtime Outbox/Inbox 与 Run 数据同分片，不引入跨项目分布式事务；第一阶段不写无消费者的 Definition Outbox。
13. Effective Output 必须先在 MySQL 中持久化并由 Engine 通过 `effective_attempt_id` 接受，再以 Post-Commit Best-Effort 方式预热 Redis；Cache 失败不回滚状态，也不阻塞后续推进。
14. Workflow 核心权威数据不得 Redis Write-Behind；Scheduling Ready/Load/Topic/Reservation/Inflight 是可重建协调状态，不是待回写数据库的权威副本。

## 10. 第一阶段容量参数基线

容量配置提供 `local`、`test`、`production-default` 三套 Profile。本节冻结参数名称、生产默认值、参数间硬约束和调优信号；数值属于可配置的首版起点，必须通过目标环境压测校准，不是领域语义、SLO 承诺或固定容量上限，也不得反向渗透进 IR/DSL 或改变状态所有权。

### 10.1 参数间硬约束

```text
heartbeat_interval < lease_duration / 3
lost_after >= lease_duration + reaper_scan_interval
reservation_ttl >= lost_after + reconcile_interval
inbox_retention > kafka_retention + max_manual_replay_window
dispatch_window ≈ healthy_worker_slots × small_buffer_factor
所有应用实例的 DB Pool Max 之和 <= MySQL max_connections × 70%
Task Topic Partition 数 >= 计划中的同组活跃 Consumer 数
```

Redis TTL 仍不承担业务计时语义；这些关系只保证协调状态、恢复扫描和消息重放之间不产生明显时间漏洞。

### 10.2 Kafka `production-default`

| Topic | Partition | Retention |
|---|---:|---:|
| `workflow-task-builtin-v1` | 48 | 24 小时 |
| `workflow-task-sandbox-v1` | 24 | 24 小时 |
| `workflow-runtime-event-v1` | 24 | 72 小时 |
| 对应 DLQ | 不少于 6，建议与源 Topic 相同 | 14 天 |

Topic 与 Broker 侧默认：Replication Factor `3`、Min ISR `2`、`cleanup.policy=delete`、禁止自动创建 Topic、Topic 最大消息 `256 KiB`。应用层进一步将 Task/Runtime Event JSON Envelope 限制为 `64 KiB`，大对象仍通过权威存储按 ID 读取。

Producer 默认：`acks=all`、启用幂等生产、`max.in.flight.requests.per.connection=5`、`compression.type=zstd`、`linger.ms=5`、`batch.size=64 KiB`、Request Timeout `30s`、Delivery Timeout `120s`。

Consumer 默认：关闭自动提交、初始 Offset 使用 `earliest`、Session Timeout `45s`、Heartbeat `3s`、Max Poll Interval `60s`、Cooperative Sticky 分配策略、`fetch.min.bytes=1`。Task Consumer 每次只领取一个 Partition 的连续微批，应用层上限为 `min(local_free_slots, task_poll_limit, task_max_poll_records, 8)`；Runtime Event Consumer 的 `max.poll.records` 为 `100`。

Partition 是消费并行度上限而不是 Worker 执行槽上限。第一阶段每个 Worker 进程只有一个 Consumer；按 12 个期望副本估算，Builtin/Sandbox Worker 平均分别持有 4/2 个 Partition。单个 Consumer 可从多个被分配 Partition 获取微批并向本地 Slot 分发，但本地已领取执行数不得超过空闲 Slot。预计活跃 Consumer 超过 Partition 数之前必须先扩 Partition并验证 Key 分布。

### 10.3 Scheduler 与 Scheduling Redis `production-default`

| 参数 | 默认值 |
|---|---:|
| `lane_count` | 128 |
| Scheduling Epoch | 1000 ms |
| Active Project TTL | 5000 ms |
| Credit Grant Batch | 8 |
| Redis Candidate Batch | 32 |
| Admission Concurrency / Scheduler Instance | 8 |
| Inflight Reservation TTL | 300000 ms |
| Ready Reconcile Interval | 30000 ms |
| Global Dispatch Window | `ceil(total_healthy_worker_slots × 1.2)` |
| Pool Feasibility Window | `ceil(pool_healthy_worker_slots × 1.2)` |
| 每 Epoch 容量上调上限 | 10%；容量下调立即生效，避免向失去健康槽位的 Pool 继续派发 |
| 基础设施错误 Backoff | 50 ms 指数退避，最大 2s，20% Jitter |
| Idle Poll | 100 ms，最大退避至 1s |

Pool Window 只判断路由目标是否有短期消化能力，不建立第二套公平额度。Scheduler 从 Redis 每批最多取 `32` 个候选，并以最多 `8` 路并发执行逐 Attempt 的数据库 CAS；CAS 仍是 `ready → queued` 的最终裁决。

`Credit Grant Batch=8` 是 Redis 中一次发放/扣减的摊销粒度，不是“每个 Lane 每秒最多 8 个 Task”的限流上限；一个 Epoch 可以依据全局 Window 向同一 Lane 发放多个 Batch，但所有 Lane 的可用 Credit 总和不得突破当期可派发容量。

Scheduling Redis 按第 11.7 节使用 `noeviction`，单次操作 Timeout `200 ms`、最多重试 `1` 次；不可用时按既有 Fail Closed 规则暂停新准入。`noeviction` 只禁止 Redis 在内存压力下自动删除协调状态，不禁止系统依据业务生命周期执行 TTL 清理或显式删除。

### 10.4 Cache Redis `production-default`

Cache Redis 使用 `allkeys-lfu`，单次操作 Timeout `500 ms`、不在请求内重试，失败时回源 MySQL。默认 TTL：

| 数据 | 默认 TTL |
|---|---:|
| Execution Snapshot | 12 小时 |
| Active Run Context | 10 分钟；Run Input 与接受后的 Effective Output 共用该 TTL |
| Terminal Run Context | 1 小时 |
| Active Run Read Model | 15 分钟；活跃时刷新 |
| Terminal Run Read Model | 1 小时 |
| Definition Negative Cache | 30 秒 |

Inflight Reservation 的 `5 分钟` TTL 属于 Scheduling Redis，并在 Attempt 保持 Inflight 时续期，不得与 Cache Store 中的业务读取缓存混淆。

### 10.5 Worker Lease 与恢复 `production-default`

| 参数 | 默认值 |
|---|---:|
| Heartbeat Interval | 15 秒 |
| Lease Duration | 60 秒 |
| Lost 判定阈值 | 75 秒 |
| Recovery Scanner Interval | 10 秒 |
| `max_recoveries` | 3 |
| Claim API Timeout | 5 秒 |
| Complete API Timeout | 10 秒 |
| Recovery Backoff | 5 / 15 / 45 秒 |

Heartbeat、Lease 与 Lost 的数据库时间判断使用服务端时间；Worker 本地时钟不能决定 Attempt 是否失效。业务 Timeout 与 Workflow Deadline 可以早于上述恢复阈值，并始终优先阻止新的恢复执行。

### 10.6 Outbox、Inbox 与扫描器 `production-default`

| 参数 | 默认值 |
|---|---:|
| Outbox Batch | 100 |
| Outbox Active Poll | 100 ms |
| Outbox Idle Poll | 最大退避至 1 秒 |
| Outbox Claim Lease | 30 秒 |
| Publish Concurrency | 8 |
| Publish Retry | 200 ms 指数退避，最大 30 秒，20% Jitter |
| Published Outbox Retention | 7 天 |
| Inbox Retention | `max(7 天, 对应 Kafka Retention + 24 小时)` |
| Inbox/Outbox Cleanup | 30 分钟 |
| Retry Timer | 1 秒 |
| Deadline Scanner | 5 秒 |
| Recovery Scanner | 10 秒 |
| Completion/Run Reconciler | 30 秒 |
| 单次扫描 Batch | 100 |

最老未发布 Outbox 超过 `30 秒`或同一记录连续失败 `10` 次时告警。未发布 Outbox 和 DLQ 消息不得由普通保留任务自动删除。

### 10.7 MySQL `production-default`

Control Plane 每实例连接池默认 `min=5`、`max=20`，连接设置 `innodb_lock_wait_timeout`，默认事务隔离级别使用 MySQL/InnoDB `REPEATABLE READ`。Runtime 对可变 Run/Node/Attempt 使用锁定当前读和 CAS，发布版本使用 Workflow 行内计数器，避免快照读与范围读产生歧义；错误码 1213/1205 只在完整事务边界进行有界重试。部署校验必须保证所有实例连接池 Max 总和不超过数据库 `max_connections` 的 `70%`，剩余连接用于迁移、运维和故障恢复。Worker 不持有 Workflow MySQL 连接。

Scheduler 每个 Attempt 使用短事务执行带 `state_version` 的条件更新；单次从 Redis 获取 `32` 个候选，最多 `8` 路并发。Run 初始化仍是一个原子事务，Node Run 使用每批 `500` 行的批量 Insert 语句，不能为减少事务时长而拆成可见的半初始化状态。

### 10.8 `local` 与 `test` Profile

三个 Profile 使用继承覆盖模型：`production-default` 是完整基线；`test` 和 `local` 只覆盖下表列出的容量值，未列出的消息大小、可靠性语义、状态机边界和超时关系全部继承 `production-default`。Profile 只能在进程启动时选择，运行期间不得热切换；不同 Profile 必须使用隔离的 MySQL Database、Kafka Topic Prefix 和 Redis Key Prefix，不能对同一批运行数据混用。

Kafka 覆盖：

| 参数 | `local` | `test` | `production-default` |
|---|---:|---:|---:|
| Builtin Task Partitions | 1 | 3 | 12 |
| Sandbox Task Partitions | 1 | 3 | 12 |
| Runtime Event Partitions | 1 | 6 | 24 |
| DLQ Partitions | 1 | 3 | 不少于 6，建议与源 Topic 相同 |
| Replication Factor / Min ISR | 1 / 1 | 1 / 1 | 3 / 2 |
| Task Retention | 2 小时 | 12 小时 | 24 小时 |
| Runtime Event Retention | 4 小时 | 24 小时 | 72 小时 |
| DLQ Retention | 24 小时 | 3 天 | 14 天 |
| Max Manual Replay Window | 1 小时 | 24 小时 | 24 小时 |

所有 Profile 均禁止依赖 Broker 自动建 Topic，由部署脚本显式创建；均保持应用 Envelope `64 KiB` 和 Topic 最大消息 `256 KiB`，防止只在生产环境才暴露消息膨胀问题。

Scheduler、Lease 与数据库覆盖：

| 参数 | `local` | `test` | `production-default` |
|---|---:|---:|---:|
| `lane_count` | 8 | 32 | 128 |
| Scheduling Epoch | 500 ms | 500 ms | 1000 ms |
| Active Project TTL | 3 秒 | 3 秒 | 5 秒 |
| Credit Grant Batch | 4 | 8 | 8 |
| Redis Candidate Batch | 8 | 16 | 32 |
| Admission Concurrency / Instance | 2 | 4 | 8 |
| Reservation TTL | 30 秒 | 60 秒 | 300 秒 |
| Ready Reconcile Interval | 5 秒 | 10 秒 | 30 秒 |
| Heartbeat / Lease / Lost | 2 / 10 / 12 秒 | 5 / 20 / 25 秒 | 15 / 60 / 75 秒 |
| Recovery Scanner | 2 秒 | 2 秒 | 10 秒 |
| Recovery Backoff | 1 / 2 / 4 秒 | 2 / 5 / 10 秒 | 5 / 15 / 45 秒 |
| Outbox Batch / Publish Concurrency | 20 / 2 | 50 / 4 | 100 / 8 |
| Inbox、Published Outbox Retention | 24 小时 | 3 天 | 7 天 |
| MySQL Pool Min / Max（每实例） | 1 / 5 | 2 / 10 | 5 / 20 |

所有 Profile 的 Global/Pool Dispatch Window 均按健康槽位乘 `1.2` 动态计算，`max_recoveries=3`，并继续满足 10.1 的硬约束。`local` 和 `test` 的较短 Lease/Scanner 只用于降低开发和故障测试等待时间，不改变 Lost、Fencing 或 Retry 语义。

Cache TTL 覆盖：

| 数据 | `local` | `test` | `production-default` |
|---|---:|---:|---:|
| Execution Snapshot | 1 小时 | 6 小时 | 24 小时 |
| Active Run Context | 30 分钟 | 2 小时 | 6 小时 |
| Terminal Run Context | 10 分钟 | 30 分钟 | 1 小时 |
| Active Run Read Model | 5 分钟 | 10 分钟 | 15 分钟 |
| Terminal Run Read Model | 10 分钟 | 30 分钟 | 1 小时 |
| Definition Negative Cache | 10 秒 | 15 秒 | 30 秒 |

`local` 允许 Scheduling Store 与 Cache Store 共用一个 Redis Endpoint；共用时必须整体采用 `noeviction`，Cache 仅依靠 TTL 清理，不能使用会淘汰调度 Key 的 `allkeys-lfu`。`test` 默认使用两个独立 Endpoint，以验证与生产一致的故障边界。

### 10.9 压测调优触发条件

以下指标触发调整，而不是凭节点数量预估：

- Kafka Consumer Lag 持续增长：先检查 Worker 执行槽和下游依赖，再评估 Consumer/Partition；
- Ready-to-Queued P95 超过 `1 秒`：检查 Redis/数据库 CAS 后，再提高 Scheduler 实例数或 Admission Concurrency；
- Scheduling Redis CPU 持续超过 `60%`：检查 Hot Lane，并评估分片或 Cluster；
- Execution Context Cache Hit Rate 低于 `80%`：检查 TTL、访问分布与容量；
- MySQL Pool Wait P95 超过 `100 ms`：检查慢事务和实例总连接数，不能直接无限增大 Pool；
- 最老未发布 Outbox 超过 `30 秒`：检查数据库 Claim、Kafka 生产和 Relay 容量；
- Task Topic 积压显著超过有界 Dispatch Window：视为准入或记账缺陷，而不是正常深队列。

第 10 节中的 Kafka Partition、Worker Lease/Heartbeat、Outbox/Inbox Polling 和数据库连接池参数仍可作为容量起点；其中 Redis Lane/Credit、每秒 Epoch、Slot Dispatch Window 已废止。具体内存水位、批量大小和时间间隔仍需要压测校准，不是领域常量。

## 11. Engine、Scheduler、Kafka、Worker 与 Cache 当前高并发实现

> 历史实现说明：本节所述代码已经通过自动化正确性验收，但其中 11.4～11.8 的独立 Scheduler、Project Load、Topic Queue Window、Reservation/Inflight 和 Scheduling Redis 设计已被第 12 节废止。11.1～11.3 的 Engine 有界并发与集合式 SQL、Outbox 批量发布、Worker Claim/Lease/Fencing、Execution Context Gateway 和 Cache Redis 边界继续有效。

> 状态：已实现并通过自动化正确性验收；生产容量结论仍待目标环境实测
>
> 日期：2026-08-27
>
> 说明：本节记录 2026-08-27 冻结并已进入代码的实现设计。第 1～10 节中的冲突调度规则属于历史背景，不得重新引入。Scheduling Redis 的当前内存策略按第 11.7 节执行。

### 11.1 Engine 的角色与主要容量问题

Engine 继续是 Workflow Run 与 Node Run 语义状态的唯一推进者。它消费上游完成、失败、Lost、Retry Due、Cancel 和 Deadline 等运行事件，依据不可变 Snapshot 与 MySQL 权威状态计算拓扑，再推进下游 Node Ready、Node Retry 或 Workflow 终态。

工作流节点数不超过 20 时，单次拓扑计算不是主要瓶颈。本轮优化主要解决大量不同 Run 同时返回结果时，单条 Kafka Delivery、单个 Run 事务串行处理以及逐 Node SQL 往返造成的吞吐上限。同一 Run 多分支同时完成可以合并处理，但它是次要优化，不作为容量方案的主要依据。

### 11.2 Engine 的有界跨 Run 并发

当前处理链路为：

```text
Kafka 批量拉取 Runtime Event
→ 校验并按 run_id 分组
→ 放入有界 Engine 待处理队列
→ 不同 Run 通过 Engine 工作池并发处理
→ 同一 Run 始终串行
→ 每个 Run 使用独立 MySQL 事务
→ 事务成功后提交对应 Kafka 消费进度
```

必须满足：

- 不同 Run 不合并到一个数据库事务，避免锁、回滚和失败相互牵连；
- 同一 Run 通过 `run_id` 队列、数据库 Run 行锁和 State Version CAS 串行化；
- 一个 Run 事务只恢复一次 Run、Node、相关 Attempt 与 Snapshot；若同一批次包含该 Run 的多个事件，则按顺序应用后统一计算最终差异；
- Inbox 接受记录与 Run/Node 状态变化仍在同一事务中，提交后崩溃导致的 Kafka 重放由 Inbox/CAS 收敛；
- Node 初始化和变化持久化改成集合式 Insert/Update，并校验 `RETURNING` 行数；不再按变化 Node 逐条往返数据库；
- Engine 队列、批量大小和并发数都必须有上限。超过即时处理能力的事件保留在 Kafka Lag 中，不在 Control Plane 内无限创建等待数据库连接的 Goroutine。

第一版可以在一个 Kafka 拉取批次内等待全部 Run 分组完成后再提交 Offset。若某一 Run 失败，已经提交数据库的其他 Run 会被重放，但其 Inbox 记录使重放没有重复业务效果。只有容量证据表明批次屏障造成明显队头阻塞时，才增加按 Partition 连续 Offset 的并发提交器。

### 11.3 Engine 的 MySQL 并发预算

Engine 并发度不是 Kafka 批量大小，也不是 MySQL `pool_max`，而是数据库总预算中显式分配给 Engine 的短事务并发数。

继续满足：

```text
全部 Control Plane Pool Max 之和
<= MySQL max_connections × 70%
```

每个 Control Plane 在共享连接池之前增加模块级并发限制，至少为 Engine 保留单独的 `engine_max_inflight` 配置，防止 Engine、API、Scheduler、Outbox消息发布器和 Recovery 互相耗尽连接。初始值只能是压测起点，不能作为领域常量。

Engine 并发预算的估算式为：

```text
所需 Engine 总并发
≈ 目标 Runtime Event 处理速率
× Engine 单 Run 事务平均耗时（秒）
× 突发余量
```

若所需并发超过数据库预算，应优先降低事务往返和持锁时间、增加 Control Plane 副本或让 Kafka 吸收突发，不能直接无限增大连接池。调优以 MySQL Pool Acquire、Engine 事务延迟、数据库 CPU/WAL/IO、行锁等待和 Kafka Runtime Lag 的吞吐拐点为准。

### 11.4 Scheduler 的四条常态链路与按需重建

Scheduler 不再用一个每秒 Epoch 同时承担全量快照、Redis 重建、容量计算、公平分配和节点准入。当前实现拆成四条常态链路，并仅在必要时执行全量重建：

| 工作 | 含义 | 触发方式 |
|---|---|---|
| Ready 登记 | Engine 提交后把新 Ready Node 登记为 Redis 热候选 | 状态变化触发 |
| 持续准入 | 只要对应 Task Topic 仍有排队空位，就从 Redis 选择候选并尝试 `ready → queued` | 事件驱动，空闲时短退避 |
| Topic 容量校准 | 根据对应 Worker 最近实际完成速率更新未来约 5 秒的 Kafka 目标排队量 | 默认每 5 秒一次，可按压测调整 |
| 权威 Generation 重建 | 从 MySQL 的有界最老 Ready 批次和完整 Queued/Running 在途事实建立新 Generation，修复漏写、过期候选和计数漂移 | 启动、Redis 丢失，以及可配置低频周期；当前 Profile 为 15～30 秒 |

持续准入只使用最近一次已经计算出的 Topic Queue Window，不重新扫描全部 MySQL，也不等待下一次容量校准。Worker 成功 Claim 一个 Task 后立即释放一个 Topic 排队空位并触发可丢失的准入唤醒；唤醒丢失时由短退避轮询兜底。Worker停止消费后不会继续释放空位，因此即使最近完成速率尚未衰减，进入Kafka的Task数量也不会无限增长。

容量上升可以平滑等待下一次校准；完成速率下降或Worker故障时，已有排队量可能暂时超过新窗口，但Scheduler不抢占Queued/Running Attempt，只暂停该Topic的新Reservation，直到Claim或权威校准使占用回落。Worker Heartbeat继续用于运行健康和能力校验，但Worker Slot数量不再进入Scheduler的Topic Queue Window公式；Running并发由Worker本地Slot约束。

### 11.5 一秒时间桶 FIFO 与同桶 Project Load

#### 11.5.1 适用负载假设

目标调度策略依据EvalFrog第一阶段的真实负载，而不是按单Project瞬间产生数万Ready Node的极端模型设计：

- 单个Workflow图通常不超过20个拓扑Node；
- 批处理Node一次最多形成或处理约100个同批执行单元，当前批完成后才继续产生下一批；
- 一个Project可以在固定时间内并发调用多次Workflow Run，但Ready Node随各Run推进逐批涌现，不会把全部后继Node一次性展开；
- Kafka只保留对应Worker未来约5秒能够消化的Task，其余Node继续处于MySQL权威Ready状态。

这些是容量模型和调度取舍的显式假设，不是数据库正确性约束。生产容量测试必须记录单Project Ready批量分布和最老Ready等待时间；若实际流量长期偏离该模型，应先以证据重新评估排序策略，而不是预先增加Project轮转、老化队列或复杂配额。

#### 11.5.2 排序规则

Scheduler按Builtin与Sandbox Task Topic分别选择当前可准入候选，避免一个Topic没有空位时阻塞另一个Topic。两个Topic共享同一个跨资源类型Project Load，但不共享排队容量。

`ready_at`使用Node真正进入Ready状态时的MySQL事务时间；Engine一次批量推进产生的Ready Node使用同一事务时间，Retry重新进入Ready时使用新的时间。Redis重建继续使用MySQL保存的原始值，不使用Control Plane实例本地时钟重新生成。

当前实现把`ready_at`截断为一秒UTC时间桶：

```text
ready_bucket
= floor(ready_at_unix_ms / 1000)
```

一秒只是排序粒度，不是调度周期。Scheduler仍在Topic出现空位时持续准入，不等待当前一秒结束，也不每秒重建调度数据。100毫秒会让网络和事务抖动主导排序，5秒又会让后来Task在整个Kafka缓冲周期内参与重排；一秒把同一固定时刻的调用视为同批，同时把跨批重排限制在一秒内。

完整选择顺序冻结为：

```text
1. candidate.resource_class 对应的 Topic Queue Window 仍有空位
2. ready_bucket ASC
3. Project Load ASC
4. project_last_granted_seq ASC
5. 同一Project、同一时间桶内 priority DESC
6. ready_at ASC
7. node_run_id ASC
```

Project Load定义为：

```text
Project Load[project_id]
= 该Project跨Builtin与Sandbox的Reservation + Queued + Running
```

时间桶是第一排序条件。后来时间桶中的低Load Project不能越过更早时间桶中的Ready Node；Project Load只在同一秒内打散并发突发。Load相同时使用最近一次成功准入序号轮转，避免固定Project ID或字典序形成长期偏置。每次成功Reservation后立即增加Project Load并更新`project_last_granted_seq`，下一次选择重新比较，不向一个Project预先授予一轮持久Credit。

Project内部原有Priority仍保留，但只在同一时间桶内生效；高Priority的新时间桶不能越过更早时间桶。过去的时间桶结束后不会再有正常新候选加入，因此第一版不增加Project Aging、Starvation Threshold或Project Round-Robin。长期输入速率超过处理速率时，FIFO的最老Ready时间会持续增长，这是容量不足而不是通过重新排序能够消除的问题。

### 11.6 五秒 Kafka Topic Queue Window 与 Reservation

Builtin与Sandbox分别根据对应Worker最近实际完成速率计算未来约5秒的目标Kafka排队量，不使用Worker Pool大小或健康Slot数分配公平额度。完成速率只统计由Worker实际执行后从Running进入业务终态的Attempt，不使用Kafka Publish/Claim的瞬时速率，也不把Reaper产生的Lost当作有效吞吐：

```text
Topic Queue Window[resource_class]
= clamp(
    最近实际完成速率EWMA[resource_class] × 5秒,
    最小排队量[resource_class],
    最大排队量[resource_class]
  )

Topic Queue Occupancy[resource_class]
= 未确认Reservation + Queued Attempt

Topic Available[resource_class]
= max(0, Topic Queue Window - Topic Queue Occupancy)
```

`Queued Attempt`已经覆盖提交但尚未发布的Task Outbox、已进入Kafka但尚未Claim的Task；Redis中的未确认Reservation覆盖从选择候选到MySQL Dispatch提交的短窗口。Running不计入Topic Queue Occupancy，因为Task已经被Worker领取，运行并发由Worker本地Slot限制；但Running继续计入Project Load。

Kafka Lag只用于观测、异常诊断和校准，不作为每次Redis Reserve的强一致输入。Topic Queue Occupancy必须以MySQL Attempt状态为权威进行低频校准，不能仅依赖Kafka Offset，因为Outbox中尚未发布的Task同样已经占用准入位置。

Reservation是Scheduling Redis中的调度预占记录，不是MySQL权威Attempt。它解决多Scheduler同时选择同一Ready Node，以及Redis选择到MySQL提交之间对Project Load和Topic空位的重复占用：

```text
Redis选择当前Topic中最早时间桶的Candidate
→ 同桶内按Project Load与公平序号选择Project
→ 原子移出Candidate、Project Load +1、Topic Queue Occupancy +1
→ 建立带短TTL的Reservation
→ MySQL CAS 执行 ready → queued
→ 同事务创建 Attempt 与 Task Outbox
→ 成功则把Reservation确认成紧凑Inflight
→ 失败则Abort，恢复Candidate并回退两个计数
```

生命周期冻结为：

- 未确认 Reservation：仅覆盖 Redis Reserve 到 MySQL Dispatch 提交的短窗口，使用短过期时间；
- 已确认 Inflight：MySQL提交后不再保存完整Candidate，只保留`attempt_id/project_id/resource_class/queue_occupied`等幂等释放必需字段；
- Worker通过Attempt Coordinator成功执行`queued → running`后，提交后尽力把`queue_occupied`从1改为0并递减Topic Queue Occupancy；Project Load不变；
- Attempt进入Succeeded、Failed、TimedOut、Canceled或Lost等终态后，提交后尽力删除Inflight并递减Project Load；若终态发生时`queue_occupied`仍为1，则同一次幂等释放还要递减Topic Queue Occupancy；
- Redis更新失败只会造成暂时少派发，权威校准依据MySQL Queued/Running/终态事实修复，不得提前释放导致超量准入。

### 11.7 Scheduling Redis 内存与生命周期策略

目标策略冻结为：Scheduling Redis 统一使用 `noeviction`，禁止 Redis 在内存压力下依据 LRU/LFU/TTL 随机提前删除调度协调状态；系统仅通过业务生命周期、语义过期时间和显式删除主动回收内存。MySQL 仍是 Ready、Queued、Running 与 Attempt 的唯一权威。

第一版不为 Ready Candidate 设置全局、Lane 或单 Project 业务配额，也不建设冷热候选分层。`redis_candidate_batch` 只约束单次 MySQL 权威读取和单次 Generation 重建的工作量，不限制 Engine 提交后持续登记形成的热队列。高并发流量超过 Redis 当前可承载内存时，通过停止新增候选并排空已有候选控制增长，不提前引入复杂的候选裁剪算法。

#### 11.7.1 最小必要状态

当前实现只维护确实参与公平计算、容量约束、幂等释放或恢复的数据：

| 状态 | 形式 | 用途 |
|---|---|---|
| Ready Candidate | 按Resource Class、`ready_bucket`和Project组织的有序候选 | 先选择最早一秒时间桶，再进入同桶Project比较 |
| Active Bucket/Project Index | 每个Task Topic当前仍有Ready需求的时间桶和Project索引 | 避免每次准入扫描全部Candidate |
| Project Load | `project_id → reservation + queued + running` | 仅在同一时间桶内比较Project当前已准入负载 |
| Project Last Granted Sequence | `project_id → 最近成功准入序号` | Load相同时轮转，避免固定Project顺序偏置 |
| Topic Queue Window/Occupancy | `resource_class → window, reservation + queued` | 限制Builtin/Sandbox各自进入Kafka的约5秒排队深度 |
| Reservation | `attempt_id → 完整 Candidate + 派发身份` | 覆盖 Redis Reserve 到 MySQL Dispatch 提交的短窗口 |
| Inflight | `attempt_id → project_id + resource_class + queue_occupied` | Claim时释放Topic占用、终态时释放Project Load，并支持权威校准 |
| Meta | Generation、Paused、Lease、Fencing | 控制重建、故障暂停和旧协调者失效 |

当前实现已删除Pool Load、Global/Pool Dispatch Window、Project Credit、Pool Credit、Grant和每秒公平Epoch。Worker Slot不进入Scheduler容量公式，Resource Class也不形成第二套项目公平身份。第一版使用专用的单逻辑分片Scheduling Redis完成同槽Lua原子Reserve；只有容量测试证明单分片成为瓶颈时，才设计跨分片Permit或Lane协调协议。

Reserve热路径只允许原子维护三类事实：Candidate生命周期、Project Load/Last Granted和目标Topic Queue Occupancy。MySQL Dispatch CAS仍是Candidate有效性的最终裁决，Redis排序与计数只负责加速、避免重复预占和限制Kafka排队深度。

Redis丢失后的全量重建从MySQL恢复Ready Candidate、`Queued + Running`形成的Project Load，以及Queued形成的Topic Queue Occupancy；不存在MySQL事实的短Reservation按失效处理。`project_last_granted_seq`不是权威业务状态，重建时可以统一重置，只会在一个时间桶内产生短暂平局误差，不影响FIFO、Attempt唯一性或容量上限。

#### 11.7.2 数据生命周期

| 数据 | 主动删除时机 |
|---|---|
| Ready Candidate | Reserve成功、MySQL Dispatch CAS确认已失效、Run取消/终结，或权威校准确认Node不再Ready |
| Bucket/Project索引 | 对应时间桶或Project已无Ready Candidate |
| 未确认Reservation | MySQL派发失败后Abort，或短语义超时到期 |
| Inflight的`queue_occupied` | Worker Claim事务成功后置0；若Redis更新失败则由权威校准修复 |
| 已确认Inflight | MySQL Attempt进入Succeeded、Failed、TimedOut、Canceled或Lost等终态后删除 |
| Project Load字段 | 计数幂等减为零且该Project已无Ready、Reservation和Inflight |
| Project Last Granted字段 | Project已无Ready、Reservation和Inflight且Load为零 |
| Topic Queue Window | 下一次容量校准原位覆盖，不创建按Epoch累积的Key |
| 旧Generation | 新Generation完整建立并原子切换后 |

Task进入Kafka不释放Topic Queue Occupancy，因为它仍在等待Worker；Worker Claim成功并把Attempt从Queued改为Running后释放Topic占用，但不释放Project Load。Attempt Coordinator或Reaper提交Attempt终态后删除Inflight并递减Project Load。所有释放都以首次成功改变紧凑Inflight中的对应状态为准，重复Claim、重复Completion和权威校准不得把计数减为负数。

#### 11.7.3 内存水位与排空

生产部署必须显式设置 Redis `maxmemory` 和容器/Pod 内存限制，并保证 `maxmemory` 低于进程内存限制。应用只配置两个可调水位：`scheduling_memory_high_watermark` 与更低的 `scheduling_memory_resume_watermark`，具体数值通过容量测试确定。

```text
Normal
  used_memory < high_watermark
  → 允许提交后Ready登记、MySQL候选补回和正常准入

Draining
  used_memory >= high_watermark
  → 停止所有会扩大数据量的Ready登记、候选补回和全量重建
  → 继续消费已有Ready、确认/撤销Reservation、处理Claim占用释放、释放终态Inflight和执行过期清理
  → 新Ready Node只保留在MySQL权威状态

Resume
  used_memory <= resume_watermark
  → 恢复Ready登记
  → 通过MySQL权威校准补回排空期间遗漏的Ready Node
```

这里“停止写入”特指停止会扩大Redis数据量的写入。若同时禁止Reservation确认、Abort、终态释放和删除，系统反而无法处理已有数据并回收内存。高水位必须低于`maxmemory`并预留完成这些内存中性或内存递减操作的空间。

若Redis已经返回OOM或无法安全建立新Reservation，Scheduler立即Fail Closed，暂停新准入，只执行可验证的生命周期清理；降到Resume水位并完成一次权威校准后再开放。Ready登记失败不回滚Engine事务，因为遗漏候选仍可从MySQL恢复。

### 11.8 Kafka 的目标职责与批量发布

Kafka 的目标不是保存无限深的业务积压，也不是替代 Scheduler 的排序，而是保存 Worker 未来若干秒能够消化的公共任务缓冲，并作为 Scheduler、Worker 与 Engine 之间可重放、可水平扩展的异步边界。超过5秒Topic窗口的需求继续保持为MySQL权威Ready事实和Redis热索引，不转移到Control Plane进程内存、Worker本地深队列或大量等待数据库连接的Goroutine。

Builtin/Sandbox各自的目标Kafka排队量按第11.6节的最近实际完成速率与5秒缓冲时间计算。Scheduler热路径不为每次准入同步查询Kafka Lag；Topic Queue Occupancy覆盖尚未发布的Task Outbox、已进入Kafka但未Claim的Queued Attempt和临时Reservation，Running不再占用Kafka排队窗口。Kafka Lag用于容量观测、异常诊断和权威校准，不作为Redis Lua准入的强一致输入。

Task链路目标为：

```text
Scheduler MySQL事务
  ready → queued + Attempt + Task Outbox
→ 任务消息发布器批量Claim Task Outbox
→ Kafka Producer批量发送到Builtin/Sandbox Task Topic
→ MySQL批量Mark成功记录为Published
→ 发布失败记录释放Claim并退避重试
```

运行事件链路目标为：

```text
Run创建、Attempt完成、Lost、Retry、Cancel或Deadline事务
→ 同事务写Runtime Outbox
→ 运行事件发布器批量Claim Runtime Outbox
→ Kafka Producer批量发送Runtime Event
→ MySQL批量Mark成功记录为Published
→ Engine批量消费并按run_id处理
```

批量Publish发生部分成功时，只批量Mark Kafka确认成功且Claim Token仍匹配的记录；失败记录保留为未发布并可重试。Publisher在Publish成功后、Mark前崩溃会造成至少一次重复发布，由Worker Claim幂等或Engine Inbox/CAS收敛，不能要求Kafka事务替代MySQL Outbox。

Worker本地等待队列保持有界，只在本地有空闲Slot时领取Task；不把Kafka中的公共积压深度预取到各Worker。Claim成功后立即ACK Kafka，ACK不等待Executor完成；这使Kafka Consumer Group可以正常Rebalance，而长任务可靠性由MySQL Lease、Heartbeat、Reaper和Fencing承担。

Attempt Coordinator提交`queued → running`后，提交后尽力通知Scheduling Redis释放该Attempt的Topic Queue Occupancy并唤醒持续准入；Worker不直接访问Redis。该Redis更新失败不影响Claim响应和Kafka ACK，只会暂时少派发，并由下一次MySQL权威校准修复。Attempt仍保留紧凑Inflight直到终态，用于最终释放Project Load。

### 11.9 Worker 调用边界与完成闭环

Execution Context Gateway和Attempt Coordinator是同一个Control Plane部署单元内的两个逻辑模块，不是Worker必须串行穿越的两个独立服务。Worker调用顺序冻结为：

```text
Kafka Task
→ Attempt Coordinator：ClaimAttempt
→ Execution Context Gateway：LoadExecutionContext
→ Executor执行
→ Attempt Coordinator：CompleteAttempt
```

Gateway只在执行前读取一次不可变Snapshot、Run Input、已接受的上游Effective Output和授权资源；执行完成后Worker不再调用Gateway，而是直接调用Attempt Coordinator。第一版不增加`StartAttempt`聚合接口，也不把Claim与Context装配合成新的领域组件；只有容量测试证明执行前一次额外HTTP往返构成显著瓶颈时，才允许在Worker API Adapter中把两步编排成一次网络请求，Coordinator与Gateway的职责仍保持分离。

Worker不得直接写MySQL、Scheduling Redis或Cache Redis，也不得直接向Runtime Event Topic发布完成事件。执行完成后的目标链路为：

```text
Worker CompleteAttempt
→ Attempt Coordinator短事务：
   校验Current Attempt、Attempt Sequence、Lease和Fencing
   更新Attempt终态
   成功时插入不可变Output Candidate
   插入Completion Runtime Outbox
→ MySQL提交后立即响应Worker
→ 提交后尽力删除Scheduling Redis Inflight并释放Project Load
   若Inflight仍标记queue_occupied，则同时释放Topic Queue Occupancy
→ 运行事件发布器批量发布Kafka
→ Engine批量消费，按run_id分组
→ 不同Run有界并发，同一Run串行
→ Engine接受Current Attempt，设置effective_attempt_id
→ 批量推进下游Ready或Workflow终态
→ 提交后预热Effective Output并登记新Ready Candidate
```

Worker响应只等待Attempt、Output Candidate和Runtime Outbox的MySQL事务提交，不等待Kafka发布或Engine推进。Output继续使用当前受请求大小限制的MySQL JSON存储；第一版不新增对象存储、结果Kafka Topic或Worker批量Completion事务。

生产Worker API必须使用mTLS或等价的服务身份认证，并把认证身份绑定到Worker ID、Resource Class与Lease Owner；不能信任请求Body自报Worker身份。Gateway和Coordinator继续逐Attempt校验Project、Run、Attempt Sequence、Lease Token、Fencing Token、Running状态和Lease有效期。Builtin/Sandbox Worker仍不持有Workflow MySQL或Redis凭证。

### 11.10 Run 创建事件来源

Engine不接受未经鉴权的上游Kafka命令，也不负责创建最初的权威Workflow Run行。无论CLI、Web还是企业内部评测调度器，Workflow启动链路均为：

```text
调用External CreateRun API
→ MySQL事务：
   校验Project权限、Workflow发布状态或Draft Test Snapshot
   绑定不可变snapshot_id与execution_identity_id
   校验幂等键
   插入Pending Workflow Run
   插入run.created Runtime Outbox
→ 运行事件发布器批量发布Kafka Runtime Event
→ Engine消费run.created
→ 原子初始化Node Run并把Run推进为Running或合法终态
```

因此大量Workflow开始事件最终与Attempt完成事件一样通过Kafka进入Engine，但上游不能绕过External API和MySQL直接生产`run.created`。Kafka消息只是可重放Wake-up；MySQL中的Pending Run、Snapshot绑定、权限结果和幂等记录才是权威事实。

第一版继续使用单Run CreateRun契约并通过API和Control Plane水平扩展承接并发，不提前增加Bulk CreateRun领域接口。若容量测试证明命令接收本身成为瓶颈，后续可以增加批量Transport接口与集合式数据库写入，但每个Run仍必须独立校验、独立幂等，并在同一数据库事务中写Pending Run与对应RunCreated Outbox。

### 11.11 Cache Redis 主动预热与一致性

Cache-Aside 继续作为Redis失败时的MySQL回源语义，同时增加权威提交后的主动预热：

| 数据 | 主动预热时机 | 一致性依据 |
|---|---|---|
| Execution Snapshot DSL | Draft Test Snapshot落库，或Publish写入/复用Snapshot并提交后 | `snapshot_id`对应内容不可变 |
| Run Input | CreateRun提交后 | 单个Run的Input不可变 |
| Effective Output | Engine设置`effective_attempt_id`并将Node提交为Succeeded后 | 单个Node Run只有一个有效Attempt和一个最终Output |
| Run Read Model | 读取未命中后回填；状态变化后提交后删除旧缓存 | MySQL运行表始终是权威投影来源 |

主动预热严格发生在MySQL提交之后，不在数据库事务中写Redis。Redis写入失败不回滚业务事务；后续读取按Cache-Aside回源并重新填充。

Effective Output缓存不增加额外防御性Envelope。缓存直接以 `run_id + execution_node_id` 定位最终JSON值，并依赖以下冻结不变量保证正确性：

- Attempt Coordinator只能写Output Candidate，不能写Effective Output缓存；
- 只有Engine接受Current Attempt并在MySQL提交`effective_attempt_id`后才能预热；
- 一个Node Run一旦Succeeded，其Effective Attempt和Effective Output不再变化；
- 预热失败、缓存淘汰、Redis不可用或JSON损坏均按Miss处理并回源MySQL；
- 重复Completion或并发Miss最多重复写入同一个最终值，不产生多个有效版本；
- Workflow Replay使用新的`run_id`，不会复用旧Run的Output缓存。

Execution Context读取应使用进程内已解析Snapshot缓存作为L1、Cache Redis作为L2，并通过一次Redis多字段读取获得Run Input与所需上游Output。部分Miss时使用一次MySQL批量查询补齐，避免逐上游Node执行Redis和MySQL N+1读取。Worker仍先通过MySQL权威Claim/Lease/Fencing边界取得执行资格，缓存不能替代Attempt有效性判断。

Run Read Model是面向Web/CLI的只读运行视图，聚合Run、Node、Output、Failure和Source Map位置；它不参与Engine、Scheduler、Worker Context或Attempt状态裁决。Run/Node事务提交后删除旧视图并发送可丢失Wake-up，客户端重新GET权威投影。

### 11.12 目标术语

后续设计与代码说明优先使用以下中文名称：

| 名称 | 含义 |
|---|---|
| 提交后批量登记Ready节点 | MySQL提交成功后，尽力把一批Ready候选写入Scheduling Redis；失败由权威校准补回 |
| 一秒Ready时间桶 | 以MySQL `ready_at`的UTC秒为FIFO主顺序；它是排序粒度，不是调度周期 |
| 同桶Project Load | 只在相同Ready时间桶内比较Project的`reservation + queued + running`，不允许后来时间桶越过旧桶 |
| Topic Queue Window | 按Builtin/Sandbox最近实际完成速率分别计算未来约5秒的Kafka排队量，不包含Running和Worker Slot |
| 持续准入并批量预占 | Topic有空位时持续按时间桶、Project Load和稳定平局顺序选择少量Candidate并建立Reservation |
| Topic容量校准 | 默认每5秒更新完成速率EWMA、Topic Queue Window和权威Queued占用，不重建Ready数据 |
| 排空模式 | Scheduling Redis达到高水位后停止新增候选，继续处理已有调度状态和生命周期删除，降到恢复水位后再从MySQL补回 |
| 任务消息发布器 | 当前代码中的Task Relay；从MySQL Task Outbox领取待发布记录，发送到对应Kafka Task Topic，再把成功记录标记为已发布 |
| 运行事件发布器 | 当前代码中的Runtime Relay；从MySQL Runtime Outbox领取待发布事件，发送到Kafka Runtime Event Topic，再把成功记录标记为已发布 |
| Execution Context Gateway | 执行上下文读取器；只在Worker执行前读取和装配经过Attempt授权的执行数据，不参与Completion和状态推进 |
| 主动预热 | MySQL权威提交后尽力提前写Cache Redis，失败时仍可按Cache-Aside回源 |

### 11.13 实施验收要求

本轮实现验收至少需要证明：

- 1000个不同Run同时产生Runtime Event时，Engine并发不超过配置预算，队列有界且不存在永久卡住；
- 同一Run仍串行，不同Run不共享事务，Inbox/CAS在批量拉取与重放下保持一次业务效果；
- Node初始化与推进使用集合式SQL，变化行数不匹配时整个Run事务回滚；
- 容量校准使用最近实际完成速率形成约5秒Topic Queue Window；持续准入在Worker Claim释放Topic空位后及时补充Kafka缓冲，不等待下一次校准；
- 每个Resource Class都满足`Topic Queue Occupancy = Reservation + Queued`且不超过当前Window；Worker Claim只释放Topic占用，Attempt终态只释放Project Load，重复或漏失回调可由MySQL权威校准收敛且计数不为负；
- 同一Resource Class中更早一秒时间桶的Ready Node不被后来时间桶越过；同桶内Project Load较低者优先，Load相同时按最近准入序号轮转；Builtin/Sandbox任一Topic无空位不阻塞另一个Topic；
- 模拟单Project多Workflow并发调用、每批最多100个Ready Node且后继批逐步涌现时，FIFO不会产生永久饥饿，最老Ready等待时间能够在可持续负载下回落；
- Task与Runtime Outbox能够批量Publish和批量Mark；部分发布成功、Publisher崩溃重放和Kafka短时不可用均不丢失Task/Event，也不产生重复业务效果；
- Kafka缓冲能够吸收固定时间突发并保持在按完成速率计算的有界秒数内，Worker本地队列不形成第二个深积压；
- Worker执行完成后直接调用Attempt Coordinator，不经过Gateway；Coordinator事务提交后即可响应，Kafka和Engine异步完成后续推进；
- Worker API服务身份、Lease Owner、Resource Class和Fencing校验阻止伪造Worker、跨Project Context读取与迟到Result；
- 并发CreateRun均先形成Pending Run与RunCreated Outbox，再由Kafka唤醒Engine；重复请求、重复事件和Engine重放不重复初始化Run；
- Cache主动预热失败、Cache Redis全量清空和并发Miss均能从MySQL恢复，Candidate Output永不进入Effective Output缓存；
- Scheduling Redis达到高水位后不再增长Ready候选，已有候选仍能被派发并释放内存；降到恢复水位后能够从MySQL补回期间遗漏的Ready Node；
- Scheduling Redis OOM时保持Fail Closed，不发生自动Key淘汰、重复Attempt、Project Load或Topic Queue Occupancy负数，也不新增突破Topic Queue Window的Reservation；
- 容量报告同时记录Engine吞吐与事务延迟、MySQL Pool Acquire、Kafka缓冲秒数、最老Ready等待时间、Ready-to-Queued、同桶Project分布和两类Redis命中/内存指标。

## 12. 最新冻结目标：Engine 直接形成执行意图，删除 Scheduler 与 Scheduling Redis

> 决策状态：已确认；代码迁移与本地自动化验收已完成
>
> 决策日期：2026-08-27
>
> 取代范围：第 1～11 节中所有独立 Scheduler、公平准入、Ready Redis 索引、Topic Queue Window、Project Load、Reservation/Inflight、Scheduling Redis Generation/Rebuild/OOM 排空规则

### 12.1 决策背景与取舍

EvalFrog 第一阶段用于企业内部日常评测：一个 Workflow 通常不超过 20 个拓扑 Node，Ready Task 随 Run 推进逐步产生；系统需要承受固定时间内大量 Workflow 调用，但不再要求大、小 Project 之间的公平份额，也不要求 Kafka 只保留 Worker 未来约 5 秒的任务。正常执行语义采用先产生、先进入公共任务队列，Worker 不足表现为 Outbox/Kafka 积压并通过扩容解决。

在这些约束下，独立 Scheduler 的一秒时间桶、Project Load、Topic Queue Window、Reservation、Inflight 与 Scheduling Redis 只是在 MySQL Ready 和 Kafka 之间增加第二次状态推进、额外事务和可重建协调状态。删除它们可以把执行意图的创建合并回已经拥有 Workflow/Node 状态机的 Engine，同时保留 MySQL 权威、Outbox 原子性、Kafka 异步边界和 Worker Lease/Fencing，不牺牲业务正确性。

本决策明确接受以下取舍：

- Kafka 不再承诺约 5 秒的有界排队深度；当输入长期超过 Worker 能力时，积压会增长；
- 不提供 Project 公平、优先级重排或全局严格 FIFO；
- Kafka 只保证单 Partition 有序，多 Partition/多 Worker 下是近似先到先消费，Task 完成顺序不受保证；
- 取消、Deadline 或旧 Attempt 对应的已发布 Task 可能仍被 Worker 领取，但 Claim 必须依据 MySQL 当前事实拒绝并安全 ACK；
- 容量不足通过 Outbox Age、Kafka Lag/最老消息年龄、Queued-to-Running 延迟、Worker Slot 利用率和存储水位暴露，而不是由 Scheduler 估算 Worker 完成速率。

### 12.2 Engine 的新事务边界

Engine 继续是 Run/Node 语义状态的唯一推进者，并新增“为刚满足执行条件的 Task Node 创建执行意图”的职责。它不执行 Task，也不直接写 Kafka。

目标链路为：

```text
RunCreated / AttemptCompleted / AttemptLost / RetryDue / Cancel / Deadline Runtime Event
→ Engine Inbox去重并锁定一个Run
→ 依据不可变Snapshot与MySQL权威状态计算拓扑
→ Task Node在领域状态机中满足依赖并进入Ready
→ Engine在同一领域计算中立即执行Ready → Queued
→ 同一个Run事务内集合式写入：
   Node状态变化
   新Node Attempt
   新Task Outbox
   Run状态变化
   Engine Inbox
→ MySQL提交
→ Task消息发布器异步批量发送Kafka
```

必须满足：

- Start、End、Branch等控制节点不创建Attempt；只有Task Node进入Queued时创建物理Attempt；
- 初始化根Task、上游完成后出现的下游Task、业务Retry和基础设施Recovery都遵循同一事务边界；
- 一个Run内新增的多个Attempt与Task Outbox使用集合式SQL，不能逐Node执行独立事务；
- `Node queued + Attempt + Task Outbox`必须原子提交，任何一项失败都回滚整个Run事务；
- 不把不同Run合并进一个数据库事务；不同Run仍按`engine.max_inflight`有界并发；
- 重复Runtime Event由Inbox去重，竞争推进由Run行锁、Node State Version、Current Attempt和唯一约束收敛，不能创建两个有效Attempt；
- 正常Task Node不再长期持久化为Ready并等待第二个组件推进。Ready可以继续作为领域状态机中的瞬时合法状态，但事务最终落库为Queued；
- MySQL或Task Outbox写入失败时不允许先发布Kafka，Runtime Event重放后重新计算；
- Engine提交后不再登记Scheduling Redis Ready Candidate，也不执行任何Redis调度生命周期回调。

### 12.3 为什么数据库已有 Attempt 仍需要 Kafka

MySQL中的Queued Attempt回答“这个执行意图是否权威存在、现在处于什么状态”；它不负责把任务高效交给某个Worker。Execution Context Gateway回答“一个已经通过Claim授权的Attempt执行时需要读取哪些Snapshot、Input、Effective Output和资源信息”；它也不负责发现待执行Attempt、竞争领取、公共积压、消费者扩缩容或消息重放。

Kafka继续承担不可替代的传输职责：

- 把Control Plane产生的Task异步分发到Builtin/Sandbox Worker Consumer Group；
- 在Worker短时不足、重启或滚动发布时保存公共积压并削峰；
- 通过Partition支持多个Worker并行消费和Rebalance；
- 在Worker Claim前崩溃时重新投递尚未完成责任转移的Task；
- 将任务发现流量留在Broker，避免所有Worker持续轮询MySQL或Gateway；
- 让Control Plane提交Run事务后立即释放数据库连接，不等待某个Worker在线。

因此Worker链路保持：

```text
Kafka Task
→ Worker本地有空闲Slot后领取
→ Attempt Coordinator ClaimAttempt
→ MySQL queued → running + Lease/Fencing
→ Claim成功后ACK Kafka
→ Execution Context Gateway LoadExecutionContext
→ Executor执行
→ Attempt Coordinator CompleteAttempt
```

Gateway必须继续是只读、按Attempt授权的Context读取器。若让Worker通过Gateway轮询“下一个Attempt”，Gateway就会同时承担任务队列、竞争领取、长轮询、背压和故障重放，实际上重新实现一个性能与可靠性都更弱的Broker，并把高频空轮询压回MySQL。Worker仍不得持有Workflow MySQL凭证，也不得绕过Coordinator直接改变Attempt。

### 12.4 Outbox与Kafka边界

Engine不能在数据库事务内直接调用Kafka，也不能先写Kafka再更新MySQL。目标Task链路固定为：

```text
Engine MySQL事务
  Node queued + Attempt + Task Outbox
→ Task消息发布器批量Claim Outbox
→ Kafka Producer批量发送Resource Class Topic
→ MySQL批量Mark成功记录为Published
→ 失败记录释放Claim并退避重试
```

Kafka发布成功、Outbox Mark前崩溃仍可能产生重复Task。Worker Claim以Attempt当前状态、Sequence、Lease和Fencing幂等收敛；不能为了删除重复而引入跨MySQL/Kafka分布式事务。Runtime Event继续使用Runtime Outbox、Kafka、Engine Inbox/CAS，职责不变。

### 12.5 删除与保留的组件和数据

后续代码迁移应删除：

- Scheduler Service、Scheduler领域端口和`DispatchReady`路径；
- Scheduling Redis Client及其Ready、Active、Candidate Index、Project Load、Last Granted、Topic Window/Occupancy、Completion EWMA、Reservation、Inflight、Generation、Lease/Fence数据；
- Scheduling Redis Endpoint、`noeviction/maxmemory`专用部署、配置和故障恢复手册；
- Attempt Coordinator中的Scheduling Redis `MarkClaimed/MarkTerminal`提交后回调；
- Scheduler/Topic Window/Redis Rebuild相关指标、容量Profile和测试；
- 只服务于Scheduler Ready/FIFO或Scheduling Inflight扫描的索引；删除索引必须通过新Migration完成，不能改写已发布Migration历史。

继续保留：

- MySQL Workflow/Node/Attempt/Output/Outbox/Inbox权威状态；
- Engine有界跨Run并发、同Run串行和集合式Node SQL；
- Task与Runtime Outbox批量Claim/Publish/Mark；
- Kafka Builtin Task、Sandbox Task、Runtime Event和DLQ Topic；
- Worker本地Slot背压、Attempt Claim/Heartbeat/Complete、Lease/Reaper/Fencing；
- Execution Context Gateway；
- Cache Redis，用于不可变Snapshot、Run Input、Effective Output和Run Read Model，不参与Task准入。

Worker Heartbeat若仅用于在线状态展示，可以交给部署平台/Prometheus，或作为允许TTL和淘汰的非权威数据写入Cache Redis；Worker能力完整性仍由Worker启动校验、Resource Class Topic隔离和Claim时校验保证，不能为了注册信息继续保留Scheduling Redis。

### 12.6 过载、FIFO与运行观测

删除Scheduler后，Kafka和未发布Task Outbox是正式的任务等待层。生产系统必须观测：

- Runtime与Task Outbox未发布数量、最老Age和发布错误；
- 各Task Consumer Group的Kafka Lag与最老消息年龄；
- Attempt `queued → running` P50/P95/P99；
- Worker Slot使用率、Claim失败、Lease Lost和任务完成速率；
- Kafka磁盘、Retention和Broker健康；
- MySQL Pool Acquire、Engine事务延迟、WAL/IO和锁等待。

当积压持续增长时，首选增加对应Resource Class Worker、增加Kafka Partition或排查Executor/外部依赖；若Kafka/MySQL存储接近安全水位，则在External CreateRun入口实施全局容量保护、限流或明确拒绝，不能等到存储耗尽。该保护是系统过载边界，不重新引入Project公平Scheduler。

Task使用`attempt_id`作为Partition Key以分散负载，因此只承诺单Partition顺序和Topic层面的近似先到先消费。Workflow依赖顺序由Engine只在上游Effective完成后创建下游Attempt保证，不依赖Kafka全局顺序。

### 12.7 兼容迁移顺序

为避免滚动发布期间旧Engine产生的Ready Node永久停留，代码迁移按以下顺序执行：

1. 新版本Engine先支持在同一Run事务中直接创建Attempt与Task Outbox，但暂时保留旧Scheduler用于处理旧实例或升级前遗留的Ready Node；两者竞争时仍由MySQL行锁/CAS保证只产生一个有效Attempt；
2. 全部Control Plane实例升级后，确认MySQL不存在可执行但长期停留的Ready Node，或使用一次性受控Drainer完成遗留派发；
3. 禁用Scheduler角色并观察至少一个最大任务/Retry/Recovery周期，验证Task Outbox、Kafka Lag、Queued-to-Running和Run终态正常；
4. 删除Scheduler代码、Scheduling Redis部署与生命周期回调；
5. 通过追加Migration删除仅服务于旧Scheduler的索引，再更新README、架构图、运行手册和学习文档为“已实现”。

不得先停Scheduler再滚动升级仍会持久化Ready等待调度的旧Engine，也不得通过直接修改数据库状态清理遗留Ready。

### 12.8 后续代码验收不变量

迁移完成至少需要证明：

- Run初始化、上游完成、业务Retry和Recovery都能在一个Engine事务内原子形成`Node queued + Attempt + Task Outbox`；
- 同一Run一次产生多个可运行Task时使用集合式Node、Attempt和Task Outbox SQL，任一行冲突使整个Run事务回滚；
- 1000个不同Run同时推进时Engine并发不超过连接池预算，且不存在Scheduler或Scheduling Redis依赖；
- Runtime Event重复100次、Kafka Task重复发布和多个Engine实例竞争时都只产生一个有效Attempt和一次业务推进；
- Kafka不可用时Task停留在Outbox，恢复后可发布；Worker不足时Task积压在Kafka，扩容后能够继续消费；
- Worker只在本地Slot可用时Poll，Claim成功后ACK；Claim前崩溃由Kafka重投，ACK后崩溃由Lease/Reaper恢复；
- 取消、Deadline、旧Attempt和迟到Task在Claim时被权威状态拒绝，不启动Executor且不会永久重投；
- Gateway只允许当前合法Running Attempt读取Context，不提供待执行Attempt发现或状态写入接口；
- 删除Scheduling Redis后，Cache Redis全量清空仍只触发MySQL回源，不影响Task创建、Kafka消费和Run推进；
- 容量报告以Outbox Age、Kafka Lag/最老消息、Queued-to-Running、Worker Slot和Engine/MySQL指标替代Ready-to-Queued、Topic Window、Project Load和Scheduling Redis指标；
- 部署清单只包含Control Plane、Builtin Worker、Sandbox Worker、MySQL、Kafka和Cache Redis，不残留未使用的Scheduler角色或Scheduling Redis实例。

第一阶段节点能力和安全边界详见 [02_节点模型与执行能力边界.md](./02_节点模型与执行能力边界.md)。

## 13. Kafka 单分区微批与短任务执行热路径

> 状态：第一版已实现并通过本地 Unit/Contract 验收；真实 Kafka/MySQL 集成与性能结论仍待目标环境验证
>
> 日期：2026-08-28
>
> 适用基线：第 12 节 Engine 直接派发、无 Scheduler、无 Scheduling Redis 架构

### 13.1 Consumer Group、Partition 与 Slot

`production-default` 当前参数为：

| 消息流 | Partition | Partition Key | 一个 Group Member | 单进程并发 |
|---|---:|---|---|---:|
| Builtin Task | 48 | `attempt_id` | 一个 Builtin Worker 进程中的一个 Kafka Consumer | 32 Slot |
| Sandbox Task | 24 | `attempt_id` | 一个 Sandbox Worker 进程中的一个 Kafka Consumer | 12 Slot |
| Runtime Event | 24 | `run_id` | 一个 Control Plane 进程中的一个 Kafka Consumer | 最多 12 个不同 Run |

Partition 不与某个 Worker 永久绑定，Kafka 会在 Consumer Group 重平衡时动态分配。按配置中 12 个期望 Worker 估算，Builtin Worker 平均可持有 4 个 Partition，Sandbox Worker 平均可持有 2 个 Partition；实际分配仍取决于在线副本数。Slot 是 Executor 并发额度，不是 Kafka Consumer 数。一个 Worker 仍只创建一个 Consumer，但可持有多个 Partition，并从不同 Partition 的连续微批逐步填满 Slot。

Runtime Event 当前仍存在配置语义欠账：`expected_runtime_event_consumers=24` 只是校验值，而实际每个 Control Plane 只创建一个 Consumer。该字段后续应与 Control Plane 副本数对齐或重命名；本次 Task 微批不顺带改变 Engine 消费拓扑。

### 13.2 已实现链路

```text
单 Worker Poller 查看空闲 Slot
→ 从一个 Topic/Partition 取最多 min(8, 空闲Slot) 条 Task
→ 一次 ClaimBatch HTTP
→ MySQL 一个短事务按稳定顺序 Claim，逐项返回结果
→ 对 Claim 成功项一次 ContextBatch HTTP
→ 整批已安全处置后，一次提交该 Partition 的最高 Offset
→ 释放 Kafka 重平衡门闩
→ 交给本地 Slot 并发 Execute
→ Heartbeat、Complete 继续逐 Attempt 执行
```

第一版故意不实现跨 Partition 的“部分成功、分别提交安全前缀”。每次交付只含一个 Partition；技术错误使 Claim 事务整体回滚并且整批不 ACK，因此不存在提交 Offset 越过未取得执行责任的 Task。高 Slot Worker 依靠被分配多个 Partition 获得并行来源，而不是把不同 Partition 拼成一个复杂事务。

### 13.3 Claim 事务与逐项结果

批次上限固定为 8。Repository 在一个事务内按 `(project_id, run_id, node_run_id, attempt_id)` 排序后依次取得 Node→Attempt 行锁，再将结果映射回 Kafka 输入顺序：

- `NotFound`、`NotCurrent`、`StateConflict`、`LeaseMismatch`、`CapabilityMismatch` 是单条业务处置，不导致其他有效 Attempt 回滚；
- MySQL 连接、语句、锁超时或 Commit 失败属于技术错误，整个批次回滚且不 ACK；
- 同一批出现重复 `attempt_id` 时只允许第一条参与 Claim，后续条目按重复安全跳过；
- 同一 Worker 在相邻批次再次收到同一 Attempt 时，Worker 本地 active 集合阻止它进入第二个 Slot；跨 Worker、重平衡和进程崩溃后的重复仍由 MySQL Claim/Lease/Fencing 裁决。

这里已经减少了每批的 HTTP 请求、事务 Begin/Commit 和 Kafka Commit 次数，但当前 MySQL 事务内部仍逐 Attempt 执行查询与 CAS，并非单条集合式 SQL。只有基准证明事务内 SQL 往返成为瓶颈后，才继续改成 JSON/数组输入的集合式查询；文档和简历不得提前声称“单条 SQL 批量 Claim”。

### 13.4 Context 批量边界

Worker API 已提供 `ContextBatch`，因此最多 8 个 Context 只发生一次 Worker→Control Plane HTTP 请求。Control Plane 当前仍逐项调用 Execution Context Gateway；每项继续使用 Snapshot、Run Input、Effective Output 的 Cache-Aside 与 MySQL 回源。Redis MGET、Snapshot 批内去重和 MySQL Miss 集合回源尚未实现。

Claim 已提交但 Context 读取失败时可以 ACK，因为执行责任已经转移到 MySQL Lease；该 Attempt 不作为业务失败完成，而由 Reaper 标记 Lost 后创建 Recovery Attempt。第一版不新增立即 Lost 操作。

### 13.5 为什么仍是 Claim 后 ACK

Execute 后 ACK 不减少 Claim、Context 或 Complete 开销，还会让十几秒的 LLM 节点长期占用 Kafka 重平衡门闩并阻塞同 Partition。删除 Claim 则无法阻止重复发布、ACK 不确定、重平衡或进程崩溃导致的重复外部副作用。因此责任转移点保持为：MySQL Claim 成功后才允许 ACK；批量化只摊薄固定开销，不改变 Lease/Reaper/Fencing 恢复闭环。

### 13.6 当前证据与待验收项

已完成的自动化证据：

- Kafka Adapter 只从一个 Partition 选择最多 8 条，并保留其他 Partition 的已拉取记录；
- 8 条 Task 只调用一次 ClaimBatch、一次 ContextBatch 和一次批次 ACK；
- Claim 技术错误使整批 NACK；
- Worker API 批量 Contract 往返、逐项 Lease/Context 映射通过；
- 单条接口仍保留兼容，Batch Size 为 1 时沿用旧路径。

仍需真实依赖与容量测试证明：

- MySQL 中“有效 + 过期/重复”混合 Claim 不回滚有效项，技术故障整批回滚；
- Kafka Commit 后崩溃、Commit 响应不确定、重平衡与多 Partition 分配下无 Offset 越过和重复有效输出；
- 本地已领取未执行数始终不超过空闲 Slot；
- 短 Task 的吞吐、端到端 P95、协调开销、MySQL Pool Wait、Kafka Lag、数据库 CPU/WAL 相比单条基线有可复现改善。

在上述性能测试完成前，简历可以描述“实现上限 8 的单分区微批、批量 Claim/Context 与一次 Offset Commit”，但不能填写未经实测的吞吐或延迟提升比例，也不能声称 Context 数据库与 Redis 读取已经完全集合化。

## 14. Cache 主动预热、Outbox 近似 FIFO 与 Engine 读取热路径

> 决策状态：已确认；14.1-14.3 第一版代码与自动化用例已完成，真实 MySQL/Redis/Kafka 故障与容量验收待目标环境执行；14.4 Engine 读取热路径优化仍待实现
>
> 决策日期：2026-08-28
>
> 适用基线：第 12 节 Engine 直接派发、无 Scheduler/无 Scheduling Redis 架构

### 14.1 Cache Redis 主动预热边界

Cache-Aside 仍然是缓存失效时的正确性边界；主动预热只用来减少第一次 Worker/Engine 读取的 MySQL 往返。预热必须发生在权威事务提交之后，失败不回滚业务事务，也不阻止 Outbox 发布或后续状态推进。

| 数据 | 预热主体与时机 | 目标缓存寿命 | 失败语义 |
|---|---|---:|---|
| Execution Snapshot DSL | Draft Test/Publish 完成 Snapshot 事务提交后 | 12 小时 | Miss 时按 `snapshot_id` 回源；Snapshot 不可变 |
| Run Input | CreateRun 事务提交后 | 10 分钟 | Miss 时按 `run_id` 回源；单 Run Input 不可变 |
| Effective Output | Engine 接受 Current Attempt、提交 `effective_attempt_id` 和 Node Succeeded 后 | 10 分钟 | Miss 时按 `run_id + execution_node_id` 回源 |
| Run View | 不主动预热；查询 Miss 时构建，Run/Node 提交后删除 | 独立短 TTL 安全网 | 删除失败时仍由 TTL 限制陈旧上限 |

Attempt Coordinator 只能在一个事务内提交 Attempt 终态、Output Candidate 和 Completion Outbox，不得将 Candidate 写入 Effective Output 缓存。重复、过期或失去 Current 资格的 Attempt 都可能有合法 Candidate，但只有 Engine 接受的那一个 Output 可以被下游观测和预热。

预热不引入 Redis Write-Behind，也不要求建立持久重试队列。进程在 MySQL Commit 与 Redis Put 之间崩溃时，后续读取依靠 Cache-Aside 自愈。实现必须增加预热尝试、成功、失败和 Miss 回源指标，但不得记录 Input/Output 内容。

### 14.2 Task Outbox 的先来先发

Engine 不按时间选择同一 Run 内的拓扑节点；它仍按依赖和边激活状态计算可执行节点。当一次推进同时产生多个 Task 时，这些 Task 共享本次 MySQL 权威事务时间，并以 `task_id` 作为稳定平局顺序。

`node_task_outbox.occurred_at` 冻结为“该 Attempt 首次成为可发布执行意图的 MySQL 事务时间”，即删除 Scheduler 后的 Task Ready Time。不再额外创建与 `occurred_at` 重复的 `ready_at` 列。Task Relay 的候选集合必须先过滤已到 `available_at` 且 Claim 可用的记录，再按以下顺序批量 Claim：

```sql
ORDER BY occurred_at ASC, task_id ASC
```

`ASC` 才是最早 Task 先发；`DESC` 会变成后到先发。`available_at` 只表示首次可发布或失败退避后的下次可重试时间，不得覆盖原始到达顺序。某条旧 Task 在退避期内不阻塞后续可发布 Task；退避到期后，它按原始 `occurred_at` 回到当前候选集合前部。

此顺序是“近似 FIFO”而不是全局严格 FIFO：

- 多个 Relay 通过 `FOR UPDATE SKIP LOCKED` 并行 Claim 时，后一批可能先完成 Kafka 发布；
- Task 以 `attempt_id` 分散到多个 Partition，Kafka 只保证单 Partition 内顺序；
- Worker Slot、执行时长和 Rebalance 会使完成顺序与发布顺序不同。

为了换取全局严格 FIFO 而收缩为单 Relay、单 Partition 或增加中央序列器，会破坏当前水平扩展目标，第一阶段不实施。验收只声称“Outbox 候选集合最老优先 + Kafka 单 Partition FIFO”。

### 14.3 Runtime Event 与延迟扫描顺序

Runtime Outbox 同样在已到 `available_at` 的候选中按原始业务发生时间批量 Claim：

```sql
ORDER BY occurred_at ASC, event_id ASC
```

Runtime Event 使用 `run_id` 作为 Partition Key，因此同一 Run 在单 Partition 中保持 Producer 送入顺序；重复发布、多 Relay 并发与故障退避仍可产生乱序，Engine 必须继续依靠 Inbox、Run 行锁/CAS 和 MySQL 当前事实收敛，不把 FIFO 当成正确性前提。

延迟或恢复扫描器的候选顺序固定为：

- Retry Timer：`next_retry_at ASC, node_run_id ASC`；
- Deadline Scanner：`deadline_at ASC, run_id ASC`；
- Attempt Reaper：`lease_expires_at ASC, attempt_id ASC`；
- Reconciler：`due_at ASC, aggregate_id ASC`。

上述扫描 SQL 当前已使用升序；后续实现主要补充 Contract/Integration 测试保护，避免回归为无序或降序。

### 14.4 Engine 不使用 Redis 裁决可变聚合状态

Engine 消费 Runtime Event 后，不先从 Redis 读取 Run/Node/Attempt 作为状态机输入。原因是 Engine 每次推进都必须在 MySQL 事务中完成 Inbox 去重、Run/Node/Attempt 锁定或 CAS、状态更改与新 Outbox 写入。即使 Redis 命中，也无法省去这个数据库连接和事务。

Post-Commit 缓存更新与 Kafka 可见性之间不存在顺序保证：Outbox Relay 可能在 Redis Put 之前已经发布 Event，Redis 更新也可能失败、被淘汰或与其他 Engine/Coordinator 写入乱序。若让 Engine 相信该副本，就必须再引入 Aggregate Version、原子 CAS 与失效协议，最终仍需回 MySQL 核对，第一阶段收益不足以支付复杂度。

当前 `LoadEngineState(project_id, run_id)` 确实按 Run 聚合读取：

1. 读取并锁定一行 Workflow Run；
2. 读取该 Run 全部 Node Run，同时左连接已接受 Effective Output；
3. 读取该 Run 全部 Attempt 及候选 Output；
4. 另行读取 Run 绑定的不可变 Snapshot DSL。

单 Workflow 通常不超过 20 个拓扑 Node，因此一次聚合读取是有界的；但历史 Attempt 会随重试/恢复增长，同一 Kafka Poll 内同 Run 多个 Event 当前也会重复执行聚合查询。后续优化顺序固定为：

1. 为 Engine 增加进程内 L1 + Cache Redis L2 的不可变 Snapshot 缓存，Miss 回源 MySQL；
2. `ConsumeBatch` 先按 Run 分组，每个 Run 在一个事务中批量接受 Inbox，只加载一次 Engine State，按该 Run 的 Kafka 交付顺序在内存中连续应用 Event，最后一次集合式提交状态和 Outbox；
3. 将 Attempt 加载收缩为状态机确实需要的 Current/Effective Attempt，不在每次推进时物化全部历史 Attempt；
4. 在不改变 Run 行锁/CAS 边界的前提下，用尽可能少的集合查询拉取 Run + Node + 必要 Attempt，并用压测比较 SQL 行数、事务时长、Pool Wait 和 Engine Event P95。

只有上述优化后容量证据仍表明 Engine State 读取是主要瓶颈，才重新评估带 Aggregate Version 校验的 Redis 可变状态读模型；它仍不得取代 MySQL 锁/CAS 裁决。

### 14.5 后续代码验收

- Snapshot、Run Input 和 Effective Output 在对应权威事务提交后尝试预热；注入 Redis 故障后主事务、Outbox 与后续执行仍成功；
- Candidate Output、过期 Attempt 和未被 Engine 接受的 Completion 不得进入 Effective Output 缓存；
- Task Relay 在单实例、无发布故障基线下按 `occurred_at ASC, task_id ASC` 发布；退避中的头部 Task 不阻塞其他已可发布 Task；
- Runtime Relay 在同样基线下按 `occurred_at ASC, event_id ASC` 发布；Retry/Deadline/Reaper/Reconciler 升序扫描有 Contract/Integration 测试保护；
- 多 Relay、多 Partition 故障测试允许跨 Partition 乱序，但必须保证不丢 Task/Event、不重复业务推进且旧 Attempt 不生效；
- 同 Run 微批处理必须只加载一次可变聚合状态，且不增加 Redis 命中作为 Engine 正确性前提；
- 分别记录冷/热 Snapshot、单 Event/同 Run 多 Event 下的 MySQL 查询数、返回行数、事务时长、Pool Wait P95 和 Engine Event P95，没有实测不声称提升比例。

## 15. 最新冻结目标：数据库优化完成后移除 Cache Redis

> 决策状态：已实施；代码与静态/单元验收已完成，真实 MySQL/Kafka E2E 与固定负载容量复验待本轮最终验收记录
>
> 决策日期：2026-11-17
>
> 取代范围：第 12～14 节中继续保留 Cache Redis、Execution Context 主动预热、Run View Redis Cache-Aside 与 Redis Pub/Sub 的目标设计；历史实现与验收记录仍保留为演进证据

### 15.1 从需求重新推导基础设施边界

第一阶段已经由 MySQL 承担唯一事实源和事务裁决，由 Kafka 承担 Task/Runtime Event 的异步传输、积压与削峰。Run/Node/Attempt 状态推进、Inbox/Outbox、Lease/Fencing、幂等和恢复都必须进入 MySQL 权威事务，Redis 即使命中也不能消除这些正确性路径。

当前 Cache Redis 仅减少 Snapshot、Run Input、Effective Output 和 Run View 的部分重复读取，并以 Pub/Sub 为 SSE 提供可丢失 wake-up。单 Workflow 通常不超过 20 个节点，现有容量证据尚未证明这些可缓存读取是数据库主要瓶颈，也未证明 Redis 相对集合式查询、连接池治理和进程内不可变缓存具有足以覆盖独立部署、故障、失效和运维成本的收益。因此第一阶段最终目标不再包含 Redis。

### 15.2 实施顺序

不得与数据库引擎迁移并行实施本节。MySQL 迁移完成并通过既定 Unit、Contract、Integration、E2E 和事务故障验收后，按以下顺序推进：

1. 建立数据库直读基线，记录 Context/Engine/Run View 的查询数、返回行数、事务时长、Pool Acquire P95、数据库 CPU 与吞吐；
2. 完成数据库批量化：ContextBatch 按 Run/Snapshot 去重并集合读取 Run Input、所需 Effective Output 与 Snapshot，Engine 同 Run 事件只加载一次必要聚合状态，Run View 避免读取无关诊断与历史数据；
3. 完成连接池治理：按 Control Plane 副本数分配总连接预算，使 Engine、Claim/Complete、Outbox Relay、扫描器和外部 API 的有界并发不突破 Pool，配置 Acquire/Statement/Lock 超时并暴露 Pool Wait、占用与超时指标；
4. 增加进程内 Snapshot L1：仅按不可变 `snapshot_id` 缓存已解析执行结构，必须有条目数/字节上限、淘汰、Singleflight、防止错误对象进入缓存以及命中/内存指标；不得缓存可变 Run/Node/Attempt 作为裁决输入；
5. 在数据库直读容量与故障验收通过后，删除 Cache Redis Adapter、预热/回填/失效、Run View Redis Cache、Redis Pub/Sub、配置、Readiness、Compose/生产部署、指标和 `go-redis` 依赖；
6. 以同一固定负载对比迁移前 Redis 基线与“MySQL 批量化 + Pool 治理 + Snapshot L1”结果，不为移除 Redis 降低正确性、容量或恢复门槛。

实现结果：ContextBatch 一次查询验证批内 Attempt 的 Lease/Fencing，再按 Run/Snapshot 聚合，单个 Run 用一次集合查询读取 Workflow Input 与所需 Effective Output；Engine 同 Run Kafka 微批在一个事务内复用已锁定聚合状态和不可变 Snapshot；进程内 Snapshot L1 使用条目数与估算解析后内存双上限、LRU、并发加载合并和坏值不入缓存。连接池有跨实例总预算验证，实际等待、占用和事务开始耗时指标可观测。`go-redis`、Cache Adapter、预热/失效、Redis Readiness/配置/Compose 已删除。Acquire/Statement 超时与目标环境容量复验仍需后续完成。

### 15.3 SSE 替代边界

SSE 继续只发送 `ready/update` wake-up，不携带 Runtime 权威状态；浏览器收到通知或重连后仍必须 GET Run Read Model。移除 Redis Pub/Sub 后：

- 同一 Control Plane 实例内可使用有界进程内 notifier 提供低延迟 wake-up；
- SSE 连接按 Profile 配置的短间隔加有限抖动读取轻量投影版本作为跨实例与丢通知兜底；该版本为 Run State Version 与全部 Node State Version 之和，能够识别 Run 仍为 running 时的节点变化。轮询不读取完整 Run View；读失败会结束连接，由客户端重连。更长故障下的指数退避仍待容量与故障验收。
- 客户端断线后继续按现有语义重新 GET，通知丢失只增加界面刷新延迟，不改变 Run 状态；
- 不为 SSE 单独引入新的消息中间件、数据库轮询表或 Kafka 广播拓扑，除非容量证据证明轻量版本轮询不满足目标。

### 15.4 验收不变量

- 仓库、配置、健康检查、部署和运行时不存在 Redis 客户端、Endpoint 或隐式启动依赖；
- Redis 完全不存在时，Definition、Run、Worker、Retry/Recovery、CLI/Web 和最终 E2E 全部成立；
- ContextBatch 和同 Run Engine 微批具有可重复的查询数上限，不出现逐 Context/逐上游节点 N+1；
- 所有副本数据库 Pool Max 总和保持在数据库连接预算内，过载时有界等待或明确拒绝，不以无限 goroutine 堆积掩盖瓶颈；
- Snapshot L1 只包含不可变数据，淘汰或进程重启只导致 MySQL 回源，不影响正确性；
- SSE 在同实例、跨实例、通知丢失、连接中断和进程重启场景下最终刷新到权威 Run View；
- 移除 Redis 后的固定负载仍满足已冻结的 Pool Wait、Outbox Age、Kafka Lag、Queued-to-Running、错误率和恢复门槛。
