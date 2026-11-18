# EvalFrog 简历表述与证据边界

来源：[飞书 Wiki 讲解](https://my.feishu.cn/wiki/NBfLwQwFKip0izk21HScc00hnjd)。

使用原则：

- 先说明解决的工程问题，再展开 Revision、Outbox、Lease 等实现机制；
- 只把当前代码已经实现并验收的事实写成项目成果；
- 仿真、自动化测试、故障注入与真实生产指标必须明确区分；
- 没有可靠量化证据时，用问题、方案和可验证结果表达，不虚构吞吐或延迟收益。

## 一、简历推荐版

项目简介：**EvalFrog——同时面向 Web 用户与 AI Agent 的企业工作流评测平台，使用 Go、MySQL 与 Kafka 构建统一编排、并发编辑和可靠执行闭环。**

1. **并发编辑一致性与用户体验优化：** 针对 Web/CLI 多用户、多端并发编辑工作流可能引发的丢失更新及冲突体验问题，基于 Draft Revision 实现乐观并发控制，保存时在短事务内复核预期 Revision，并在冲突时提供画布式三方合并；针对保存请求的超时重试，采用唯一键抢占式 INSERT 保证操作幂等，支持安全重试。
2. **高并发工作流推进：** 针对定时批量评测形成的瞬时 Run 洪峰，Engine 按 Run 聚合 Kafka Runtime Event，同一 Run 串行推进、不同 Run 在数据库连接预算内并发；在单 Run 事务中集合式写入 Node、Attempt 与 Task Outbox，使运行状态和待投递任务原子提交。
3. **Kafka 可靠执行闭环：** 构建 `MySQL Outbox → Kafka → Worker Claim/ACK → Runtime Outbox → Engine Inbox/CAS` 异步链路；Worker 通过 Lease/Fencing 获取执行权，崩溃后由 Reaper 创建 Recovery Attempt，重复与乱序消息由 Inbox、CAS 及有效结果指针收敛，确保只有一个结果对下游生效。
4. **Agent CLI Skill：** 设计并落地面向 Agent 的 CLI 操作手册与 Skill，支持 Agent 在本地会话中创建、校验、发布、运行和诊断工作流，并按需加载相关指南以控制上下文开销。

如果简历篇幅只允许三点，优先保留第 1～3 点；第 4 点放在项目补充或面试展开中。

## 二、面试展开口径

### 1. 并发编辑为什么使用 Revision 与三方合并

- 保存请求携带编辑所基于的 Draft Revision；服务端在短事务中锁定工作流并复核预期 Revision，仅当前 Revision 一致时创建新修订；
- Revision 不一致时不覆盖数据库中的最新内容，而是以 Base、当前 Revision 和用户编辑结果计算三方差异；
- 前端只要求用户处理双方实际修改过的节点和边，未变化元素自动合并，并允许在冲突画布中拖拽、保留或删除完整元素；
- 幂等键先通过唯一索引抢占，同一键的重试读取既有结果并校验请求摘要，避免一次保存因超时重试而被执行多次。

### 2. Kafka Consumer 与 Partition 的关系

- Builtin Task、Sandbox Task 与 Runtime Event 使用独立 Topic 和 Consumer Group；
- 一个 Worker 进程是一个 Group Member，可被分配多个 Partition；Worker Slot 决定进程内同时执行的 Attempt 数量，Partition 数决定 Consumer Group 的最大活跃成员数和 Broker 并行度；
- Task 以 `attempt_id` 为 Key 分散负载，不提供 Workflow 级全局 FIFO；Runtime Event 以 `run_id` 为 Key，同一 Run 的事件落入同一 Partition；
- Engine 对一次 Poll 的事件按 Run 分组，同一 Run 串行、不同 Run 受 `engine.max_inflight` 限制并发，为 API、Outbox 与 Recovery 保留数据库连接余量。

### 3. 为什么是 Claim 后 ACK

Kafka 负责把 Task 送达 Worker，MySQL Lease 负责 ACK 后的执行责任。Worker 只有在数据库成功授予执行权后才 ACK：ACK 前退出由 Kafka 重投，ACK 后退出由 Lease/Reaper 恢复，过期 Worker 的迟到结果由 Fencing 拒绝。

把 ACK 延迟到执行完成会让慢节点长期占用 Partition 的提交进度并增加 Rebalance 风险；直接取消 Claim 又无法可靠处理重复执行与不可撤销的外部副作用。因此执行前路径采用有界批处理：

```text
Poll partition batch
→ ClaimBatch（批量获取 Lease 与 Execution Context）
→ 提交连续安全 Offset
→ 本地 Slot 并发执行
→ CompleteAttempt
```

`ClaimBatch` 每批最多处理 8 个独立 Attempt，并在一次数据库往返中返回执行资格与上下文；批内仍保留逐 Attempt 的 Lease 与 Fencing 语义。

## 三、可引用的验证证据

| 事实 | 当前证据 | 使用边界 |
|---|---|---|
| 重复创建事件不会生成两套运行图 | Integration Test 对同一事件重放后检查 Inbox 与 Node Run 唯一性 | 自动化并发正确性测试，不等同生产流量 |
| 多 Engine 竞争由数据库收敛 | 并发 Consumer 竞争同一 Run，最终仅生成一套图和一条 Inbox | 可说明 Run Lock、Inbox 与事务边界 |
| 跨 Run 并发受配置约束 | Engine Batch Test 验证实际并发不超过配置上限 | 可说明连接预算，不报告吞吐提升比例 |
| 宽工作流批协议可以收敛 | 宽度 8/32、20 个 Run、400 个 Task 的确定性实验全部通过 | 单机协议实验，不等同生产容量 |
| Kafka 故障窗口可以恢复 | 10 个消息/事务窗口共 100 次注入全部通过 | 证明已覆盖窗口内的不变量，不宣称 Exactly-Once |
| 三 Broker 协议符合预期 | RF=3、Min ISR=2、`acks=all` 下验证单 Broker/Leader 故障、双 Broker 拒绝写入及恢复排空 | 单机容器环境，不等同跨机房容灾 |

详细实验条件与边界见 [Kafka 故障窗口实验报告](./Kafka故障窗口实验报告.md)。

## 四、仍需生产环境补充的指标

1. 实际租户与 Workflow 分布下的端到端 P95/P99、Kafka Lag 和数据库连接池等待；
2. Worker Poll、ClaimBatch、Offset Commit、Context Load、Execute、Complete 的分段耗时；
3. 长时间运行和真实故障下的恢复时间、重复执行率、DLQ 比例与人工介入次数；
4. 真实业务负载下的容量上限、扩容曲线与成本数据。
