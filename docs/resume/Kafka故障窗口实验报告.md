# EvalFrog Kafka 上下游可靠性实验报告

## 1. 结论

2026-11-16 在当前执行架构上完成三组实验：

- 10 个单消息故障窗口，每个窗口重复 10 次，共 100 次注入，全部通过；
- Workflow 宽度 8、32 各重复 10 轮，共 20 个 Run、400 个 Task，全部收敛为 `succeeded`；
- 三 Broker（RF=3、`min.insync.replicas=2`、Producer `acks=all`）协议验收全部通过。

验证期间未出现永久卡住 Run、重复 Effective Output、错误 Inbox 提交、过期 Worker 迟到结果被接受，或 Kafka 不满足 Min ISR 时仍将 Outbox 标记为 Published 的情况。

本报告证明当前代码中的传输与数据库协议在确定性故障窗口下满足不变量。它不是生产吞吐量、可用性百分比、Kafka Exactly-Once 或跨机房容灾证明。

## 2. 执行链路

```text
Engine transaction
  → Node Run + Attempt + Task Outbox
  → Task Relay → Kafka Task
  → Worker Poll → MySQL ClaimBatch → Kafka ACK
  → CompleteAttempt + Runtime Outbox
  → Runtime Relay → Kafka Runtime Event
  → Engine Inbox + Run CAS
```

Engine 在同一 MySQL 事务中集合式生成 `queued Node + Attempt + Task Outbox`。MySQL 保存权威状态，Kafka 负责缓冲与传输；Worker 通过 Control Plane API 获取 Lease 并提交执行结果。

## 3. 十个单消息故障窗口

测试：`TestM12KafkaReliabilityFaultWindows`

Workflow 宽度：1

重复次数：每窗口 10 次

Lease：2s

墙钟：25.64s

| 窗口 | 注入位置 | 验证的恢复机制 | 结果 |
|---|---|---|---|
| F1 | Task Outbox 已提交，尚未 Publish | Claim 过期后新 Relay 重发 | 通过 |
| F2 | Task 已 Publish，尚未 Mark Published | 重投与 Attempt Claim 收敛 | 通过 |
| F3 | Worker Poll 后、Claim 前退出 | 未提交 Offset 由同组新 Consumer 重投 | 通过 |
| F4 | Claim 已提交、Kafka ACK 前退出 | 重投后 Worker 复用同一有效 Lease | 通过 |
| F5 | Kafka ACK 后、Complete 前退出 | Lease 过期、Lost、Retry Due 与 Recovery Attempt | 通过 |
| F6 | Complete 已提交、Runtime Event Publish 前退出 | Runtime Outbox 保留并补发 | 通过 |
| F7 | Runtime Event 已 Publish、Outbox Mark 前退出 | 重投与 Engine Inbox 去重 | 通过 |
| F8 | Engine Inbox Insert 后事务回滚 | Inbox 与业务推进共同回滚，事件可重试 | 通过 |
| F9 | Recovery Attempt 已创建、过期 Worker 迟到 Complete | Lease、Fencing 与 Current Attempt Pointer 拒绝过期结果 | 通过 |
| F10 | Poll/Claim/ACK 临界区发生 Consumer Rebalance | Rebalance Gate 延迟分区撤销，未提交消息安全转移 | 通过 |

F3/F4 使用真实 Consumer 关闭和同组新 Consumer 重建，不用同一 Consumer 的 `Nack` 模拟进程重启。`Nack` 只释放 Rebalance Gate；同进程 Fetch Position 不会因此回退。

## 4. 宽工作流与批处理

测试：`TestM12KafkaWideWorkflowExperiment`

宽度：8、32

重复次数：每种宽度 10 轮

总量：20 Run、400 Task

墙钟：6.60s

每个 Run 验证：

1. Engine 初始化事务结束后，`queued task node`、`node_attempts`、`node_task_outbox` 数量均等于 Workflow 宽度；
2. Relay 对整批 Task 交替注入发布失败，首次仅 200 条收到 Kafka ACK 并标记 Published，另外 200 条保留在 Outbox；
3. 正常 Relay 重试后，400 条 Outbox 全部 Published；
4. Worker 按单 Partition、每批最多 8 条执行 `ClaimBatch → ACK → Complete`；
5. Runtime Event 分批进入 Engine，同一 Run 的多个完成事件最终只形成一次有效状态推进；
6. 每个 Run 最终为 `succeeded`，输出行数与 Inbox Completion 行数均等于宽度。

这组数据只说明批协议在宽度 8/32 下成立，不报告 `tasks/s`，也不推断生产容量。

## 5. 三 Broker 协议验收

测试：`TestM12KafkaThreeBrokerProtocol`

编排：`deployments/compose.kafka-ha.yaml`

Kafka：3 个 KRaft Broker，RF=3，Min ISR=2，Producer `acks=all`

墙钟：107.68s

| 场景 | 观测 | 结果 |
|---|---|---|
| 停 1 个非 Leader Broker | ISR 从 3 降到 2，Task 仍获 `acks=all`，Outbox 才标记 Published | 通过 |
| 停当前 Partition Leader | 选出新 Leader，ISR=2，Task 继续成功发布 | 通过 |
| 同时停 2 个 Broker | 发布失败，Relay 返回错误，Task Outbox 保持 Unpublished | 通过 |
| 恢复 1 个 Broker | ISR 恢复到 2，原 Outbox 被 Relay 成功排空 | 通过 |
| 全部恢复 | Replica=3、ISR=3 | 通过 |

`acks=all` 的成功条件受 Min ISR 约束：单 Broker 故障时仍有两个同步副本确认；仅剩一个 Broker 时，即使它仍持有 Leader，也不能把写入判断为成功。

## 6. 运行环境与边界

- 分支：`codex/m12-capacity-security-release`；
- MySQL：`mysql:8.4`；
- Kafka：`apache/kafka:3.9.1`；
- 单 Broker 实验：Compose 网络 `evalfrog-local_authority`；
- 三 Broker 实验：Compose 网络 `evalfrog-kafka-ha_ha`；
- 测试直接调用生产 Store、Relay、Kafka Adapter、Coordinator、Reaper、Retry Timer 与 Engine Consumer；
- Python 代码没有进入真实 Sandbox 执行，测试在 Worker 协议边界构造成功结果，重点验证消息传输、执行权与状态推进协议。

## 7. 复现

启动本地依赖后执行：

```powershell
$env:EVALFROG_KAFKA_RELIABILITY_EXPERIMENT = "1"
$env:EVALFROG_KAFKA_FAULT_SAMPLES = "10"
$env:EVALFROG_KAFKA_FAULT_LEASE = "2s"
go test -tags=integration ./tests/integration -run '^TestM12KafkaReliabilityFaultWindows$' -count=1 -timeout 30m -v

$env:EVALFROG_KAFKA_WIDE_EXPERIMENT = "1"
$env:EVALFROG_KAFKA_WIDE_REPETITIONS = "10"
go test -tags=integration ./tests/integration -run '^TestM12KafkaWideWorkflowExperiment$' -count=1 -timeout 30m -v
```

三 Broker 验收需要启动独立编排，并允许测试通过 Docker Socket 停启指定 Broker：

```powershell
docker compose -f deployments/compose.kafka-ha.yaml up -d --wait

$env:EVALFROG_KAFKA_HA_EXPERIMENT = "1"
$env:EVALFROG_KAFKA_EXPERIMENT_BROKERS = "kafka-1:9092,kafka-2:9092,kafka-3:9092"
$env:EVALFROG_KAFKA_EXPERIMENT_REPLICATION_FACTOR = "3"
$env:EVALFROG_KAFKA_EXPERIMENT_MIN_ISR = "2"
go test -tags=integration ./tests/integration -run '^TestM12KafkaThreeBrokerProtocol$' -count=1 -timeout 20m -v
```

若测试在容器中执行，还需挂载 `/var/run/docker.sock` 并加入 `evalfrog-kafka-ha_ha` 网络。

## 8. 简历建议表述

> 设计并落地 MySQL Transactional Outbox → Kafka → Worker Claim/ACK → Runtime Outbox → Engine Inbox/CAS 的异步执行闭环，以数据库作为权威状态源，通过 Lease、Fencing 与 Current Attempt Pointer 收敛重复投递、Worker 崩溃和迟到结果。围绕 10 个消息/事务故障窗口完成 100 次确定性注入，并以宽度 8/32 的 20 个 Workflow、400 个 Task 验证批量派发与同 Run 多完成事件收敛；在 RF=3、Min ISR=2、`acks=all` 的三 Broker 环境中验证单 Broker/Leader 故障持续写入、双 Broker 故障拒绝写入且 Outbox 保留、恢复后自动排空，未出现重复 Effective Output 或非法状态推进。

面试时应主动补充：这些数字来自单机协议验收，不是生产吞吐量；核心贡献是故障模型、状态不变量、恢复协议和可复现测试，而不是单纯“使用了 Kafka”。
