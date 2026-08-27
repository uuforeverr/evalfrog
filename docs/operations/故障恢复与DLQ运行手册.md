# EvalFrog 故障恢复与 DLQ 运行手册

本手册只覆盖 M11 已实现的恢复与诊断能力。它不授予绕过 Engine、直接编辑 Runtime 表或向 Kafka 注入任意消息的权限。

## 不变量

- PostgreSQL 的 Run、Node Run、Attempt、Termination Intent、Outbox/Inbox 是权威事实；
- Retry Timer、Deadline Scanner、Attempt Reaper、Reconciler 只补发 durable wake-up；
- Engine 是唯一能够推进 Workflow 语义状态的组件；
- `trace_id` 是一次 Run 的根关联 ID，跨 Outbox、Kafka、Worker、Completion、Recovery 与 Audit 保留；
- Cache Redis 丢失时回源 PostgreSQL，不影响任务创建和状态推进；
- DLQ 是诊断线索，绝不是任意 Kafka 重放或状态修复入口。

## 告警与第一响应

| 信号 | 权威来源 | 首先检查 | 自动恢复预期 |
|---|---|---|---|
| `evalfrog_outbox_oldest_unpublished_age_seconds` | Runtime + Node Task Outbox | Relay、Kafka 连接、Outbox Claim/Publish | Relay 重启后至少一次发布，Consumer Inbox 收敛重复 |
| `evalfrog_kafka_consumer_lag_records` | Kafka Group/Topic offset | Consumer 健康、partition/rebalance、Worker Slot | 恢复消费后 Lag 下降；Offset 不代表 Run 成功 |
| `evalfrog_attempt_lease_lost_total` | PostgreSQL Lease Reaper | Worker 崩溃、Heartbeat、执行时长、网络 | Lost → Engine 的独立 Recovery Budget；旧结果被 Fencing 拒绝 |
| Queued-to-Running 延迟 | PostgreSQL Attempt 时间 | Kafka Lag、Worker Slot、Claim/API 延迟 | Worker 恢复或扩容后回落 |
| `evalfrog_runtime_recovery_wakeups_total` | Recovery Scanner/Emitter | 条件是否仍 actionable、扫描器日志、Outbox | 重复 wake-up 由 cooldown、Inbox 与 CAS 收敛 |

阈值由当前 Profile 的 SLO、Worker 容量与业务时限制定；不得把示例秒数写入领域代码。告警触发后，先按 `project_id + run_id` 获取诊断并保留 `trace_id`，不要先重启全部组件或修改数据库。

## 诊断流程

```text
告警 / 用户报告 Run 卡住
  → GET Run，确认当前状态
  → run diagnose，读取 Attempt、Audit、trace_id
  → 检查 Outbox Age / Kafka Lag / Worker / PostgreSQL 健康
  → 等待相应 Scanner 或 Relay 自动恢复
  → 仅在事实仍 actionable 且自动恢复未及时发生时，Project Admin 使用受限 Replay
```

示例：

```powershell
evalfrog run diagnose --server <url> --token <token> --project <project-id> --run <run-id>
evalfrog run replay --server <url> --token <token> --project <project-id> --run <run-id> --event-type attempt.lost --aggregate-id <attempt-id>
```

Replay 只能选择 `run.created`、`run.cancel_requested`、`attempt.completed`、`attempt.lost`、`retry.due` 或 `run.deadline_reached`。服务端再次读取 PostgreSQL：目标不再 actionable 时返回未接受，这不是失败；若接受，服务端原子写 Audit 和 Runtime Outbox，Engine 之后正常收敛。禁止复制、伪造或手动投递原始 Kafka Payload。

## 常见故障处置

### Worker 崩溃或 ACK 后失联

等待 Lease 到期。Reaper 只会把仍为 Current Attempt 的 Running Attempt 标记为 Lost；Engine 根据独立 `recovery_count` 决定是否创建恢复 Attempt。不要把旧 Worker 的迟到 Completion 当作成功：Lease Token、Fencing Token、Attempt Sequence 和 Current Attempt 会拒绝它。

### Kafka 暂停、乱序或 Rebalance

恢复 Broker/Consumer 后，Runtime Outbox Relay 会至少一次发布；Engine Inbox 与 CAS 允许重复和乱序。Kafka adapter 会对短暂网络错误、`COORDINATOR_NOT_AVAILABLE`、`NOT_COORDINATOR` 及需要 rejoin 的 rebalance/generation/member 信号退避、重新 Poll/join group，Control Plane 与 Worker 不应因此退出。对于长时间的 Lag，先确认 Outbox Age、Consumer group、partition 数和 Worker Slot；不可重试的认证、Topic、协议或 static member fencing 错误需要修复部署配置，不能无限重试。不要因为 Task 已在 Kafka 中就把数据库 Attempt 标记为完成。

### Cache Redis 全量清空

不需要手工修复 Runtime 状态。Execution Context 和 Run View Cache Miss 会从 PostgreSQL 回源并回填；读取延迟可能短暂升高。若读失败，检查 PostgreSQL 与授权，而不是将缓存临时改为权威写入。

### PostgreSQL 短时不可用或连接池耗尽

Control Plane、Relay、Scanner 和 Worker Gateway 会返回可重试基础设施错误；不要把这类错误记成业务失败。恢复数据库连接后，Outbox、Scanner 与 Reconciler 重新读取事实并继续。检查连接池总预算是否仍满足所有副本 `PoolMax` 总和不超过 PostgreSQL `max_connections` 的 70%。

### Deadline 与 Cancel 竞争

以持久化时间为准：Deadline 到达前已经写入的 Cancel 获胜；Deadline 之后才写入的 Cancel 不能复活 Run。任何延迟的 `RetryDue`、`RunCreated` 或 Completion 都必须先接受这个终止结果。

## DLQ 处置

1. 记录 Topic、partition、offset、消息 ID、`trace_id`（若存在）和错误类别；不要把输入、输出、Secret、Lease Token 或 stderr 复制到工单。
2. 使用 Run Diagnostics 和对应结构化日志确认 PostgreSQL 是否已有 Attempt/Run 事实。
3. 若消息已经有对应权威事实，优先让 Relay/Consumer/Scanner 重试；若是当前 actionable 的唤醒遗漏，可使用受限 Replay。
4. 若是协议/代码缺陷，修复并走正常发布；不得把原始 DLQ Payload 直接重新生产到 Runtime Topic。
5. 处置完成后验证：不存在非法状态组合、重复 Effective Output、跨 Project 数据、未发布 Outbox 长期积压或 Fencing 被绕过。

## 故障演练验证

本地与 CI 的核心恢复演练入口：

```powershell
go test -tags=integration ./tests/integration -run TestM11 -count=1
go test -race ./...
docker compose -f deployments/compose.yaml restart kafka
```

`TestM11RecoveryWakeupsAreDurableTraceableAndEngineOwned` 验证 Lease Lost、Reconciler 不直接改语义状态、旧结果 Fencing 拒绝；`TestM11DeadlineBlocksRetryRecoveryAndDiagnosticsAuditAreSafe` 验证 Deadline 阻止恢复与安全诊断；`TestM11TraceIsPreservedFromRunThroughTaskAndCompletion` 验证完整 Trace 链路。Compose Kafka 重启演练要求 Control Plane 与两个 Worker Pool 在 Broker 恢复后仍持续健康；`internal/adapters/kafka/client_test.go` 覆盖其可重试/不可重试错误分类。
