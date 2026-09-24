# 本地可靠事件处理管道

用进程内且可恢复的存储（写前日志 journal 文件）模拟消息中间件，保证事件在并发投递、
失败重试与服务重启后的状态一致且可解释。

## 组件

| 组件 | 职责 |
| --- | --- |
| `InProcessEventStore` | 事件存储：所有状态变更先追加到 journal 文件再应用到内存，重启后重放恢复；所有方法在同一锁下串行化，保证并发安全 |
| `EventDispatcher` | 派发器：认领 → 调用 `EventHandler` → 确认 / 退避重试 / 死信隔离，每次判定都打印事件标识、尝试次数与判定依据 |
| `RetryPolicy` | 可配置的指数退避策略（最大尝试次数、初始退避、倍率、上限） |
| `PipelineStats` | 各状态事件数量与死信原因分布的快照，始终由实际事件状态实时统计得出 |

## 投递语义

- **原子提交**：`commitBusinessAndEvent(operationKey, businessWrite, event)` 在同一临界区内
  先执行业务写入、再把「操作键 + 事件」作为一条 journal 记录落盘。业务写入失败则整体失败，
  不会出现只有事件没有业务数据（或相反）的中间状态。
- **幂等提交**：同一 `operationKey` 重复提交返回 `DUPLICATE_IGNORED`，不产生重复事件，
  业务写入也不会重复执行；操作键随 journal 持久化，重启后仍然有效。
- **聚合内有序**：每个聚合的事件在提交时分配递增序号；认领时每个聚合只认领队首
  （序号最小的未完成）事件，因此同一聚合的事件严格按提交顺序投递。
  单个失败事件只会阻塞本聚合的后续事件，不影响其他聚合。
- **至少一次投递 + 幂等消费**：事件可能被重复投递（如确认前崩溃），消费方应通过
  `consumeOnce(eventId, sideEffect)` 保证业务副作用只生效一次；消费标记随 journal 持久化。
- **并发安全认领**：认领带租约（`claim-timeout-ms`），租约未过期他人不得抢占；
  超时未确认的事件可被其他消费者重新认领，进程重启后未完成事件可继续处理。

## 重试与死信规则

- 处理器抛出 `NonRetryableException` → 事件**立即进入死信**，原因记为 `NON_RETRYABLE_FAILURE`。
- 处理器抛出其他异常 → 视为可重试故障，按 `RetryPolicy` 指数退避重新挂起
  （`backoff = initial * multiplier^(attempt-1)`，封顶 `max-backoff-ms`），退避期内不可被认领。
- 可重试故障达到 `max-attempts` 次 → 进入死信，原因记为 `MAX_RETRIES_EXCEEDED`。
  两种死信原因在事件记录与 `PipelineStats.deadByReason` 中可区分。
- **死信重投**：`requeueDead(eventId)` 将死信事件重置为 `PENDING`（尝试计数清零）；
  仅 `DEAD` 状态允许重投，对其他状态调用会抛出 `IllegalStateException` 拒绝非法重投。

事件状态机：`PENDING → CLAIMED → DELIVERED`，失败时 `CLAIMED → PENDING`（退避重试）
或 `CLAIMED → DEAD`（死信），死信可经 `requeueDead` 回到 `PENDING`。

## 配置

见 `src/main/resources/application.properties`：

```properties
pipeline.journal-file=./data/event-journal.jsonl   # journal 文件位置
pipeline.claim-timeout-ms=30000                    # 认领租约超时
pipeline.retry.max-attempts=3                      # 最大尝试次数
pipeline.retry.initial-backoff-ms=100              # 初始退避
pipeline.retry.multiplier=2.0                      # 退避倍率
pipeline.retry.max-backoff-ms=5000                 # 退避上限
```

## 本地验证

```bash
mvn test
```

测试覆盖（日志中打印事件标识、尝试次数与判定依据）：

- `AtomicCommitTest`：原子提交、重复提交幂等忽略、失败不留中间状态、聚合序号分配。
- `RetryDeadLetterTest`：指数退避重试、重试耗尽死信、不可重试故障直接死信（原因可区分）、
  死信重投与非法重投拒绝、单事件失败不阻塞其他聚合。
- `ConcurrentDeliveryTest`：并发重复提交只生效一次、并发消费副作用只生效一次、
  多派发器并发下每事件恰好投递一次且聚合内保序、统计量与实际状态一致。
- `RestartRecoveryTest`：重启后未完成事件继续处理、租约超时回收、
  消费标记与已提交操作键不丢失。

手动运行服务：`mvn spring-boot:run`（默认处理器仅打印日志，业务方替换
`PipelineConfig#loggingEventHandler` 注入自己的 `EventHandler`）。
