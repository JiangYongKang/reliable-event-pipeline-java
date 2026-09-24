# 本地可靠事件处理管道

基于 Spring Boot 的进程内可靠事件管道：用**可恢复的本地存储**（内存索引 + JSON 快照原子落盘）
模拟消息中间件，保证事件在并发投递、失败重试与服务重启后的状态一致且可解释。

## 架构

```
业务代码 ──submit──▶ EventPipeline ──commitBusinessWithEvent──▶ EventStore(单锁 + 原子快照)
                                                              │
派发线程 ◀──claimNext──┘                                       ▼
   │                                                   data/pipeline-store.json
   ▼ handle(event, EffectContext)
EventHandler（业务副作用，幂等）
   │ 成功 ack / 失败 fail(RETRYABLE|NON_RETRYABLE)
   ▼
PENDING → IN_FLIGHT → ACKED
   ↑          │
   └─退避重排──┘  超过上限或不可重试 → DEAD（可 redeliver 重投）
```

- `store/EventStore`：所有状态变更在单锁内完成并原子写盘（临时文件 + `ATOMIC_MOVE`），
  模拟中间件的持久化语义；重启时 `load()` 从快照恢复。
- `pipeline/EventPipeline`：原子提交入口 + 后台派发循环，按处理结果确认或重试/死信。
- `handler/EventHandler` + `EffectContext`：业务处理器与幂等副作用上下文。

## 投递语义

1. **原子提交**：`submit(idempotencyKey, aggregateId, businessData, payload)` 在同一锁内
   同时写入业务记录与事件并落盘——要么都成功，要么都失败，不存在只有事件没有业务数据
   （或相反）的中间状态。
2. **幂等提交**：同一 `idempotencyKey` 重复提交直接返回已存在事件，不产生重复业务记录与事件。
3. **聚合内有序**：每个事件携带聚合内递增序号 `seq`；派发时若某聚合存在在途（IN_FLIGHT）
   事件，则跳过该聚合的其他事件，保证同一聚合按提交顺序逐个投递；不同聚合并行处理，
   单个失败事件不会阻塞其他聚合。
4. **认领与超时回收**：派发线程通过 `claimNext` 认领事件，获得 `claim-timeout` 租约；
   超时未确认的事件被回收为 PENDING，可被重新认领（崩溃/慢处理的恢复路径）。
5. **幂等消费**：业务副作用通过 `EffectContext.tryBeginEffect()/completeEffect()` 执行，
   由持久化的副作用账本（NONE/EXECUTING/DONE + 执行租约）保证同一事件的副作用只生效一次：
   - `EXECUTE`：获得执行权，应执行副作用并 `completeEffect()`；
   - `SKIP_ALREADY_DONE`：副作用已完成，直接返回即可确认；
   - `BUSY`：他人正在执行，应抛出可重试异常稍后重试。
6. **统计一致性**：`stats()` 的各状态计数与副作用完成数始终由存储中的实际事件状态推导。

## 重试与死信规则

- 处理器抛出 `EventProcessingException.retryable(...)`（或未分类异常）→ 按
  `backoff-base * 2^(attempt-1)` 指数退避重排；达到 `max-delivery-attempts` 后进入死信，
  原因 `RETRIES_EXHAUSTED`。
- 抛出 `EventProcessingException.nonRetryable(...)` → 立即进入死信，
  原因 `NON_RETRYABLE_FAILURE`。
- 死信可通过 `redeliver(eventId)` 重投：状态重置为 PENDING、尝试计数清零、`redeliveries` 递增；
  **仅 DEAD 状态允许重投**，其他状态抛出 `IllegalRedeliveryException`。

## 配置（application.properties）

| 键 | 默认 | 说明 |
|---|---|---|
| `pipeline.max-delivery-attempts` | 3 | 最大投递尝试次数（含首次） |
| `pipeline.backoff-base` | 100ms | 退避基数 |
| `pipeline.claim-timeout` | 5s | 认领租约时长 |
| `pipeline.effect-lease-timeout` | 10s | 副作用执行租约时长 |
| `pipeline.worker-threads` | 4 | 派发线程数 |
| `pipeline.poll-interval` | 20ms | 派发轮询间隔 |
| `pipeline.store-file` | data/pipeline-store.json | 快照文件路径 |

## 本地验证

### 运行测试

```bash
mvn test
```

测试覆盖：原子提交与幂等提交、聚合内顺序、并发认领唯一性、重试耗尽死信、
不可重试死信、死信重投与非法重投拒绝、失败聚合隔离、认领超时回收、
并发重复投递副作用仅生效一次、重启恢复与死信状态保留。
单测日志会打印事件标识、尝试次数与判定依据（`判定依据: ...`）。

### 运行服务并通过 REST 验证

```bash
mvn spring-boot:run

# 提交（业务写入 + 事件入队原子提交）
curl -X POST localhost:8080/api/pipeline/events -H 'Content-Type: application/json' -d \
  '{"idempotencyKey":"order-1","aggregateId":"order","businessData":"{}","payload":"created"}'

# 重复提交同一幂等键 → 返回同一事件，不产生重复
curl -X POST localhost:8080/api/pipeline/events -H 'Content-Type: application/json' -d \
  '{"idempotencyKey":"order-1","aggregateId":"order","businessData":"{}","payload":"created"}'

# 查看事件与统计
curl localhost:8080/api/pipeline/events
curl localhost:8080/api/pipeline/stats

# 死信重投（仅 DEAD 状态允许）
curl -X POST localhost:8080/api/pipeline/events/{eventId}/redeliver

# 验证重启恢复：kill 进程后重新启动，未完成事件会继续处理
```
