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

## 投递时机与背压规则

1. **延迟投递**：提交时可指定最早可投递时间 `deliverAt`（REST 为 ISO-8601 字段），
   到点之前该事件不会被任何派发线程认领。聚合内实行**队首阻塞**：只有该聚合最早
   未完成（PENDING/IN_FLIGHT）的事件可被认领——前面的事件还没到点或还在途时，
   后面已就绪的事件不能抢先，保证聚合内始终按提交顺序投递。`deliverAt` 随快照持久化，
   进程重启后未到点的等待保留，不会提前放出。
2. **延迟上限**：`pipeline.max-delay` 限制剩余等待时长。提交时 `deliverAt` 超过
   `now + max-delay` 的事件直接以死信落库，原因 `DELAY_EXCEEDED`（与
   `RETRIES_EXHAUSTED` / `NON_RETRYABLE_FAILURE` 可区分）；派发循环也会清扫
   剩余等待超限的延迟事件。此类死信的业务记录已随事件原子提交，可排查后
   `redeliver` 重投。注意：重投只重置投递调度，若 `deliverAt` 仍超限会被再次清扫。
3. **聚合级暂停/恢复**：`pauseAggregate(aggregateId)` 后该聚合的事件一条都不会被认领，
   其他聚合照常处理；暂停期间新提交的事件保持 PENDING，不计失败、不进死信。
   `resumeAggregate` 后从暂停前的位置按提交顺序继续，不跳序、不丢失。
   暂停状态随快照持久化，重启后仍然有效。
4. **积压水位**：`backlogStats()`（REST: `GET /api/pipeline/backlog`）按聚合与全局给出
   未完成（PENDING + IN_FLIGHT）事件数与最久积压时长（由最老未完成事件的
   `createdAt` 推导）。
5. **背压拒绝**：某聚合积压达到 `pipeline.max-backlog-per-aggregate`（<=0 表示不限制）时，
   对该聚合的新提交抛出 `BackpressureRejectedException`（REST 返回 429 +
   `error=BACKPRESSURE_REJECTED`）。拒绝发生在任何写入之前——业务记录与事件
   **都不会**落库，不存在"业务落了事件没落"的半截状态；幂等键重复的提交视为
   查询已有结果，不触发背压。其他聚合的提交不受该聚合触顶影响。

### 与重试/死信的配合

- 延迟等待（`deliverAt` 未到）与失败退避（`nextAttemptAt` 未到）是两套独立的
  时间门槛，认领时同时检查；重试不会清除 `deliverAt`。
- 暂停只影响"能否被认领"，不改变事件状态与尝试计数；恢复后重试/死信规则照常生效。
- 背压拒绝发生在提交阶段，与投递阶段的重试/死信互不影响；`DELAY_EXCEEDED`
  死信与重试耗尽死信走同一个 `redeliver` 重投通道。

### 兼容范围

- 快照格式向后兼容：旧快照缺少 `deliverAt` / `pausedAggregates` 字段时按
  "立即可投递 / 无暂停"处理；新快照的新增字段对旧代码透明（Jackson 忽略未知字段）。
- 既有 API 签名全部保留（`submit` 四参、`claimNext` 三参、`commitBusinessWithEvent` 五参），
  新语义通过重载方法提供；不配置新参数时行为与上一版完全一致。
- 行为变化点：聚合内从"跳过未到点事件继续投递后续事件"收紧为"队首阻塞"，
  同一聚合前序事件退避/延迟期间，后续事件不再抢先（顺序保证更强）。

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
| `pipeline.max-delay` | 24h | 延迟投递最大等待时长，超限以 DELAY_EXCEEDED 死信 |
| `pipeline.max-backlog-per-aggregate` | 1000 | 单聚合最大积压，触顶拒绝新提交；<=0 不限制 |

## 本地验证

### 运行测试

```bash
mvn test
```

测试覆盖：原子提交与幂等提交、聚合内顺序、并发认领唯一性、重试耗尽死信、
不可重试死信、死信重投与非法重投拒绝、失败聚合隔离、认领超时回收、
并发重复投递副作用仅生效一次、重启恢复与死信状态保留；
延迟投递（到点前不可认领、聚合内队首阻塞、重启保留等待、超限 DELAY_EXCEEDED 死信）、
聚合暂停与恢复（暂停期不可认领且不计失败、恢复后按序继续、暂停状态跨重启保留）、
积压水位与背压（按聚合/全局水位统计、触顶原子拒绝、排空后恢复接受、幂等重复不受限）。
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

# 延迟投递：10 分钟后才可被认领（如下单后延迟关单）
curl -X POST localhost:8080/api/pipeline/events -H 'Content-Type: application/json' -d \
  '{"idempotencyKey":"order-close-1","aggregateId":"order","businessData":"{}","payload":"close", "deliverAt":"2026-10-06T12:30:00Z"}'

# 暂停/恢复某个聚合的投递（如商家故障时临时冻结）
curl -X POST localhost:8080/api/pipeline/aggregates/merchant-1/pause
curl -X POST localhost:8080/api/pipeline/aggregates/merchant-1/resume

# 查看积压水位（全局 + 按聚合）
curl localhost:8080/api/pipeline/backlog

# 背压验证：将 pipeline.max-backlog-per-aggregate 调小后打满某聚合，
# 新提交返回 429 {"error":"BACKPRESSURE_REJECTED", ...}，且业务与事件均未落库

# 验证重启恢复：kill 进程后重新启动，未完成事件会继续处理，
# 未到点的延迟事件与暂停状态保持原样
```
