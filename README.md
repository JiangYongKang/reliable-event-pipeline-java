# 本地可靠事件处理管道

基于 Spring Boot 的进程内可靠事件管道：用**可恢复的本地存储**（内存索引 + JSON 快照原子落盘）
模拟消息中间件，保证事件在并发投递、失败重试与服务重启后的状态一致且可解释。

## 架构

```
业务代码 ──submit──▶ EventPipeline ──commitBusinessWithEvent──▶ EventStore(单锁 + 原子快照)
                                                              │
派发线程 ◀──claimNext──┘                                       ▼
   │   认领门控：仅聚合队头 & 已到点(notBefore/退避)              data/pipeline-store.json
   │             & 聚合未暂停 & 未在途；延迟超 max-delay → 死信
   ▼ handle(event, EffectContext)
EventHandler（业务副作用，幂等）
   │ 成功 ack / 失败 fail(RETRYABLE|NON_RETRYABLE)
   ▼
PENDING → IN_FLIGHT → ACKED
   ↑          │
   └─退避重排──┘  超过上限/不可重试/延迟超限 → DEAD（可 redeliver 重投）

提交侧：pauseAggregate/resumeAggregate（聚合冻结）；max-backlog-per-aggregate 触顶 → 明确拒绝(429)
```

- `store/EventStore`：所有状态变更在单锁内完成并原子写盘（临时文件 + `ATOMIC_MOVE`），
  模拟中间件的持久化语义；重启时 `load()` 从快照恢复。
- `pipeline/EventPipeline`：原子提交入口 + 后台派发循环，按处理结果确认或重试/死信。
- `handler/EventHandler` + `EffectContext`：业务处理器与幂等副作用上下文。

## 投递语义

1. **原子提交**：`submit(idempotencyKey, aggregateId, businessData, payload)` 在同一锁内
   同时写入业务记录与事件并落盘——要么都成功，要么都失败，不存在只有事件没有业务数据
   （或相反）的中间状态。
2. **幂等提交**：同一 `idempotencyKey` 重复提交直接返回已存在事件，不产生重复业务记录与事件；
   即使该聚合已触达积压上限，幂等重复提交仍返回原事件而不会被拒绝。
3. **聚合内严格有序**：每个事件携带聚合内递增序号 `seq`；认领时每个聚合只有其**队头**
   （seq 最小的 PENDING 事件）且不存在在途事件时才可能被认领。后到的事件（含已到期的延迟
   事件）不能越过尚未处理完的队头；不同聚合并行处理，单个聚合故障/冻结不阻塞其他聚合。
4. **认领与超时回收**：派发线程通过 `claimNext` 认领事件，获得 `claim-timeout` 租约；
   超时未确认的事件被回收为 PENDING，可被重新认领（崩溃/慢处理的恢复路径）。
5. **幂等消费**：业务副作用通过 `EffectContext.tryBeginEffect()/completeEffect()` 执行，
   由持久化的副作用账本（NONE/EXECUTING/DONE + 执行租约）保证同一事件的副作用只生效一次：
   - `EXECUTE`：获得执行权，应执行副作用并 `completeEffect()`；
   - `SKIP_ALREADY_DONE`：副作用已完成，直接返回即可确认；
   - `BUSY`：他人正在执行，应抛出可重试异常稍后重试。
6. **统计一致性**：`stats()` 的各状态计数与副作用完成数始终由存储中的实际事件状态推导。

## 投递时机规则（新增）

### 延迟投递（下单后延迟自动关单等场景）

- 提交时可指定最早可投递时间：
  `submitDelayed(key, aggregate, data, payload, Instant notBefore)`
  或相对时间版本 `submitDelayed(..., Duration delay)`。
- 到点之前事件保持 PENDING，**任何 worker 都不能认领**；`nextAttemptAt` 同时承载
  “延迟到期/退避到期”的最早可认领时刻。
- **聚合内仍按提交先后走**：只有队头事件可被认领。前面的事件没处理完，后面的延迟事件
  即便已到点也不能抢先。
- **跨进程重启**：`notBefore` 与事件一起落在快照里，重启后未到点的等待继续保留，
  不会因重启被提前放出。
- **等待上限 `pipeline.max-delay`（默认 24h，0=不限）**：仍处于“按业务指定时间等待”
  （`notBefore` 未到）的事件，自其**提交时刻**起等待超过该上限，会被转入死信，原因
  `DELAY_LIMIT_EXCEEDED`——与处理失败导致的 `RETRIES_EXHAUSTED` / `NON_RETRYABLE_FAILURE`
  明确区分。注意：因队头未处理完而排队的事件不算“挂着没投”，不受此上限影响。

### 聚合级暂停与恢复（故障商家临时冻结等场景）

- `pauseAggregate(aggregateId)`：暂停期间该聚合的事件一条都不会被认领；其他聚合照常处理。
  暂停前已被认领（IN_FLIGHT）的事件仍可正常确认，不强制中断。
- `resumeAggregate(aggregateId)`：从暂停前的队头位置按 seq **继续**，不跳序、不漏事件，
  暂停期间事件保持 PENDING，**不计失败、不进死信**。
- 暂停状态随快照持久化，进程重启后仍然生效；可用 `isAggregatePaused` / 积压视图查询。

### 积压水位与背压（忙时别把单个聚合堆爆）

- `backlog()` 返回 `BacklogView`：全局未完成（PENDING/IN_FLIGHT）事件数、最久积压时长，
  以及每个聚合的未完成数、最久积压时长、是否暂停。
- 当某聚合未完成事件数达到 `pipeline.max-backlog-per-aggregate`（默认 0=不限）时，
  **之后的新提交一律拒绝**，抛出 `BacklogLimitExceededException`（携带 aggregateId /
  currentBacklog / limit；REST 映射为 **HTTP 429** 与错误码 `BACKLOG_LIMIT_EXCEEDED`），
  不静默丢弃。
- 拒绝发生在业务记录与事件**任何写入之前**（同一把锁内判定），因此不会出现业务数据落了、
  事件没落或反过来的半截状态；幂等键重复提交直接返回原事件，不受限流影响。
- 队头被 ACKED（或进入 DEAD 移出未完成集合）后积压下降，配额自动恢复，可继续提交。

## 与重试、死信的配合

- 延迟事件到期后进入正常认领通道，此后处理器失败仍按既有规则退避重试 / 死信；
  重试退避会把 `nextAttemptAt` 后移，但**不改变** `notBefore` 与等待上限的判定口径
  （上限只针对“业务指定时间未到”的纯等待阶段，退避等待不重复计入）。
- `DELAY_LIMIT_EXCEEDED` 是与两类失败原因并列的第三种 `DeadReason`；死信后同样可通过
  `redeliver(eventId)` 重投（重置为 PENDING、计数清零、`redeliveries+1`），仅 DEAD 可重投。
  对“延迟超限”死信的重投代表运维显式决定立即投递，会一并清除 `notBefore`，避免事件因等待
  时间仍在未来而立刻再次被上限打回。
- 暂停只影响“认领”，不改变事件状态、不消耗投递尝试次数，因此恢复后不会因暂停而触发重试
  或死信。
- 积压上限只统计 PENDING/IN_FLIGHT；ACKED 与 DEAD 不计入，积压下降后自动放行新提交。

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
| `pipeline.max-delay` | 24h | 延迟事件等待上限（自提交时刻起）；0 表示不限，超出且仍未到点的延迟事件死信 `DELAY_LIMIT_EXCEEDED` |
| `pipeline.max-backlog-per-aggregate` | 0 | 单聚合未完成事件数上限；0 表示不限，超过后新提交被拒绝（异常/HTTP 429） |
| `pipeline.store-file` | data/pipeline-store.json | 快照文件路径 |

## 本地验证

### 运行测试

```bash
mvn test
```

测试覆盖：原子提交与幂等提交、聚合内顺序、并发认领唯一性、重试耗尽死信、
不可重试死信、死信重投与非法重投拒绝、失败聚合隔离、认领超时回收、
并发重复投递副作用仅生效一次、重启恢复与死信状态保留；
以及本轮新增的 **延迟到期前后不可/可认领、延迟不得越过在途队头、延迟等待跨重启保留、
等待超限以 `DELAY_LIMIT_EXCEEDED` 死信、暂停期间零认领且其他聚合不受影响、恢复后原位按序
续跑、暂停状态跨重启、积压触顶拒绝反馈可区分且不留半截、幂等重试不被误拒、处理后容量恢复、
按聚合/全局的积压水位视图**。
单测日志会打印事件标识、尝试次数、场景与判定依据（`场景[...] ... 判定: ...`）。

### 运行服务并通过 REST 验证

```bash
mvn spring-boot:run

# 提交（业务写入 + 事件入队原子提交）
curl -X POST localhost:8080/api/pipeline/events -H 'Content-Type: application/json' -d \
  '{"idempotencyKey":"order-1","aggregateId":"order","businessData":"{}","payload":"created"}'

# 延迟提交（最早可投递时间 notBefore，ISO-8601）
curl -X POST localhost:8080/api/pipeline/events -H 'Content-Type: application/json' -d \
  '{"idempotencyKey":"order-2","aggregateId":"order","businessData":"{}","payload":"auto-close","notBefore":"2026-10-06T18:00:00Z"}'

# 暂停 / 恢复某聚合（如故障商家 merchant-7）
curl -X POST localhost:8080/api/pipeline/aggregates/merchant-7/pause
curl -X POST localhost:8080/api/pipeline/aggregates/merchant-7/resume
curl localhost:8080/api/pipeline/aggregates/merchant-7/paused

# 查看积压水位（全局 + 各聚合未完成数、最久积压时长、暂停标记）
curl localhost:8080/api/pipeline/backlog

# 积压触顶时新提交被明确拒绝：HTTP 429
# {"error":"BACKLOG_LIMIT_EXCEEDED","aggregateId":"...","currentBacklog":N,"limit":N}

# 查看事件与统计
curl localhost:8080/api/pipeline/events
curl localhost:8080/api/pipeline/stats

# 死信重投（仅 DEAD 状态允许；含 DELAY_LIMIT_EXCEEDED 的延迟死信）
curl -X POST localhost:8080/api/pipeline/events/{eventId}/redeliver

# 验证重启恢复：kill 进程后重新启动，未完成事件继续处理，
# 未到点的延迟等待与暂停状态也都会保留
```

## 兼容范围

- 新字段（事件 `notBefore`、快照 `pausedAggregates`、新配置项）对旧快照向后兼容：
  缺少这些字段的旧 `pipeline-store.json` 可直接加载；`notBefore` 为空即视为立即可投递，
  暂停集合为空。旧的 `submit(...)` / `commitBusinessWithEvent(...)` 签名与 REST 请求体保持
  可用（请求不带 `notBefore` 即普通提交）。
- 新增配置默认 `max-delay=24h`（普通即时事件没有等待窗口，不受影响）、
  `max-backlog-per-aggregate=0`（不限流），不改变既有部署的默认行为。
