package com.github.highcumontoa.reliableeventpipelinejava.store;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.github.highcumontoa.reliableeventpipelinejava.core.BacklogStats;
import com.github.highcumontoa.reliableeventpipelinejava.core.BusinessRecord;
import com.github.highcumontoa.reliableeventpipelinejava.core.ClaimDecision;
import com.github.highcumontoa.reliableeventpipelinejava.core.DeadReason;
import com.github.highcumontoa.reliableeventpipelinejava.core.EffectRecord;
import com.github.highcumontoa.reliableeventpipelinejava.core.EffectState;
import com.github.highcumontoa.reliableeventpipelinejava.core.EventStatus;
import com.github.highcumontoa.reliableeventpipelinejava.core.FailureKind;
import com.github.highcumontoa.reliableeventpipelinejava.core.PipelineEvent;
import com.github.highcumontoa.reliableeventpipelinejava.core.PipelineStats;
import com.github.highcumontoa.reliableeventpipelinejava.pipeline.BackpressureRejectedException;
import com.github.highcumontoa.reliableeventpipelinejava.pipeline.EventNotFoundException;
import com.github.highcumontoa.reliableeventpipelinejava.pipeline.IllegalRedeliveryException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

/**
 * 进程内可恢复存储：内存索引 + JSON 快照落盘。
 * 所有状态变更在单锁内完成，并在锁内原子写盘（临时文件 + 原子移动），
 * 以此模拟消息中间件的持久化语义：业务写入与事件入队同生共死，
 * 进程重启后从快照恢复，未完成的事件可继续处理。
 */
public class EventStore {

    private final Path storeFile;
    private final ObjectMapper mapper;
    private final Object lock = new Object();

    private final Map<String, PipelineEvent> events = new LinkedHashMap<>();
    private final Map<String, String> eventIdByIdempotencyKey = new HashMap<>();
    private final Map<String, BusinessRecord> businessRecords = new LinkedHashMap<>();
    private final Map<String, EffectRecord> effects = new HashMap<>();
    private final Map<String, Long> nextSeqByAggregate = new HashMap<>();
    private final Set<String> pausedAggregates = new HashSet<>();

    public EventStore(Path storeFile) {
        this.storeFile = storeFile;
        this.mapper = new ObjectMapper().findAndRegisterModules()
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    }

    /** 启动时加载快照；文件不存在则视为空库。 */
    public void load() {
        synchronized (lock) {
            if (!Files.exists(storeFile)) {
                return;
            }
            try {
                Snapshot snapshot = mapper.readValue(storeFile.toFile(), Snapshot.class);
                events.clear();
                eventIdByIdempotencyKey.clear();
                businessRecords.clear();
                effects.clear();
                nextSeqByAggregate.clear();
                pausedAggregates.clear();
                for (PipelineEvent e : nullToEmpty(snapshot.events)) {
                    events.put(e.eventId, e);
                    eventIdByIdempotencyKey.put(e.idempotencyKey, e.eventId);
                }
                for (BusinessRecord b : nullToEmpty(snapshot.businessRecords)) {
                    businessRecords.put(b.idempotencyKey, b);
                }
                for (EffectRecord r : nullToEmpty(snapshot.effects)) {
                    effects.put(r.eventId, r);
                }
                if (snapshot.nextSeqByAggregate != null) {
                    nextSeqByAggregate.putAll(snapshot.nextSeqByAggregate);
                }
                if (snapshot.pausedAggregates != null) {
                    pausedAggregates.addAll(snapshot.pausedAggregates);
                }
            } catch (IOException e) {
                throw new UncheckedIOException("加载存储快照失败: " + storeFile, e);
            }
        }
    }

    /**
     * 业务写入与事件入队的原子提交。
     * 同一 idempotencyKey 重复提交直接返回已存在事件，不产生重复业务记录与事件。
     */
    public PipelineEvent commitBusinessWithEvent(String idempotencyKey, String aggregateId,
            String businessData, String payload, Instant now) {
        return commitBusinessWithEvent(idempotencyKey, aggregateId, businessData, payload,
                null, null, 0, now);
    }

    /**
     * 业务写入与事件入队的原子提交（完整语义）。
     * <ul>
     *   <li>幂等：同一 idempotencyKey 重复提交直接返回已存在事件，不触发背压检查。</li>
     *   <li>背压：maxBacklogPerAggregate &gt; 0 且该聚合未完成事件数已达上限时，
     *       抛 BackpressureRejectedException，业务记录与事件都不写入（原子地整体失败）。</li>
     *   <li>延迟：deliverAt 非空时事件在该时间之前不可被认领；若剩余等待超过 maxDelay，
     *       事件直接以 DELAY_EXCEEDED 死信落库（业务记录仍与事件同事务提交，可排查后重投）。</li>
     * </ul>
     */
    public PipelineEvent commitBusinessWithEvent(String idempotencyKey, String aggregateId,
            String businessData, String payload, Instant deliverAt,
            Duration maxDelay, int maxBacklogPerAggregate, Instant now) {
        synchronized (lock) {
            String existingId = eventIdByIdempotencyKey.get(idempotencyKey);
            if (existingId != null) {
                return copy(events.get(existingId));
            }
            if (maxBacklogPerAggregate > 0) {
                long backlog = 0;
                for (PipelineEvent e : events.values()) {
                    if (e.aggregateId.equals(aggregateId)
                            && (e.status == EventStatus.PENDING || e.status == EventStatus.IN_FLIGHT)) {
                        backlog++;
                    }
                }
                if (backlog >= maxBacklogPerAggregate) {
                    // 拒绝发生在任何写入之前：业务与事件都不会落库，不存在半截状态。
                    throw new BackpressureRejectedException(aggregateId, backlog, maxBacklogPerAggregate);
                }
            }
            BusinessRecord record = new BusinessRecord();
            record.idempotencyKey = idempotencyKey;
            record.aggregateId = aggregateId;
            record.data = businessData;
            record.createdAt = now;

            PipelineEvent event = new PipelineEvent();
            event.eventId = UUID.randomUUID().toString();
            event.idempotencyKey = idempotencyKey;
            event.aggregateId = aggregateId;
            event.seq = nextSeqByAggregate.merge(aggregateId, 1L, Long::sum) - 1;
            event.payload = payload;
            event.status = EventStatus.PENDING;
            event.deliveryAttempts = 0;
            event.redeliveries = 0;
            event.nextAttemptAt = now;
            event.deliverAt = deliverAt;
            event.createdAt = now;
            event.updatedAt = now;
            if (deliverAt != null && maxDelay != null && deliverAt.isAfter(now.plus(maxDelay))) {
                // 等待超过上限：不能一直挂着，以可区分的死信原因处理掉。
                event.status = EventStatus.DEAD;
                event.deadReason = DeadReason.DELAY_EXCEEDED;
                event.lastError = "延迟等待超过上限 " + maxDelay + ": deliverAt=" + deliverAt;
            }

            // 同一锁内同时写入业务记录与事件并落盘：要么都成功，要么都失败。
            businessRecords.put(idempotencyKey, record);
            events.put(event.eventId, event);
            eventIdByIdempotencyKey.put(idempotencyKey, event.eventId);
            persistLocked();
            return copy(event);
        }
    }

    /**
     * 认领一个可投递事件。
     * 先把超时未确认的 IN_FLIGHT 事件回收为 PENDING（重新认领），
     * 再跳过存在在途事件的聚合，保证同一聚合按提交顺序逐个投递。
     */
    public Optional<PipelineEvent> claimNext(String owner, Duration claimTimeout, Instant now) {
        return claimNext(owner, claimTimeout, null, now);
    }

    /**
     * 认领一个可投递事件（完整语义）。
     * <ul>
     *   <li>回收超时认领；清扫剩余等待超过 maxDelay 的延迟事件为 DELAY_EXCEEDED 死信。</li>
     *   <li>跳过暂停的聚合；跳过 deliverAt 未到的事件。</li>
     *   <li>聚合内队首阻塞：只有该聚合最早未完成（PENDING/IN_FLIGHT）的事件可被认领，
     *       前面的事件没到点或还在途时，后面的事件不能抢先。</li>
     * </ul>
     */
    public Optional<PipelineEvent> claimNext(String owner, Duration claimTimeout,
            Duration maxDelay, Instant now) {
        synchronized (lock) {
            boolean changed = reclaimExpiredLocked(now);
            changed |= expireOverdelayedLocked(maxDelay, now);
            Set<String> busyAggregates = new HashSet<>();
            Map<String, Long> headSeqByAggregate = new HashMap<>();
            for (PipelineEvent e : events.values()) {
                if (e.status == EventStatus.IN_FLIGHT) {
                    busyAggregates.add(e.aggregateId);
                }
                if (e.status == EventStatus.PENDING || e.status == EventStatus.IN_FLIGHT) {
                    headSeqByAggregate.merge(e.aggregateId, e.seq, Math::min);
                }
            }
            for (PipelineEvent e : events.values()) {
                if (e.status != EventStatus.PENDING
                        || busyAggregates.contains(e.aggregateId)
                        || pausedAggregates.contains(e.aggregateId)
                        || e.nextAttemptAt.isAfter(now)
                        || (e.deliverAt != null && e.deliverAt.isAfter(now))
                        || e.seq != headSeqByAggregate.get(e.aggregateId)) {
                    continue;
                }
                e.status = EventStatus.IN_FLIGHT;
                e.claimedBy = owner;
                e.claimExpiresAt = now.plus(claimTimeout);
                e.deliveryAttempts++;
                e.updatedAt = now;
                persistLocked();
                return Optional.of(copy(e));
            }
            if (changed) {
                persistLocked();
            }
            return Optional.empty();
        }
    }

    /** 确认处理成功。仅当前认领持有者可确认。 */
    public boolean ack(String eventId, String owner, Instant now) {
        synchronized (lock) {
            PipelineEvent e = events.get(eventId);
            if (e == null || e.status != EventStatus.IN_FLIGHT || !owner.equals(e.claimedBy)) {
                return false;
            }
            e.status = EventStatus.ACKED;
            e.claimedBy = null;
            e.claimExpiresAt = null;
            e.updatedAt = now;
            persistLocked();
            return true;
        }
    }

    /** 记录一次失败：可重试故障按指数退避重排，超过上限或不可重试进入死信。 */
    public void fail(String eventId, String owner, FailureKind kind,
            String error, int maxAttempts, Duration backoffBase, Instant now) {
        synchronized (lock) {
            PipelineEvent e = events.get(eventId);
            if (e == null || e.status != EventStatus.IN_FLIGHT || !owner.equals(e.claimedBy)) {
                return;
            }
            e.lastError = error;
            e.claimedBy = null;
            e.claimExpiresAt = null;
            e.updatedAt = now;
            if (kind == FailureKind.NON_RETRYABLE) {
                e.status = EventStatus.DEAD;
                e.deadReason = DeadReason.NON_RETRYABLE_FAILURE;
            } else if (e.deliveryAttempts >= maxAttempts) {
                e.status = EventStatus.DEAD;
                e.deadReason = DeadReason.RETRIES_EXHAUSTED;
            } else {
                e.status = EventStatus.PENDING;
                long multiplier = 1L << Math.min(e.deliveryAttempts - 1, 20);
                e.nextAttemptAt = now.plusMillis(backoffBase.toMillis() * multiplier);
            }
            persistLocked();
        }
    }

    /** 死信重投：仅 DEAD 状态允许，否则抛 IllegalRedeliveryException。 */
    public PipelineEvent redeliver(String eventId, Instant now) {
        synchronized (lock) {
            PipelineEvent e = events.get(eventId);
            if (e == null) {
                throw new EventNotFoundException("事件不存在: " + eventId);
            }
            if (e.status != EventStatus.DEAD) {
                throw new IllegalRedeliveryException(
                        "仅 DEAD 状态可重投, 事件 " + eventId + " 当前状态: " + e.status);
            }
            e.status = EventStatus.PENDING;
            e.deliveryAttempts = 0;
            e.redeliveries++;
            e.deadReason = null;
            e.lastError = null;
            e.nextAttemptAt = now;
            e.updatedAt = now;
            persistLocked();
            return copy(e);
        }
    }

    /** 暂停某个聚合的投递：暂停期间该聚合的事件不可被认领，其他聚合不受影响。 */
    public void pauseAggregate(String aggregateId) {
        synchronized (lock) {
            if (pausedAggregates.add(aggregateId)) {
                persistLocked();
            }
        }
    }

    /** 恢复某个聚合的投递：从暂停前的位置按顺序继续，暂停期间的事件不丢不算失败。 */
    public void resumeAggregate(String aggregateId) {
        synchronized (lock) {
            if (pausedAggregates.remove(aggregateId)) {
                persistLocked();
            }
        }
    }

    public boolean isPaused(String aggregateId) {
        synchronized (lock) {
            return pausedAggregates.contains(aggregateId);
        }
    }

    public Set<String> pausedAggregates() {
        synchronized (lock) {
            return Set.copyOf(pausedAggregates);
        }
    }

    /** 尝试认领副作用执行租约：DONE 跳过；他人持有未过期租约则 BUSY；否则获得执行权。 */
    public ClaimDecision tryBeginEffect(String eventId, String owner, Duration leaseTimeout, Instant now) {
        synchronized (lock) {
            EffectRecord record = effects.computeIfAbsent(eventId, id -> {
                EffectRecord r = new EffectRecord();
                r.eventId = id;
                return r;
            });
            if (record.state == EffectState.DONE) {
                return ClaimDecision.SKIP_ALREADY_DONE;
            }
            if (record.state == EffectState.EXECUTING
                    && record.leaseExpiresAt != null && record.leaseExpiresAt.isAfter(now)
                    && !owner.equals(record.leaseOwner)) {
                return ClaimDecision.BUSY;
            }
            record.state = EffectState.EXECUTING;
            record.leaseOwner = owner;
            record.leaseExpiresAt = now.plus(leaseTimeout);
            persistLocked();
            return ClaimDecision.EXECUTE;
        }
    }

    /** 标记副作用完成。仅租约持有者可完成。 */
    public void completeEffect(String eventId, String owner, Instant now) {
        synchronized (lock) {
            EffectRecord record = effects.get(eventId);
            if (record == null || !owner.equals(record.leaseOwner)) {
                return;
            }
            record.state = EffectState.DONE;
            record.completedAt = now;
            record.leaseOwner = null;
            record.leaseExpiresAt = null;
            persistLocked();
        }
    }

    public Optional<PipelineEvent> findEvent(String eventId) {
        synchronized (lock) {
            return Optional.ofNullable(copy(events.get(eventId)));
        }
    }

    public Optional<PipelineEvent> findEventByIdempotencyKey(String idempotencyKey) {
        synchronized (lock) {
            String id = eventIdByIdempotencyKey.get(idempotencyKey);
            return Optional.ofNullable(id == null ? null : copy(events.get(id)));
        }
    }

    public Optional<EffectRecord> findEffect(String eventId) {
        synchronized (lock) {
            return Optional.ofNullable(copy(effects.get(eventId)));
        }
    }

    public List<PipelineEvent> listEvents() {
        synchronized (lock) {
            List<PipelineEvent> result = new ArrayList<>();
            for (PipelineEvent e : events.values()) {
                result.add(copy(e));
            }
            return result;
        }
    }

    public List<BusinessRecord> listBusinessRecords() {
        synchronized (lock) {
            List<BusinessRecord> result = new ArrayList<>();
            for (BusinessRecord b : businessRecords.values()) {
                result.add(copy(b));
            }
            return result;
        }
    }

    /** 统计量始终由事件实际状态推导，保证与处理结果一致。 */
    public PipelineStats stats() {
        synchronized (lock) {
            int pending = 0, inFlight = 0, acked = 0, dead = 0;
            for (PipelineEvent e : events.values()) {
                switch (e.status) {
                    case PENDING -> pending++;
                    case IN_FLIGHT -> inFlight++;
                    case ACKED -> acked++;
                    case DEAD -> dead++;
                }
            }
            int effectsDone = 0;
            for (EffectRecord r : effects.values()) {
                if (r.state == EffectState.DONE) {
                    effectsDone++;
                }
            }
            return new PipelineStats(events.size(), pending, inFlight, acked, dead,
                    businessRecords.size(), effectsDone);
        }
    }

    /** 回收超时未确认的认领，使其可被重新认领。 */
    private boolean reclaimExpiredLocked(Instant now) {
        boolean changed = false;
        for (PipelineEvent e : events.values()) {
            if (e.status == EventStatus.IN_FLIGHT
                    && e.claimExpiresAt != null && !e.claimExpiresAt.isAfter(now)) {
                e.status = EventStatus.PENDING;
                e.claimedBy = null;
                e.claimExpiresAt = null;
                e.nextAttemptAt = now;
                e.updatedAt = now;
                changed = true;
            }
        }
        return changed;
    }

    /** 清扫剩余等待超过上限的延迟事件：以 DELAY_EXCEEDED 死信处理，不能一直挂着。 */
    private boolean expireOverdelayedLocked(Duration maxDelay, Instant now) {
        if (maxDelay == null) {
            return false;
        }
        boolean changed = false;
        for (PipelineEvent e : events.values()) {
            if (e.status == EventStatus.PENDING
                    && e.deliverAt != null && e.deliverAt.isAfter(now.plus(maxDelay))) {
                e.status = EventStatus.DEAD;
                e.deadReason = DeadReason.DELAY_EXCEEDED;
                e.lastError = "延迟等待超过上限 " + maxDelay + ": deliverAt=" + e.deliverAt;
                e.updatedAt = now;
                changed = true;
            }
        }
        return changed;
    }

    /** 积压水位：按聚合与全局统计未完成（PENDING + IN_FLIGHT）事件数与最久积压时长。 */
    public BacklogStats backlogStats(Instant now) {
        synchronized (lock) {
            Map<String, long[]> byAggregate = new HashMap<>(); // aggregateId -> [count, oldestCreatedAtMillis]
            long total = 0;
            Long oldestMillis = null;
            for (PipelineEvent e : events.values()) {
                if (e.status != EventStatus.PENDING && e.status != EventStatus.IN_FLIGHT) {
                    continue;
                }
                total++;
                long created = e.createdAt.toEpochMilli();
                long[] agg = byAggregate.computeIfAbsent(e.aggregateId,
                        k -> new long[] {0L, Long.MAX_VALUE});
                agg[0]++;
                agg[1] = Math.min(agg[1], created);
                oldestMillis = oldestMillis == null ? created : Math.min(oldestMillis, created);
            }
            Map<String, BacklogStats.AggregateBacklog> detail = new LinkedHashMap<>();
            byAggregate.forEach((aggregateId, agg) -> detail.put(aggregateId,
                    new BacklogStats.AggregateBacklog(agg[0],
                            Math.max(0, now.toEpochMilli() - agg[1]))));
            return new BacklogStats(total,
                    oldestMillis == null ? null : Math.max(0, now.toEpochMilli() - oldestMillis),
                    detail);
        }
    }

    /** 锁内原子写盘：先写临时文件再原子移动，避免半写状态。 */
    private void persistLocked() {
        Snapshot snapshot = new Snapshot();
        snapshot.events = new ArrayList<>(events.values());
        snapshot.businessRecords = new ArrayList<>(businessRecords.values());
        snapshot.effects = new ArrayList<>(effects.values());
        snapshot.nextSeqByAggregate = new HashMap<>(nextSeqByAggregate);
        snapshot.pausedAggregates = new HashSet<>(pausedAggregates);
        try {
            Path parent = storeFile.toAbsolutePath().getParent();
            if (parent != null) {
                Files.createDirectories(parent);
            }
            Path tmp = storeFile.resolveSibling(storeFile.getFileName() + ".tmp");
            mapper.writeValue(tmp.toFile(), snapshot);
            Files.move(tmp, storeFile,
                    StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException e) {
            throw new UncheckedIOException("持久化存储快照失败: " + storeFile, e);
        }
    }

    private PipelineEvent copy(PipelineEvent e) {
        return e == null ? null : mapper.convertValue(e, PipelineEvent.class);
    }

    private BusinessRecord copy(BusinessRecord b) {
        return b == null ? null : mapper.convertValue(b, BusinessRecord.class);
    }

    private EffectRecord copy(EffectRecord r) {
        return r == null ? null : mapper.convertValue(r, EffectRecord.class);
    }

    private static <T> List<T> nullToEmpty(List<T> list) {
        return list == null ? List.of() : list;
    }

    /** 落盘快照结构。 */
    public static class Snapshot {
        public List<PipelineEvent> events;
        public List<BusinessRecord> businessRecords;
        public List<EffectRecord> effects;
        public Map<String, Long> nextSeqByAggregate;
        public Set<String> pausedAggregates;
    }
}
