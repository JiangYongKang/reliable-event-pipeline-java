package com.github.highcumontoa.reliableeventpipelinejava.event;

import com.github.highcumontoa.reliableeventpipelinejava.pipeline.PipelineStats;

import java.util.List;
import java.util.Optional;

/**
 * 进程内可恢复的事件存储（模拟消息中间件 + 业务库的原子提交）。
 */
public interface EventStore {

    /**
     * 原子提交：业务写入与事件入队要么同时生效，要么同时不生效。
     * 相同 operationKey 重复提交时幂等忽略。
     */
    CommitOutcome commitBusinessAndEvent(String operationKey, Runnable businessWrite, NewEvent event);

    /**
     * 并发安全地认领一批可投递事件（含租约超时回收），
     * 每个聚合只认领其队首事件以保证提交顺序。
     */
    List<StoredEvent> claimBatch(String owner, int max, long nowEpochMs, long claimTimeoutMs);

    /** 确认投递成功。仅持有有效租约的认领者可确认。 */
    boolean acknowledge(String eventId, String owner);

    /** 可重试失败：按退避时间重新挂起，累计尝试次数。 */
    boolean failRetryable(String eventId, String owner, long nextAvailableAtEpochMs, String error);

    /** 失败进入死信，记录可区分原因。 */
    boolean failToDead(String eventId, String owner, DeadLetterReason reason, String error);

    /** 死信重投：仅 DEAD 状态允许，其余状态抛出 IllegalStateException。 */
    void requeueDead(String eventId);

    /** 幂等消费：同一事件的业务副作用只生效一次，返回是否本次生效。 */
    boolean consumeOnce(String eventId, Runnable sideEffect);

    Optional<StoredEvent> find(String eventId);

    List<StoredEvent> findByAggregate(String aggregateId);

    PipelineStats stats();
}
