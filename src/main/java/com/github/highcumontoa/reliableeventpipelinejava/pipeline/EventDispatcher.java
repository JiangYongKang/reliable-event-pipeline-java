package com.github.highcumontoa.reliableeventpipelinejava.pipeline;

import com.github.highcumontoa.reliableeventpipelinejava.event.DeadLetterReason;
import com.github.highcumontoa.reliableeventpipelinejava.event.EventStore;
import com.github.highcumontoa.reliableeventpipelinejava.event.StoredEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.util.List;
import java.util.UUID;

/**
 * 事件派发器：认领 -> 调用处理器 -> 确认 / 退避重试 / 死信隔离。
 *
 * <p>故障分类：{@link NonRetryableException} 直接死信；其他异常按
 * {@link RetryPolicy} 退避重试，达到最大尝试次数后死信。</p>
 */
public class EventDispatcher {

    private static final Logger log = LoggerFactory.getLogger(EventDispatcher.class);

    private final EventStore store;
    private final EventHandler handler;
    private final RetryPolicy retryPolicy;
    private final long claimTimeoutMs;
    private final Clock clock;
    private final String ownerId = "dispatcher-" + UUID.randomUUID();

    public EventDispatcher(EventStore store, EventHandler handler, RetryPolicy retryPolicy,
                           long claimTimeoutMs) {
        this(store, handler, retryPolicy, claimTimeoutMs, Clock.systemUTC());
    }

    public EventDispatcher(EventStore store, EventHandler handler, RetryPolicy retryPolicy,
                           long claimTimeoutMs, Clock clock) {
        this.store = store;
        this.handler = handler;
        this.retryPolicy = retryPolicy;
        this.claimTimeoutMs = claimTimeoutMs;
        this.clock = clock;
    }

    /**
     * 执行一轮派发，返回本轮成功确认的事件数。
     */
    public int runOnce() {
        long now = clock.millis();
        List<StoredEvent> batch = store.claimBatch(ownerId, 100, now, claimTimeoutMs);
        int delivered = 0;
        for (StoredEvent event : batch) {
            int nextAttempt = event.getAttempts() + 1;
            try {
                handler.handle(event);
                if (store.acknowledge(event.getId(), ownerId)) {
                    delivered++;
                }
                log.info("event={} attempt={} decision=ACK basis=handler-succeeded",
                        event.getId(), nextAttempt);
            } catch (NonRetryableException e) {
                store.failToDead(event.getId(), ownerId, DeadLetterReason.NON_RETRYABLE_FAILURE, e.getMessage());
                log.warn("event={} attempt={} decision=DEAD basis=non-retryable({})",
                        event.getId(), nextAttempt, e.getMessage());
            } catch (Exception e) {
                if (nextAttempt >= retryPolicy.maxAttempts()) {
                    store.failToDead(event.getId(), ownerId, DeadLetterReason.MAX_RETRIES_EXCEEDED, e.getMessage());
                    log.warn("event={} attempt={} decision=DEAD basis=max-retries-exceeded(max={}): {}",
                            event.getId(), nextAttempt, retryPolicy.maxAttempts(), e.getMessage());
                } else {
                    long backoff = retryPolicy.backoffMs(nextAttempt);
                    store.failRetryable(event.getId(), ownerId, now + backoff, e.getMessage());
                    log.warn("event={} attempt={} decision=RETRY basis=retryable, backoff={}ms: {}",
                            event.getId(), nextAttempt, backoff, e.getMessage());
                }
            }
        }
        return delivered;
    }
}
