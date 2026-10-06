package com.github.highcumontoa.reliableeventpipelinejava.pipeline;

import com.github.highcumontoa.reliableeventpipelinejava.core.BacklogStats;
import com.github.highcumontoa.reliableeventpipelinejava.core.ClaimDecision;
import com.github.highcumontoa.reliableeventpipelinejava.core.FailureKind;
import com.github.highcumontoa.reliableeventpipelinejava.core.PipelineEvent;
import com.github.highcumontoa.reliableeventpipelinejava.core.PipelineStats;
import com.github.highcumontoa.reliableeventpipelinejava.handler.EffectContext;
import com.github.highcumontoa.reliableeventpipelinejava.handler.EventHandler;
import com.github.highcumontoa.reliableeventpipelinejava.handler.EventProcessingException;
import com.github.highcumontoa.reliableeventpipelinejava.store.EventStore;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 可靠事件管道：原子提交入口 + 后台派发循环。
 * 派发线程认领事件后调用业务处理器，按结果确认或按退避重试/死信。
 */
public class EventPipeline {

    private static final Logger log = LoggerFactory.getLogger(EventPipeline.class);

    private final EventStore store;
    private final PipelineProperties properties;
    private final List<ExecutorService> workers = new CopyOnWriteArrayList<>();
    private final AtomicBoolean running = new AtomicBoolean(false);

    public EventPipeline(EventStore store, PipelineProperties properties) {
        this.store = store;
        this.properties = properties;
    }

    /** 注册业务处理器并启动派发线程。 */
    public void start(EventHandler handler) {
        if (!running.compareAndSet(false, true)) {
            return;
        }
        ExecutorService executor = Executors.newFixedThreadPool(properties.getWorkerThreads(),
                runnable -> {
                    Thread t = new Thread(runnable, "event-pipeline-worker");
                    t.setDaemon(true);
                    return t;
                });
        workers.add(executor);
        for (int i = 0; i < properties.getWorkerThreads(); i++) {
            String owner = "worker-" + i + "-" + UUID8.random();
            executor.submit(() -> dispatchLoop(handler, owner));
        }
    }

    /** 停止派发线程。 */
    public void stop() {
        running.set(false);
        for (ExecutorService executor : workers) {
            executor.shutdownNow();
        }
        for (ExecutorService executor : workers) {
            try {
                executor.awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        workers.clear();
    }

    /** 业务写入与事件入队的原子提交；重复幂等键不产生重复事件。 */
    public PipelineEvent submit(String idempotencyKey, String aggregateId, String businessData, String payload) {
        return submit(idempotencyKey, aggregateId, businessData, payload, null);
    }

    /**
     * 业务写入与事件入队的原子提交，可指定最早可投递时间。
     * 聚合积压达到上限时抛 BackpressureRejectedException（业务与事件均未写入）；
     * 剩余等待超过 maxDelay 时事件以 DELAY_EXCEEDED 死信落库。
     */
    public PipelineEvent submit(String idempotencyKey, String aggregateId,
            String businessData, String payload, Instant deliverAt) {
        return store.commitBusinessWithEvent(idempotencyKey, aggregateId, businessData, payload,
                deliverAt, properties.getMaxDelay(), properties.getMaxBacklogPerAggregate(),
                Instant.now());
    }

    /** 暂停某个聚合的投递。 */
    public void pauseAggregate(String aggregateId) {
        store.pauseAggregate(aggregateId);
        log.info("聚合已暂停投递 aggregate={}", aggregateId);
    }

    /** 恢复某个聚合的投递，从暂停前的位置按顺序继续。 */
    public void resumeAggregate(String aggregateId) {
        store.resumeAggregate(aggregateId);
        log.info("聚合已恢复投递 aggregate={}", aggregateId);
    }

    public boolean isPaused(String aggregateId) {
        return store.isPaused(aggregateId);
    }

    /** 积压水位：全局与按聚合的未完成事件数及最久积压时长。 */
    public BacklogStats backlogStats() {
        return store.backlogStats(Instant.now());
    }

    /** 死信重投；状态非法时抛 IllegalRedeliveryException。 */
    public PipelineEvent redeliver(String eventId) {
        return store.redeliver(eventId, Instant.now());
    }

    public Optional<PipelineEvent> findEvent(String eventId) {
        return store.findEvent(eventId);
    }

    public List<PipelineEvent> listEvents() {
        return store.listEvents();
    }

    public PipelineStats stats() {
        return store.stats();
    }

    private void dispatchLoop(EventHandler handler, String owner) {
        while (running.get() && !Thread.currentThread().isInterrupted()) {
            Optional<PipelineEvent> claimed;
            try {
                claimed = store.claimNext(owner, properties.getClaimTimeout(),
                        properties.getMaxDelay(), Instant.now());
            } catch (RuntimeException e) {
                log.error("认领事件失败 owner={}", owner, e);
                sleep();
                continue;
            }
            if (claimed.isEmpty()) {
                sleep();
                continue;
            }
            process(handler, owner, claimed.get());
        }
    }

    private void process(EventHandler handler, String owner, PipelineEvent event) {
        try {
            handler.handle(event, new StoreEffectContext(event.eventId, owner));
            boolean acked = store.ack(event.eventId, owner, Instant.now());
            log.info("事件确认 eventId={} aggregate={} seq={} attempt={} acked={}",
                    event.eventId, event.aggregateId, event.seq, event.deliveryAttempts, acked);
        } catch (EventProcessingException e) {
            store.fail(event.eventId, owner, e.kind(), e.getMessage(),
                    properties.getMaxDeliveryAttempts(), properties.getBackoffBase(), Instant.now());
            log.warn("事件处理失败 eventId={} attempt={} kind={} reason={}",
                    event.eventId, event.deliveryAttempts, e.kind(), e.getMessage());
        } catch (RuntimeException e) {
            // 未分类异常按可重试处理。
            store.fail(event.eventId, owner, FailureKind.RETRYABLE, String.valueOf(e.getMessage()),
                    properties.getMaxDeliveryAttempts(), properties.getBackoffBase(), Instant.now());
            log.warn("事件处理异常 eventId={} attempt={} kind=RETRYABLE reason={}",
                    event.eventId, event.deliveryAttempts, e.toString());
        }
    }

    private void sleep() {
        try {
            Thread.sleep(properties.getPollInterval().toMillis());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /** 基于存储的幂等副作用上下文。 */
    private final class StoreEffectContext implements EffectContext {
        private final String eventId;
        private final String owner;

        private StoreEffectContext(String eventId, String owner) {
            this.eventId = eventId;
            this.owner = owner;
        }

        @Override
        public ClaimDecision tryBeginEffect() {
            return store.tryBeginEffect(eventId, owner, properties.getEffectLeaseTimeout(), Instant.now());
        }

        @Override
        public void completeEffect() {
            store.completeEffect(eventId, owner, Instant.now());
        }
    }

    /** 简短随机标识，区分不同运行实例的认领者。 */
    private static final class UUID8 {
        static String random() {
            return java.util.UUID.randomUUID().toString().substring(0, 8);
        }
    }
}
