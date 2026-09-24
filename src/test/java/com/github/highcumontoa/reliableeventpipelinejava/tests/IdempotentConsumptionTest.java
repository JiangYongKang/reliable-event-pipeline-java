package com.github.highcumontoa.reliableeventpipelinejava.tests;

import static org.assertj.core.api.Assertions.assertThat;

import com.github.highcumontoa.reliableeventpipelinejava.core.ClaimDecision;
import com.github.highcumontoa.reliableeventpipelinejava.core.EffectState;
import com.github.highcumontoa.reliableeventpipelinejava.core.EventStatus;
import com.github.highcumontoa.reliableeventpipelinejava.core.PipelineEvent;
import com.github.highcumontoa.reliableeventpipelinejava.pipeline.EventPipeline;
import com.github.highcumontoa.reliableeventpipelinejava.pipeline.PipelineProperties;
import com.github.highcumontoa.reliableeventpipelinejava.store.EventStore;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** 幂等消费、认领租约超时回收、并发重复投递副作用仅生效一次。 */
class IdempotentConsumptionTest {

    private static final Logger log = LoggerFactory.getLogger(IdempotentConsumptionTest.class);

    @TempDir
    Path tempDir;

    @Test
    void timedOutClaimCanBeReclaimed() throws Exception {
        EventStore store = new EventStore(tempDir.resolve("timeout.json"));
        store.load();
        PipelineEvent event = store.commitBusinessWithEvent(
                "k-1", "agg-1", "d", "p", Instant.now());

        Optional<PipelineEvent> first = store.claimNext("owner-1", Duration.ofMillis(100), Instant.now());
        assertThat(first).isPresent();
        assertThat(store.findEvent(event.eventId).orElseThrow().status).isEqualTo(EventStatus.IN_FLIGHT);
        // 未确认且租约未到期：不能被他人认领
        Optional<PipelineEvent> tooEarly = store.claimNext("owner-2", Duration.ofMillis(100), Instant.now());
        assertThat(tooEarly).isEmpty();

        Thread.sleep(150);
        // 租约超时：回收后可被 owner-2 重新认领
        Optional<PipelineEvent> reclaimed = store.claimNext("owner-2", Duration.ofMillis(100), Instant.now());
        assertThat(reclaimed).isPresent();
        assertThat(reclaimed.get().claimedBy).isEqualTo("owner-2");
        log.info("判定依据: eventId={} 超时前认领者={}, 超时后被{}重新认领",
                event.eventId, "owner-1", reclaimed.get().claimedBy);
    }

    @Test
    void concurrentDuplicateDeliveryExecutesSideEffectOnce() throws Exception {
        EventStore store = new EventStore(tempDir.resolve("effect.json"));
        store.load();
        PipelineProperties props = new PipelineProperties();
        props.setWorkerThreads(3);
        props.setClaimTimeout(Duration.ofMillis(120));
        props.setEffectLeaseTimeout(Duration.ofSeconds(10));
        props.setPollInterval(Duration.ofMillis(5));
        EventPipeline pipeline = new EventPipeline(store, props);

        AtomicInteger sideEffects = new AtomicInteger();
        pipeline.start((event, context) -> {
            ClaimDecision decision = context.tryBeginEffect();
            if (decision == ClaimDecision.EXECUTE) {
                int n = sideEffects.incrementAndGet();
                log.info("eventId={} 第{}轮消费获得执行权, 判定依据: 副作用租约EXPIRED/NONE, 开始执行(耗时200ms)",
                        event.eventId, event.deliveryAttempts);
                // 执行时间超过认领租约，模拟崩溃/慢处理：认领会超时并被其他线程重新认领
                try { Thread.sleep(200); } catch (InterruptedException ie) { Thread.currentThread().interrupt(); }
                context.completeEffect();
                log.info("eventId={} 副作用第{}次生效完成", event.eventId, n);
            } else if (decision == ClaimDecision.SKIP_ALREADY_DONE) {
                log.info("eventId={} 第{}轮消费跳过, 判定依据: 副作用DONE",
                        event.eventId, event.deliveryAttempts);
            } else {
                log.info("eventId={} 第{}轮消费让出, 判定依据: 副作用租约被他人持有(BUSY)",
                        event.eventId, event.deliveryAttempts);
                throw com.github.highcumontoa.reliableeventpipelinejava.handler
                        .EventProcessingException.retryable("effect busy");
            }
        });

        PipelineEvent event = pipeline.submit("k-1", "agg-1", "d", "p");
        long deadline = System.currentTimeMillis() + 15_000;
        PipelineEvent acked;
        do {
            acked = pipeline.findEvent(event.eventId).orElseThrow();
            Thread.sleep(20);
        } while (acked.status != EventStatus.ACKED && System.currentTimeMillis() < deadline);
        pipeline.stop();

        assertThat(acked.status).isEqualTo(EventStatus.ACKED);
        assertThat(sideEffects.get()).isEqualTo(1);
        assertThat(store.findEffect(event.eventId).orElseThrow().state).isEqualTo(EffectState.DONE);
        assertThat(pipeline.stats().effectsDone()).isEqualTo(1);
        log.info("判定依据: eventId={} 最终状态={} 副作用生效次数={} (必须为1)",
                event.eventId, acked.status, sideEffects.get());
    }

    @Test
    void multiRoundRedeliveryDoesNotDuplicateSideEffect() throws Exception {
        EventStore store = new EventStore(tempDir.resolve("multi.json"));
        store.load();
        PipelineProperties props = new PipelineProperties();
        props.setWorkerThreads(2);
        props.setClaimTimeout(Duration.ofMillis(80));
        props.setBackoffBase(Duration.ofMillis(5));
        props.setPollInterval(Duration.ofMillis(5));
        EventPipeline pipeline = new EventPipeline(store, props);

        AtomicInteger sideEffects = new AtomicInteger();
        pipeline.start((event, context) -> {
            ClaimDecision decision = context.tryBeginEffect();
            if (decision == ClaimDecision.SKIP_ALREADY_DONE) {
                log.info("eventId={} 第{}次尝试: SKIP_ALREADY_DONE, 判定依据: 幂等账本DONE, 立即返回以确认",
                        event.eventId, event.deliveryAttempts);
                return;
            }
            sideEffects.incrementAndGet();
            context.completeEffect();
            // 首轮确认前睡过认领租约，原认领者确认失败，由后续轮次重新认领并确认
            try { Thread.sleep(150); } catch (InterruptedException ie) { Thread.currentThread().interrupt(); }
        });

        PipelineEvent event = pipeline.submit("k-1", "agg-1", "d", "p");
        long deadline = System.currentTimeMillis() + 15_000;
        PipelineEvent e;
        do {
            e = pipeline.findEvent(event.eventId).orElseThrow();
            Thread.sleep(20);
        } while (e.status != EventStatus.ACKED && System.currentTimeMillis() < deadline);
        pipeline.stop();

        assertThat(e.status).isEqualTo(EventStatus.ACKED);
        assertThat(sideEffects.get()).isEqualTo(1);
        assertThat(e.deliveryAttempts).isGreaterThan(1);
        log.info("判定依据: eventId={} 状态={} 总尝试轮次={} 副作用次数={} (必须为1)",
                event.eventId, e.status, e.deliveryAttempts, sideEffects.get());
    }
}
