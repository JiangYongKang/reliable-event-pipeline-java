package com.github.highcumontoa.reliableeventpipelinejava.tests;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.github.highcumontoa.reliableeventpipelinejava.core.DeadReason;
import com.github.highcumontoa.reliableeventpipelinejava.core.EventStatus;
import com.github.highcumontoa.reliableeventpipelinejava.core.PipelineEvent;
import com.github.highcumontoa.reliableeventpipelinejava.handler.EventProcessingException;
import com.github.highcumontoa.reliableeventpipelinejava.pipeline.EventPipeline;
import com.github.highcumontoa.reliableeventpipelinejava.pipeline.IllegalRedeliveryException;
import com.github.highcumontoa.reliableeventpipelinejava.pipeline.PipelineProperties;
import com.github.highcumontoa.reliableeventpipelinejava.store.EventStore;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** 失败重试、死信隔离与死信重投。 */
class RetryAndDeadLetterTest {

    private static final Logger log = LoggerFactory.getLogger(RetryAndDeadLetterTest.class);

    @TempDir
    Path tempDir;

    private EventPipeline newPipeline(String name, int maxAttempts, int workerThreads) {
        EventStore store = new EventStore(tempDir.resolve(name));
        store.load();
        PipelineProperties props = new PipelineProperties();
        props.setMaxDeliveryAttempts(maxAttempts);
        props.setBackoffBase(Duration.ofMillis(10));
        props.setPollInterval(Duration.ofMillis(5));
        props.setWorkerThreads(workerThreads);
        return new EventPipeline(store, props);
    }

    private static PipelineEvent awaitStatus(EventPipeline pipeline, String eventId,
            EventStatus expected, long timeoutMillis) throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        while (System.currentTimeMillis() < deadline) {
            PipelineEvent e = pipeline.findEvent(eventId).orElseThrow();
            if (e.status == expected) {
                return e;
            }
            Thread.sleep(10);
        }
        return pipeline.findEvent(eventId).orElseThrow();
    }

    @Test
    void retryableFailureExhaustsAttemptsThenDeadLetters() throws Exception {
        EventPipeline pipeline = newPipeline("retry.json", 3, 1);
        Map<String, AtomicInteger> attempts = new ConcurrentHashMap<>();
        pipeline.start((event, context) -> {
            int attempt = attempts.computeIfAbsent(event.eventId, k -> new AtomicInteger())
                    .incrementAndGet();
            log.info("处理失败(可重试) eventId={} 第{}次尝试, 判定依据: 模拟瞬时故障", event.eventId, attempt);
            throw EventProcessingException.retryable("simulated transient failure");
        });
        PipelineEvent event = pipeline.submit("k-retry", "agg-1", "d", "p");

        PipelineEvent dead = awaitStatus(pipeline, event.eventId, EventStatus.DEAD, 10_000);
        pipeline.stop();

        assertThat(dead.deadReason).isEqualTo(DeadReason.RETRIES_EXHAUSTED);
        assertThat(dead.deliveryAttempts).isEqualTo(3);
        assertThat(attempts.get(event.eventId).get()).isEqualTo(3);
        assertThat(pipeline.stats().dead()).isEqualTo(1);
        log.info("判定依据: eventId={} 尝试次数={} 达到上限=3, 死信原因={}",
                dead.eventId, dead.deliveryAttempts, dead.deadReason);
    }

    @Test
    void nonRetryableFailureDeadLettersImmediately() throws Exception {
        EventPipeline pipeline = newPipeline("nonretry.json", 5, 1);
        AtomicInteger attempts = new AtomicInteger();
        pipeline.start((event, context) -> {
            int attempt = attempts.incrementAndGet();
            log.info("处理失败(不可重试) eventId={} 第{}次尝试, 判定依据: 数据校验失败", event.eventId, attempt);
            throw EventProcessingException.nonRetryable("invalid payload");
        });
        PipelineEvent event = pipeline.submit("k-nr", "agg-1", "d", "p");

        PipelineEvent dead = awaitStatus(pipeline, event.eventId, EventStatus.DEAD, 10_000);
        pipeline.stop();

        assertThat(dead.deadReason).isEqualTo(DeadReason.NON_RETRYABLE_FAILURE);
        assertThat(dead.deliveryAttempts).isEqualTo(1);
        assertThat(attempts.get()).isEqualTo(1);
        log.info("判定依据: eventId={} 仅尝试{}次即死信, 原因={}",
                dead.eventId, dead.deliveryAttempts, dead.deadReason);
    }

    @Test
    void deadLetterCanBeRedeliveredAndIllegalRedeliveryRejected() throws Exception {
        EventPipeline pipeline = newPipeline("redeliver.json", 2, 1);
        AtomicInteger attempts = new AtomicInteger();
        pipeline.start((event, context) -> {
            int attempt = attempts.incrementAndGet();
            log.info("处理事件 eventId={} 第{}次尝试, 判定依据: 前两次失败之后成功", event.eventId, attempt);
            if (attempt <= 2) {
                throw EventProcessingException.retryable("not yet");
            }
        });
        PipelineEvent event = pipeline.submit("k-rd", "agg-1", "d", "p");
        PipelineEvent dead = awaitStatus(pipeline, event.eventId, EventStatus.DEAD, 10_000);
        assertThat(dead.deadReason).isEqualTo(DeadReason.RETRIES_EXHAUSTED);

        // 状态非法的重投必须被拒绝
        PipelineEvent ok = pipeline.submit("k-ok", "agg-2", "d", "p");
        awaitStatus(pipeline, ok.eventId, EventStatus.ACKED, 10_000);
        assertThatThrownBy(() -> pipeline.redeliver(ok.eventId))
                .isInstanceOf(IllegalRedeliveryException.class);
        log.info("判定依据: 对 ACKED 事件 {} 重投被拒绝", ok.eventId);

        // 死信重投后恢复处理并最终确认
        PipelineEvent redelivered = pipeline.redeliver(dead.eventId);
        assertThat(redelivered.status).isEqualTo(EventStatus.PENDING);
        assertThat(redelivered.redeliveries).isEqualTo(1);
        PipelineEvent acked = awaitStatus(pipeline, dead.eventId, EventStatus.ACKED, 10_000);
        pipeline.stop();

        assertThat(acked.redeliveries).isEqualTo(1);
        assertThat(pipeline.stats().dead()).isZero();
        log.info("判定依据: eventId={} 重投后状态={} 重投次数={}",
                acked.eventId, acked.status, acked.redeliveries);
    }

    @Test
    void failingAggregateDoesNotBlockOtherAggregates() throws Exception {
        EventPipeline pipeline = newPipeline("isolation.json", 2, 3);
        pipeline.start((event, context) -> {
            if (event.aggregateId.equals("agg-bad")) {
                log.info("处理失败 eventId={} 第{}次尝试, 判定依据: 聚合agg-bad持续故障",
                        event.eventId, event.deliveryAttempts);
                throw EventProcessingException.retryable("agg-bad always fails");
            }
        });
        PipelineEvent bad = pipeline.submit("k-bad", "agg-bad", "d", "p");
        PipelineEvent good1 = pipeline.submit("k-good-1", "agg-good", "d", "p1");
        PipelineEvent good2 = pipeline.submit("k-good-2", "agg-good", "d", "p2");

        awaitStatus(pipeline, bad.eventId, EventStatus.DEAD, 10_000);
        PipelineEvent g1 = awaitStatus(pipeline, good1.eventId, EventStatus.ACKED, 10_000);
        PipelineEvent g2 = awaitStatus(pipeline, good2.eventId, EventStatus.ACKED, 10_000);
        pipeline.stop();

        assertThat(g1.status).isEqualTo(EventStatus.ACKED);
        assertThat(g2.status).isEqualTo(EventStatus.ACKED);
        assertThat(pipeline.stats().dead()).isEqualTo(1);
        assertThat(pipeline.stats().acked()).isEqualTo(2);
        log.info("判定依据: agg-bad事件={} 已死信, agg-good事件 {} 与 {} 均已确认, 未受阻塞",
                bad.eventId, g1.eventId, g2.eventId);
    }
}
