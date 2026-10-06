package com.github.highcumontoa.reliableeventpipelinejava.tests;

import static org.assertj.core.api.Assertions.assertThat;

import com.github.highcumontoa.reliableeventpipelinejava.core.DeadReason;
import com.github.highcumontoa.reliableeventpipelinejava.core.EventStatus;
import com.github.highcumontoa.reliableeventpipelinejava.core.PipelineEvent;
import com.github.highcumontoa.reliableeventpipelinejava.pipeline.EventPipeline;
import com.github.highcumontoa.reliableeventpipelinejava.pipeline.PipelineProperties;
import com.github.highcumontoa.reliableeventpipelinejava.store.EventStore;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** 延迟投递：到点前不可认领、聚合内队首阻塞、重启保留等待、超过上限以 DELAY_EXCEEDED 死信。 */
class DelayedDeliveryTest {

    private static final Logger log = LoggerFactory.getLogger(DelayedDeliveryTest.class);

    @TempDir
    Path tempDir;

    private EventStore newStore(String name) {
        EventStore store = new EventStore(tempDir.resolve(name));
        store.load();
        return store;
    }

    @Test
    void delayedEventNotClaimableBeforeDeliverAt() {
        EventStore store = newStore("delay.json");
        Instant now = Instant.now();
        PipelineEvent event = store.commitBusinessWithEvent("k-1", "agg-1", "d", "p",
                now.plusSeconds(600), Duration.ofHours(1), 0, now);

        Optional<PipelineEvent> tooEarly = store.claimNext("w1", Duration.ofSeconds(30), Instant.now());
        assertThat(tooEarly).isEmpty();
        log.info("判定依据: deliverAt={} 未到, 认领结果为空", event.deliverAt);

        // 时间推进到 deliverAt 之后（用未来的 now 模拟），可被认领
        Optional<PipelineEvent> due = store.claimNext("w1", Duration.ofSeconds(30),
                event.deliverAt.plusMillis(1));
        assertThat(due).isPresent();
        assertThat(due.get().eventId).isEqualTo(event.eventId);
        log.info("判定依据: 到点后 eventId={} 被认领, deliverAt={}", due.get().eventId, event.deliverAt);
    }

    @Test
    void laterEventCannotOvertakeEarlierDelayedEventInSameAggregate() {
        EventStore store = newStore("headofline.json");
        Instant now = Instant.now();
        PipelineEvent delayed = store.commitBusinessWithEvent("k-1", "agg-1", "d", "p-delayed",
                now.plusSeconds(600), Duration.ofHours(1), 0, now);
        PipelineEvent ready = store.commitBusinessWithEvent("k-2", "agg-1", "d", "p-ready",
                null, Duration.ofHours(1), 0, now);

        // 后面的就绪事件不能抢先到点前的队首事件
        Optional<PipelineEvent> claimed = store.claimNext("w1", Duration.ofSeconds(30), Instant.now());
        assertThat(claimed).isEmpty();
        log.info("判定依据: 队首事件 seq={} 未到点, 后续事件 seq={} 虽就绪但被阻塞",
                delayed.seq, ready.seq);

        // 队首到点后按顺序投递：先 delayed 后 ready
        Instant afterDue = delayed.deliverAt.plusMillis(1);
        Optional<PipelineEvent> first = store.claimNext("w1", Duration.ofSeconds(30), afterDue);
        assertThat(first).isPresent();
        assertThat(first.get().eventId).isEqualTo(delayed.eventId);
        store.ack(delayed.eventId, "w1", afterDue);
        Optional<PipelineEvent> second = store.claimNext("w1", Duration.ofSeconds(30), afterDue);
        assertThat(second).isPresent();
        assertThat(second.get().eventId).isEqualTo(ready.eventId);
        log.info("判定依据: 到点后投递顺序 {} -> {}, 与提交顺序一致",
                first.get().payload, second.get().payload);
    }

    @Test
    void delayedWaitSurvivesRestart() {
        Path storeFile = tempDir.resolve("delay-restart.json");
        Instant now = Instant.now();
        Instant deliverAt = now.plusSeconds(600);

        EventStore store1 = new EventStore(storeFile);
        store1.load();
        PipelineEvent event = store1.commitBusinessWithEvent("k-1", "agg-1", "d", "p",
                deliverAt, Duration.ofHours(1), 0, now);

        // 重启后：deliverAt 保留，到点前仍不可认领
        EventStore store2 = new EventStore(storeFile);
        store2.load();
        PipelineEvent restored = store2.findEvent(event.eventId).orElseThrow();
        assertThat(restored.deliverAt).isEqualTo(deliverAt);
        assertThat(restored.status).isEqualTo(EventStatus.PENDING);
        Optional<PipelineEvent> tooEarly = store2.claimNext("w1", Duration.ofSeconds(30), Instant.now());
        assertThat(tooEarly).isEmpty();
        log.info("判定依据: 重启后 eventId={} deliverAt={} 保留, 到点前认领为空",
                event.eventId, restored.deliverAt);
    }

    @Test
    void delayBeyondMaxDelayDeadLettersWithDistinctReason() {
        EventStore store = newStore("delay-exceeded.json");
        Instant now = Instant.now();
        PipelineEvent event = store.commitBusinessWithEvent("k-1", "agg-1", "d", "p",
                now.plusSeconds(3600), Duration.ofMinutes(10), 0, now);

        // 等待超过上限：直接死信，原因可与其他失败区分；业务记录仍原子提交
        assertThat(event.status).isEqualTo(EventStatus.DEAD);
        assertThat(event.deadReason).isEqualTo(DeadReason.DELAY_EXCEEDED);
        assertThat(event.deadReason).isNotEqualTo(DeadReason.RETRIES_EXHAUSTED);
        assertThat(event.deadReason).isNotEqualTo(DeadReason.NON_RETRYABLE_FAILURE);
        assertThat(store.listBusinessRecords()).hasSize(1);
        assertThat(store.stats().dead()).isEqualTo(1);
        log.info("判定依据: 等待3600s超过上限600s, eventId={} 状态={} 原因={} 业务记录数=1",
                event.eventId, event.status, event.deadReason);
    }

    @Test
    void delayedEventDeliveredAfterDueThroughPipeline() throws Exception {
        EventStore store = newStore("delay-pipeline.json");
        PipelineProperties props = new PipelineProperties();
        props.setWorkerThreads(1);
        props.setPollInterval(Duration.ofMillis(10));
        EventPipeline pipeline = new EventPipeline(store, props);
        List<String> delivered = new CopyOnWriteArrayList<>();
        pipeline.start((event, context) -> delivered.add(event.payload));

        // 300ms 后可投递
        pipeline.submit("k-1", "agg-1", "d", "p-delayed",
                Instant.now().plusMillis(300));
        Thread.sleep(150);
        assertThat(delivered).isEmpty();
        log.info("判定依据: 150ms 时未到点, 已投递={} (应为空)", delivered);

        long deadline = System.currentTimeMillis() + 10_000;
        while (delivered.isEmpty() && System.currentTimeMillis() < deadline) {
            Thread.sleep(20);
        }
        pipeline.stop();
        assertThat(delivered).containsExactly("p-delayed");
        log.info("判定依据: 到点后投递成功, 已投递={}", delivered);
    }
}
