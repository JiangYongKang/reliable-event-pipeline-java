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

/** 延迟投递：到期前后的认领行为、聚合内保序、重启保留等待、等待上限死信。 */
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
    void delayedEventNotClaimableBeforeNotBeforeAndClaimableAfter() {
        EventStore store = newStore("delay-expire.json");
        Instant t0 = Instant.parse("2026-10-06T10:00:00Z");
        PipelineEvent delayed = store.commitBusinessWithEvent(
                "k-delay", "agg-1", "d", "p", t0.plusSeconds(60), t0);
        PipelineEvent immediate = store.commitBusinessWithEvent(
                "k-now", "agg-2", "d", "p2", null, t0);

        // 未到点：延迟事件不能被任何人认领；其他聚合的即时事件不受影响
        Optional<PipelineEvent> before = store.claimNext("w", Duration.ofSeconds(30), t0.plusSeconds(30));
        assertThat(before).isPresent();
        assertThat(before.get().eventId).isEqualTo(immediate.eventId);
        store.ack(immediate.eventId, "w", t0.plusSeconds(30));
        Optional<PipelineEvent> stillWaiting = store.claimNext("w", Duration.ofSeconds(30), t0.plusSeconds(59));
        assertThat(stillWaiting).isEmpty();
        log.info("场景[到期前]: notBefore={} 当前=+59s, 延迟事件 {} 不可认领, 即时事件 {} 正常处理, 判定: 时间未到不放量",
                delayed.notBefore, delayed.eventId, immediate.eventId);

        // 到期：可被认领
        Optional<PipelineEvent> due = store.claimNext("w", Duration.ofSeconds(30), t0.plusSeconds(60));
        assertThat(due).isPresent();
        assertThat(due.get().eventId).isEqualTo(delayed.eventId);
        log.info("场景[到期]: 当前=+60s, 延迟事件 {} 被认领 status={}, 判定: 到达最早可投递时间",
                delayed.eventId, due.get().status);
    }

    @Test
    void delayedEventCannotOvertakeUndeliveredHeadWithinSameAggregate() {
        EventStore store = newStore("delay-order.json");
        Instant t0 = Instant.parse("2026-10-06T10:00:00Z");
        PipelineEvent head = store.commitBusinessWithEvent("k-1", "agg-A", "d", "head", null, t0);
        // 后面的延迟事件，很快到点
        PipelineEvent later = store.commitBusinessWithEvent(
                "k-2", "agg-A", "d", "later", t0.plusSeconds(10), t0);

        Optional<PipelineEvent> first = store.claimNext("w", Duration.ofSeconds(30), t0.plusSeconds(5));
        assertThat(first).isPresent();
        assertThat(first.get().eventId).isEqualTo(head.eventId);
        // 队头在途未确认：后面的延迟事件即便已到点也不能抢
        Optional<PipelineEvent> overtake = store.claimNext("w2", Duration.ofSeconds(30), t0.plusSeconds(20));
        assertThat(overtake).as("在途队头未完成, 后到事件不得越序").isEmpty();

        store.ack(head.eventId, "w", t0.plusSeconds(20));
        Optional<PipelineEvent> second = store.claimNext("w2", Duration.ofSeconds(30), t0.plusSeconds(20));
        assertThat(second).isPresent();
        assertThat(second.get().eventId).isEqualTo(later.eventId);
        log.info("场景[聚合内保序]: 队头 {} 在途时延迟事件 {} 到点也不认领; 队头确认后 {} 才被认领, 判定: 严格按seq顺序",
                head.eventId, later.eventId, second.get().eventId);
    }

    @Test
    void waitingDelayedEventSurvivesRestartWithoutEarlyRelease() {
        Path file = tempDir.resolve("delay-restart.json");
        Instant t0 = Instant.parse("2026-10-06T10:00:00Z");
        EventStore store1 = new EventStore(file);
        store1.load();
        PipelineEvent delayed = store1.commitBusinessWithEvent(
                "k-d", "agg-1", "d", "p", t0.plusSeconds(120), t0);

        // “重启”：从同一快照恢复，时间推进 60s（未到点），仍不得提前放出
        EventStore store2 = new EventStore(file);
        store2.load();
        PipelineEvent restored = store2.findEvent(delayed.eventId).orElseThrow();
        assertThat(restored.status).isEqualTo(EventStatus.PENDING);
        assertThat(restored.notBefore).isEqualTo(t0.plusSeconds(120));
        Optional<PipelineEvent> early = store2.claimNext("w", Duration.ofSeconds(30), t0.plusSeconds(60));
        assertThat(early).isEmpty();
        log.info("场景[重启保留等待]: 重启后 eventId={} status={} notBefore={}, 在+60s仍不可认领, 判定: 等待时间持久化不提前释放",
                delayed.eventId, restored.status, restored.notBefore);

        Optional<PipelineEvent> due = store2.claimNext("w", Duration.ofSeconds(30), t0.plusSeconds(120));
        assertThat(due).isPresent();
        assertThat(due.get().eventId).isEqualTo(delayed.eventId);
        log.info("场景[重启后到期]: +120s 事件 {} 正常认领, 判定: 到期语义跨重启保持", delayed.eventId);
    }

    @Test
    void delayBeyondConfiguredLimitDeadLettersWithDistinctReason() {
        EventStore store = newStore("delay-cap.json");
        store.setMaxDelay(Duration.ofSeconds(100));
        Instant t0 = Instant.parse("2026-10-06T10:00:00Z");
        PipelineEvent delayed = store.commitBusinessWithEvent(
                "k-cap", "agg-1", "d", "p", t0.plusSeconds(600), t0);

        // 未超上限前仍是等待
        assertThat(store.claimNext("w", Duration.ofSeconds(30), t0.plusSeconds(50))).isEmpty();
        // 自提交起超过 100s 上限仍未到点：以区别于失败原因的方式处理掉（死信 DELAY_LIMIT_EXCEEDED）
        assertThat(store.claimNext("w", Duration.ofSeconds(30), t0.plusSeconds(101))).isEmpty();
        PipelineEvent dead = store.findEvent(delayed.eventId).orElseThrow();
        assertThat(dead.status).isEqualTo(EventStatus.DEAD);
        assertThat(dead.deadReason).isEqualTo(DeadReason.DELAY_LIMIT_EXCEEDED);
        log.info("场景[等待上限]: eventId={} 等待超过100s上限 -> status={} reason={} (区别于RETRIES_EXHAUSTED/NON_RETRYABLE_FAILURE)",
                delayed.eventId, dead.status, dead.deadReason);
    }

    @Test
    void delayCappedDeadLetterRedeliveryClearsWaitAndDeliversImmediately() {
        EventStore store = newStore("delay-cap-redeliver.json");
        store.setMaxDelay(Duration.ofSeconds(100));
        Instant t0 = Instant.parse("2026-10-06T10:00:00Z");
        PipelineEvent delayed = store.commitBusinessWithEvent(
                "k-cap", "agg-1", "d", "p", t0.plusSeconds(600), t0);
        // 触发延迟上限死信
        assertThat(store.claimNext("w", Duration.ofSeconds(30), t0.plusSeconds(101))).isEmpty();
        assertThat(store.findEvent(delayed.eventId).orElseThrow().deadReason)
                .isEqualTo(DeadReason.DELAY_LIMIT_EXCEEDED);

        // 运维显式重投：清除等待时间，立即可以认领，不会再次被上限打回
        PipelineEvent redelivered = store.redeliver(delayed.eventId, t0.plusSeconds(102));
        assertThat(redelivered.status).isEqualTo(EventStatus.PENDING);
        assertThat(redelivered.notBefore).isNull();
        Optional<PipelineEvent> claimed = store.claimNext("w", Duration.ofSeconds(30), t0.plusSeconds(102));
        assertThat(claimed).isPresent();
        assertThat(claimed.get().eventId).isEqualTo(delayed.eventId);
        log.info("场景[延迟死信重投]: eventId={} 重投后 notBefore=null 立即认领, 判定: 运维显式介入即立即投递, 不再二次超限",
                delayed.eventId);
    }

    @Test
    void delayedEventDeliveredAfterWallClockExpiry() throws Exception {
        EventStore store = newStore("delay-wall.json");
        PipelineProperties props = new PipelineProperties();
        props.setWorkerThreads(1);
        props.setPollInterval(Duration.ofMillis(5));
        EventPipeline pipeline = new EventPipeline(store, props);
        List<String> delivered = new CopyOnWriteArrayList<>();
        pipeline.start((event, context) -> delivered.add(event.payload));

        PipelineEvent delayed = pipeline.submitDelayed(
                "k-w", "agg-1", "d", "late", Duration.ofMillis(500));
        long deadline = System.currentTimeMillis() + 8_000;
        PipelineEvent e;
        do {
            e = pipeline.findEvent(delayed.eventId).orElseThrow();
            Thread.sleep(10);
        } while (e.status != EventStatus.ACKED && System.currentTimeMillis() < deadline);
        pipeline.stop();

        assertThat(e.status).isEqualTo(EventStatus.ACKED);
        assertThat(delivered).containsExactly("late");
        assertThat(deadline - System.currentTimeMillis()).as("必须在超时前确认, 而非轮询耗尽").isGreaterThan(0);
        log.info("场景[端到端墙钟到期]: eventId={} 延迟500ms后被投递并确认, delivered={}, 判定: 到期自动放量",
                delayed.eventId, delivered);
    }
}
