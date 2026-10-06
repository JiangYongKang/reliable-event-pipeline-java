package com.github.highcumontoa.reliableeventpipelinejava.tests;

import static org.assertj.core.api.Assertions.assertThat;

import com.github.highcumontoa.reliableeventpipelinejava.core.EventStatus;
import com.github.highcumontoa.reliableeventpipelinejava.core.PipelineEvent;
import com.github.highcumontoa.reliableeventpipelinejava.core.PipelineStats;
import com.github.highcumontoa.reliableeventpipelinejava.pipeline.EventPipeline;
import com.github.highcumontoa.reliableeventpipelinejava.pipeline.PipelineProperties;
import com.github.highcumontoa.reliableeventpipelinejava.store.EventStore;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** 聚合级暂停/恢复：冻结单个聚合、其他聚合照常、恢复后原位按序续跑、状态跨重启保留。 */
class AggregatePauseResumeTest {

    private static final Logger log = LoggerFactory.getLogger(AggregatePauseResumeTest.class);

    @TempDir
    Path tempDir;

    private EventStore newStore(String name) {
        EventStore store = new EventStore(tempDir.resolve(name));
        store.load();
        return store;
    }

    private static void awaitUntil(long timeoutMillis, java.util.function.BooleanSupplier cond)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        while (!cond.getAsBoolean() && System.currentTimeMillis() < deadline) {
            Thread.sleep(5);
        }
        assertThat(cond.getAsBoolean()).isTrue();
    }

    @Test
    void pausedAggregateEventsCannotBeClaimedWhileOthersProceed() {
        EventStore store = newStore("pause-isolate.json");
        Instant now = Instant.parse("2026-10-06T10:00:00Z");
        PipelineEvent frozen1 = store.commitBusinessWithEvent("k-f1", "frozen", "d", "f1", now);
        PipelineEvent frozen2 = store.commitBusinessWithEvent("k-f2", "frozen", "d", "f2", now);
        PipelineEvent other = store.commitBusinessWithEvent("k-o1", "other", "d", "o1", now);

        store.pauseAggregate("frozen", now);
        assertThat(store.isAggregatePaused("frozen")).isTrue();

        // 连续认领：只能拿到 other 聚合的事件，frozen 的一条都拿不到
        List<String> claimedAggs = new CopyOnWriteArrayList<>();
        for (int i = 0; i < 4; i++) {
            Instant at = now.plusSeconds(i);
            store.claimNext("w", Duration.ofSeconds(30), at)
                    .ifPresent(e -> {
                        claimedAggs.add(e.aggregateId);
                        store.ack(e.eventId, "w", at);
                    });
        }
        assertThat(claimedAggs).containsExactly("other");
        assertThat(store.findEvent(frozen1.eventId).orElseThrow().status).isEqualTo(EventStatus.PENDING);
        assertThat(store.findEvent(frozen2.eventId).orElseThrow().status).isEqualTo(EventStatus.PENDING);
        assertThat(store.findEvent(other.eventId).orElseThrow().status).isEqualTo(EventStatus.ACKED);
        log.info("场景[暂停隔离]: 认领序列={}, frozen事件({},{})保持PENDING, other={}已ACKED, 判定: 只冻结目标聚合",
                claimedAggs, frozen1.eventId, frozen2.eventId, other.eventId);
    }

    @Test
    void resumeContinuesFromHeadInOrderWithoutFailureOrLoss() throws Exception {
        EventStore store = newStore("resume-order.json");
        PipelineProperties props = new PipelineProperties();
        props.setWorkerThreads(2);
        props.setPollInterval(Duration.ofMillis(5));
        EventPipeline pipeline = new EventPipeline(store, props);

        // a1 被认领后在处理器内等待放行，形成确定性的“在途队头”
        CountDownLatch a1Entered = new CountDownLatch(1);
        CountDownLatch releaseA1 = new CountDownLatch(1);
        List<String> order = new CopyOnWriteArrayList<>();
        pipeline.start((event, context) -> {
            order.add(event.aggregateId + ":" + event.payload);
            if (event.idempotencyKey.equals("k-a1")) {
                a1Entered.countDown();
                try {
                    releaseA1.await(10, TimeUnit.SECONDS);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                }
            }
        });

        PipelineEvent a1 = pipeline.submit("k-a1", "frozen", "d", "1");
        pipeline.submit("k-a2", "frozen", "d", "2");
        pipeline.submit("k-a3", "frozen", "d", "3");
        PipelineEvent b1 = pipeline.submit("k-b1", "other", "d", "1");

        assertThat(a1Entered.await(5, TimeUnit.SECONDS)).isTrue();
        awaitUntil(5_000, () -> store.findEvent(a1.eventId).orElseThrow().status == EventStatus.IN_FLIGHT);
        // 此刻 a1 在途、a2/a3 排在后面；暂停 frozen 后再补一条 a4
        pipeline.pauseAggregate("frozen");
        pipeline.submit("k-a4", "frozen", "d", "4");
        log.info("场景[恢复续跑-准备]: a1在途时暂停frozen, a2/a3/a4等待, 暂停期间不计失败不丢事件");

        // 其他聚合不受影响：b1 照常完成
        awaitUntil(5_000, () -> store.findEvent(b1.eventId).orElseThrow().status == EventStatus.ACKED);
        assertThat(store.findEvent(b1.eventId).orElseThrow().status).isEqualTo(EventStatus.ACKED);
        // 暂停期间放行 a1：它是暂停前已在途的，允许完成；a2/a3/a4 仍被冻结
        releaseA1.countDown();
        awaitUntil(5_000, () -> store.findEvent(a1.eventId).orElseThrow().status == EventStatus.ACKED);
        Thread.sleep(100);
        long frozenAckedWhilePaused = store.listEvents().stream()
                .filter(e -> e.aggregateId.equals("frozen") && e.status == EventStatus.ACKED)
                .filter(e -> !e.idempotencyKey.equals("k-a1"))
                .count();
        assertThat(frozenAckedWhilePaused).as("暂停期间 a2/a3/a4 不得被认领").isZero();
        log.info("场景[暂停期间]: a1(暂停前在途)完成, a2/a3/a4 零认领, b1(其他聚合)正常完成");

        pipeline.resumeAggregate("frozen");
        awaitUntil(10_000, () -> store.listEvents().stream()
                .allMatch(e -> e.status == EventStatus.ACKED));
        pipeline.stop();

        PipelineStats stats = pipeline.stats();
        assertThat(stats.dead()).as("暂停不得产生失败/死信").isZero();
        assertThat(stats.acked()).isEqualTo(5);
        List<String> frozenOrder = order.stream().filter(s -> s.startsWith("frozen:")).toList();
        assertThat(frozenOrder).containsExactly("frozen:1", "frozen:2", "frozen:3", "frozen:4");
        log.info("场景[恢复续跑-结果]: frozen投递顺序={}, dead={}, acked={}, 判定: 恢复后接暂停前位置按序续跑, 不跳序不漏不失败",
                frozenOrder, stats.dead(), stats.acked());
    }

    @Test
    void pauseStateSurvivesRestartAndResumeStillWorks() {
        Path file = tempDir.resolve("pause-restart.json");
        Instant now = Instant.parse("2026-10-06T10:00:00Z");
        EventStore store1 = new EventStore(file);
        store1.load();
        PipelineEvent e = store1.commitBusinessWithEvent("k-1", "frozen", "d", "p", now);
        store1.pauseAggregate("frozen", now);

        // 重启后暂停状态保留，事件仍被冻结
        EventStore store2 = new EventStore(file);
        store2.load();
        assertThat(store2.isAggregatePaused("frozen")).isTrue();
        assertThat(store2.claimNext("w", Duration.ofSeconds(30), now.plusSeconds(10))).isEmpty();

        store2.resumeAggregate("frozen", now.plusSeconds(10));
        assertThat(store2.claimNext("w", Duration.ofSeconds(30), now.plusSeconds(10)))
                .isPresent().get().extracting(x -> x.eventId).isEqualTo(e.eventId);
        log.info("场景[暂停跨重启]: 重启后frozen仍暂停, 事件 {} 不可认领; resume 后立即恢复认领, 判定: 暂停状态持久化",
                e.eventId);
    }
}
