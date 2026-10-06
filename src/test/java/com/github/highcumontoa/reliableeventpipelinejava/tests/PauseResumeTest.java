package com.github.highcumontoa.reliableeventpipelinejava.tests;

import static org.assertj.core.api.Assertions.assertThat;

import com.github.highcumontoa.reliableeventpipelinejava.core.EventStatus;
import com.github.highcumontoa.reliableeventpipelinejava.core.PipelineEvent;
import com.github.highcumontoa.reliableeventpipelinejava.pipeline.EventPipeline;
import com.github.highcumontoa.reliableeventpipelinejava.pipeline.PipelineProperties;
import com.github.highcumontoa.reliableeventpipelinejava.store.EventStore;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** 聚合级暂停与恢复：暂停期间不可认领、其他聚合不受影响、恢复后按序继续。 */
class PauseResumeTest {

    private static final Logger log = LoggerFactory.getLogger(PauseResumeTest.class);

    @TempDir
    Path tempDir;

    private EventPipeline newPipeline(EventStore store) {
        PipelineProperties props = new PipelineProperties();
        props.setWorkerThreads(2);
        props.setPollInterval(Duration.ofMillis(10));
        return new EventPipeline(store, props);
    }

    private static void await(java.util.function.Supplier<Boolean> condition, long timeoutMillis)
            throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        while (!condition.get() && System.currentTimeMillis() < deadline) {
            Thread.sleep(20);
        }
    }

    @Test
    void pausedAggregateNotClaimedWhileOthersProceed() throws Exception {
        EventStore store = new EventStore(tempDir.resolve("pause.json"));
        store.load();
        EventPipeline pipeline = newPipeline(store);
        List<String> delivered = new CopyOnWriteArrayList<>();
        pipeline.start((event, context) -> delivered.add(event.aggregateId + ":" + event.payload));

        pipeline.pauseAggregate("agg-frozen");
        pipeline.submit("k-f1", "agg-frozen", "d", "p1");
        pipeline.submit("k-f2", "agg-frozen", "d", "p2");
        pipeline.submit("k-n1", "agg-normal", "d", "p1");

        await(() -> delivered.contains("agg-normal:p1"), 10_000);
        // 给暂停聚合留出"如果被错误投递就会暴露"的时间窗口
        Thread.sleep(300);
        pipeline.stop();

        assertThat(delivered).contains("agg-normal:p1");
        assertThat(delivered.stream().filter(s -> s.startsWith("agg-frozen"))).isEmpty();
        // 暂停期间的事件保持 PENDING，不算失败、不进死信
        List<PipelineEvent> frozen = pipeline.listEvents().stream()
                .filter(e -> e.aggregateId.equals("agg-frozen")).toList();
        assertThat(frozen).hasSize(2);
        assertThat(frozen).allMatch(e -> e.status == EventStatus.PENDING);
        assertThat(frozen).allMatch(e -> e.deliveryAttempts == 0);
        log.info("判定依据: 暂停期间 agg-frozen 2 条事件均 PENDING 且 0 次投递, "
                + "agg-normal 正常投递, 已投递={}", delivered);
    }

    @Test
    void resumeContinuesInCommitOrderWithoutLoss() throws Exception {
        EventStore store = new EventStore(tempDir.resolve("resume.json"));
        store.load();
        EventPipeline pipeline = newPipeline(store);
        List<String> delivered = new CopyOnWriteArrayList<>();
        pipeline.start((event, context) -> delivered.add(event.payload));

        // 暂停前处理掉第 1 条
        pipeline.submit("k-1", "agg-1", "d", "p1");
        await(() -> delivered.contains("p1"), 10_000);

        // 暂停期间再提交 3 条
        pipeline.pauseAggregate("agg-1");
        pipeline.submit("k-2", "agg-1", "d", "p2");
        pipeline.submit("k-3", "agg-1", "d", "p3");
        pipeline.submit("k-4", "agg-1", "d", "p4");
        Thread.sleep(300);
        assertThat(delivered).containsExactly("p1");
        log.info("判定依据: 暂停期间无新投递, 已投递={}", delivered);

        // 恢复后从暂停前的位置按顺序继续
        pipeline.resumeAggregate("agg-1");
        await(() -> delivered.size() == 4, 10_000);
        pipeline.stop();

        assertThat(delivered).containsExactly("p1", "p2", "p3", "p4");
        assertThat(pipeline.stats().acked()).isEqualTo(4);
        assertThat(pipeline.stats().dead()).isZero();
        log.info("判定依据: 恢复后投递顺序={} acked={} dead=0, 无跳序无丢失",
                delivered, pipeline.stats().acked());
    }

    @Test
    void pauseStateSurvivesRestart() {
        Path storeFile = tempDir.resolve("pause-restart.json");
        EventStore store1 = new EventStore(storeFile);
        store1.load();
        store1.pauseAggregate("agg-1");
        store1.commitBusinessWithEvent("k-1", "agg-1", "d", "p", java.time.Instant.now());

        EventStore store2 = new EventStore(storeFile);
        store2.load();
        assertThat(store2.isPaused("agg-1")).isTrue();
        assertThat(store2.claimNext("w1", Duration.ofSeconds(30), java.time.Instant.now())).isEmpty();
        log.info("判定依据: 重启后 agg-1 仍处于暂停状态, 认领为空");

        store2.resumeAggregate("agg-1");
        assertThat(store2.claimNext("w1", Duration.ofSeconds(30), java.time.Instant.now())).isPresent();
        log.info("判定依据: 恢复后事件可被认领");
    }
}
