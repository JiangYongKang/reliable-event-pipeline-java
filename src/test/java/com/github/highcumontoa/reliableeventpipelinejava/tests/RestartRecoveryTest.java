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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** 进程重启后状态恢复与未完成事件继续处理。 */
class RestartRecoveryTest {

    private static final Logger log = LoggerFactory.getLogger(RestartRecoveryTest.class);

    @TempDir
    Path tempDir;

    @Test
    void unprocessedEventsSurviveRestartAndContinueProcessing() throws Exception {
        Path storeFile = tempDir.resolve("restart.json");

        // 第一次"进程"：提交 3 个事件，确认 1 个，留 1 个 PENDING、1 个 IN_FLIGHT 后"崩溃"
        EventStore store1 = new EventStore(storeFile);
        store1.load();
        PipelineEvent e1 = store1.commitBusinessWithEvent("k-1", "agg-1", "d1", "p1", Instant.now());
        PipelineEvent e2 = store1.commitBusinessWithEvent("k-2", "agg-1", "d2", "p2", Instant.now());
        PipelineEvent e3 = store1.commitBusinessWithEvent("k-3", "agg-2", "d3", "p3", Instant.now());
        store1.claimNext("owner-old", Duration.ofMillis(1), Instant.now());
        store1.ack(e1.eventId, "owner-old", Instant.now());
        store1.claimNext("owner-old", Duration.ofMillis(1), Instant.now()); // e2 被认领但未确认
        Thread.sleep(5); // 让认领租约过期，模拟崩溃时留有在途事件

        // 第二次"进程"：从同一文件恢复
        EventStore store2 = new EventStore(storeFile);
        store2.load();
        PipelineStats restored = store2.stats();
        assertThat(restored.totalEvents()).isEqualTo(3);
        assertThat(restored.acked()).isEqualTo(1);
        assertThat(store2.listBusinessRecords()).hasSize(3);
        log.info("判定依据: 重启后恢复 total={} acked={} 业务记录={}",
                restored.totalEvents(), restored.acked(), store2.listBusinessRecords().size());

        PipelineProperties props = new PipelineProperties();
        props.setWorkerThreads(2);
        props.setPollInterval(Duration.ofMillis(5));
        EventPipeline pipeline = new EventPipeline(store2, props);
        List<String> processed = new CopyOnWriteArrayList<>();
        pipeline.start((event, context) -> processed.add(event.payload));

        long deadline = System.currentTimeMillis() + 10_000;
        while (pipeline.stats().acked() < 3 && System.currentTimeMillis() < deadline) {
            Thread.sleep(20);
        }
        pipeline.stop();

        PipelineStats stats = pipeline.stats();
        assertThat(stats.acked()).isEqualTo(3);
        assertThat(stats.pending()).isZero();
        assertThat(stats.inFlight()).isZero();
        // e2 是崩溃前在途的事件，重启后必须被重新处理
        assertThat(processed).contains("p2", "p3");
        PipelineEvent e2After = pipeline.findEvent(e2.eventId).orElseThrow();
        assertThat(e2After.status).isEqualTo(EventStatus.ACKED);
        log.info("判定依据: 重启后未完成事件继续处理, eventId={} 状态={} 已处理载荷={} acked={}",
                e2.eventId, e2After.status, processed, stats.acked());
    }

    @Test
    void deadLetterStateSurvivesRestartAndRedeliveryResumes() throws Exception {
        Path storeFile = tempDir.resolve("dead-restart.json");

        EventStore store1 = new EventStore(storeFile);
        store1.load();
        PipelineEvent e = store1.commitBusinessWithEvent("k-1", "agg-1", "d", "p", Instant.now());
        store1.claimNext("w", Duration.ofSeconds(30), Instant.now());
        store1.fail(e.eventId, "w", com.github.highcumontoa.reliableeventpipelinejava.core.FailureKind.NON_RETRYABLE,
                "bad", 3, Duration.ofMillis(1), Instant.now());
        assertThat(store1.findEvent(e.eventId).orElseThrow().status).isEqualTo(EventStatus.DEAD);

        // 重启后死信状态与原因保留，可重投
        EventStore store2 = new EventStore(storeFile);
        store2.load();
        PipelineEvent restored = store2.findEvent(e.eventId).orElseThrow();
        assertThat(restored.status).isEqualTo(EventStatus.DEAD);
        assertThat(restored.deadReason)
                .isEqualTo(com.github.highcumontoa.reliableeventpipelinejava.core.DeadReason.NON_RETRYABLE_FAILURE);
        PipelineEvent redelivered = store2.redeliver(e.eventId, Instant.now());
        assertThat(redelivered.status).isEqualTo(EventStatus.PENDING);
        log.info("判定依据: 重启后 eventId={} 死信原因={} 保留, 重投后状态={}",
                e.eventId, restored.deadReason, redelivered.status);
    }
}
