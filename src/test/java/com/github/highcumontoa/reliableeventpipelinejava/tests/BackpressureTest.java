package com.github.highcumontoa.reliableeventpipelinejava.tests;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.github.highcumontoa.reliableeventpipelinejava.core.BacklogStats;
import com.github.highcumontoa.reliableeventpipelinejava.core.EventStatus;
import com.github.highcumontoa.reliableeventpipelinejava.core.PipelineEvent;
import com.github.highcumontoa.reliableeventpipelinejava.pipeline.BackpressureRejectedException;
import com.github.highcumontoa.reliableeventpipelinejava.pipeline.EventPipeline;
import com.github.highcumontoa.reliableeventpipelinejava.pipeline.PipelineProperties;
import com.github.highcumontoa.reliableeventpipelinejava.store.EventStore;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** 积压水位与背压：水位统计、触顶拒绝（原子整体失败）、排空后恢复接受。 */
class BackpressureTest {

    private static final Logger log = LoggerFactory.getLogger(BackpressureTest.class);

    @TempDir
    Path tempDir;

    private EventStore newStore(String name) {
        EventStore store = new EventStore(tempDir.resolve(name));
        store.load();
        return store;
    }

    @Test
    void backlogStatsReportPerAggregateAndGlobal() throws Exception {
        EventStore store = newStore("backlog.json");
        // 用暂停阻止 agg-a 被消费，制造稳定积压
        store.pauseAggregate("agg-a");
        Instant before = Instant.now();
        store.commitBusinessWithEvent("k-a1", "agg-a", "d", "p1", Instant.now());
        Thread.sleep(30);
        store.commitBusinessWithEvent("k-a2", "agg-a", "d", "p2", Instant.now());
        store.commitBusinessWithEvent("k-b1", "agg-b", "d", "p1", Instant.now());

        BacklogStats stats = store.backlogStats(Instant.now());
        assertThat(stats.totalBacklog()).isEqualTo(3);
        assertThat(stats.byAggregate().get("agg-a").backlog()).isEqualTo(2);
        assertThat(stats.byAggregate().get("agg-b").backlog()).isEqualTo(1);
        // 最久积压时长 >= 第一条事件入队至今的时长
        long expectedMinAge = Instant.now().toEpochMilli() - before.toEpochMilli();
        assertThat(stats.byAggregate().get("agg-a").oldestBacklogAgeMillis())
                .isBetween(30L, expectedMinAge + 1000);
        assertThat(stats.oldestBacklogAgeMillis()).isNotNull();
        log.info("判定依据: 全局积压={} agg-a积压={} 最久积压={}ms agg-b积压={}",
                stats.totalBacklog(),
                stats.byAggregate().get("agg-a").backlog(),
                stats.byAggregate().get("agg-a").oldestBacklogAgeMillis(),
                stats.byAggregate().get("agg-b").backlog());

        // 全部处理完后水位归零
        store.resumeAggregate("agg-a");
        PipelineProperties props = new PipelineProperties();
        props.setWorkerThreads(2);
        props.setPollInterval(Duration.ofMillis(10));
        EventPipeline pipeline = new EventPipeline(store, props);
        pipeline.start((event, context) -> { });
        long deadline = System.currentTimeMillis() + 10_000;
        while (pipeline.stats().acked() < 3 && System.currentTimeMillis() < deadline) {
            Thread.sleep(20);
        }
        pipeline.stop();
        BacklogStats drained = pipeline.backlogStats();
        assertThat(drained.totalBacklog()).isZero();
        assertThat(drained.oldestBacklogAgeMillis()).isNull();
        assertThat(drained.byAggregate()).isEmpty();
        log.info("判定依据: 排空后全局积压=0, 最久积压时长=null");
    }

    @Test
    void submitRejectedAtomicallyWhenBacklogExceedsLimit() {
        EventStore store = newStore("backpressure.json");
        store.pauseAggregate("agg-hot");
        // 上限 2，先填满
        store.commitBusinessWithEvent("k-1", "agg-hot", "d1", "p1",
                null, null, 2, Instant.now());
        store.commitBusinessWithEvent("k-2", "agg-hot", "d2", "p2",
                null, null, 2, Instant.now());

        // 第 3 条被背压拒绝，异常可区分
        assertThatThrownBy(() -> store.commitBusinessWithEvent("k-3", "agg-hot", "d3", "p3",
                null, null, 2, Instant.now()))
                .isInstanceOf(BackpressureRejectedException.class)
                .satisfies(e -> {
                    BackpressureRejectedException bre = (BackpressureRejectedException) e;
                    assertThat(bre.getAggregateId()).isEqualTo("agg-hot");
                    assertThat(bre.getCurrentBacklog()).isEqualTo(2);
                    assertThat(bre.getMaxBacklog()).isEqualTo(2);
                });
        log.info("判定依据: 积压2达到上限2, 第3条提交抛出 BackpressureRejectedException");

        // 原子性：业务记录与事件都没有半截写入
        assertThat(store.listEvents()).hasSize(2);
        assertThat(store.listBusinessRecords()).hasSize(2);
        assertThat(store.listBusinessRecords())
                .noneMatch(b -> b.idempotencyKey.equals("k-3"));
        assertThat(store.findEventByIdempotencyKey("k-3")).isEmpty();
        log.info("判定依据: 拒绝后事件数=2 业务记录数=2, k-3 无任何落库痕迹");

        // 其他聚合不受该上限占用的影响
        PipelineEvent other = store.commitBusinessWithEvent("k-c1", "agg-cold", "d", "p",
                null, null, 2, Instant.now());
        assertThat(other.status).isEqualTo(EventStatus.PENDING);
        log.info("判定依据: agg-cold 提交正常, eventId={}", other.eventId);
    }

    @Test
    void submitAcceptedAgainAfterBacklogDrains() throws Exception {
        EventStore store = newStore("backpressure-drain.json");
        store.pauseAggregate("agg-hot");
        store.commitBusinessWithEvent("k-1", "agg-hot", "d1", "p1",
                null, null, 1, Instant.now());
        assertThatThrownBy(() -> store.commitBusinessWithEvent("k-2", "agg-hot", "d2", "p2",
                null, null, 1, Instant.now()))
                .isInstanceOf(BackpressureRejectedException.class);

        // 排空后恢复接受
        store.resumeAggregate("agg-hot");
        PipelineProperties props = new PipelineProperties();
        props.setWorkerThreads(1);
        props.setPollInterval(Duration.ofMillis(10));
        props.setMaxBacklogPerAggregate(1);
        EventPipeline pipeline = new EventPipeline(store, props);
        pipeline.start((event, context) -> { });
        long deadline = System.currentTimeMillis() + 10_000;
        while (pipeline.stats().acked() < 1 && System.currentTimeMillis() < deadline) {
            Thread.sleep(20);
        }

        PipelineEvent accepted = pipeline.submit("k-2", "agg-hot", "d2", "p2");
        deadline = System.currentTimeMillis() + 10_000;
        while (pipeline.stats().acked() < 2 && System.currentTimeMillis() < deadline) {
            Thread.sleep(20);
        }
        pipeline.stop();

        assertThat(accepted.status).isEqualTo(EventStatus.PENDING);
        assertThat(pipeline.stats().acked()).isEqualTo(2);
        log.info("判定依据: 排空后 k-2 重新提交被接受 eventId={}, 最终 acked={}",
                accepted.eventId, pipeline.stats().acked());
    }

    @Test
    void duplicateIdempotencyKeyNotBlockedByBackpressure() {
        EventStore store = newStore("backpressure-idem.json");
        store.pauseAggregate("agg-hot");
        PipelineEvent first = store.commitBusinessWithEvent("k-dup", "agg-hot", "d", "p",
                null, null, 1, Instant.now());
        // 积压已达上限，但重复幂等键是查询已有结果而非新提交，不应被拒绝
        PipelineEvent dup = store.commitBusinessWithEvent("k-dup", "agg-hot", "d", "p",
                null, null, 1, Instant.now());
        assertThat(dup.eventId).isEqualTo(first.eventId);
        assertThat(store.listEvents()).hasSize(1);
        log.info("判定依据: 重复幂等键返回已有事件 eventId={}, 不触发背压拒绝", dup.eventId);
    }
}
