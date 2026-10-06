package com.github.highcumontoa.reliableeventpipelinejava.tests;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.github.highcumontoa.reliableeventpipelinejava.core.BacklogView;
import com.github.highcumontoa.reliableeventpipelinejava.core.EventStatus;
import com.github.highcumontoa.reliableeventpipelinejava.core.PipelineEvent;
import com.github.highcumontoa.reliableeventpipelinejava.pipeline.BacklogLimitExceededException;
import com.github.highcumontoa.reliableeventpipelinejava.pipeline.EventPipeline;
import com.github.highcumontoa.reliableeventpipelinejava.pipeline.PipelineProperties;
import com.github.highcumontoa.reliableeventpipelinejava.store.EventStore;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** 积压水位与背压：按聚合/全局查看未完成数量与最久积压时长，触顶一致拒绝且不留半截状态。 */
class BacklogBackpressureTest {

    private static final Logger log = LoggerFactory.getLogger(BacklogBackpressureTest.class);

    @TempDir
    Path tempDir;

    private EventStore newStore(String name, int limit) {
        EventStore store = new EventStore(tempDir.resolve(name));
        store.load();
        store.setMaxBacklogPerAggregate(limit);
        return store;
    }

    @Test
    void submitRejectedWhenAggregateBacklogHitsLimitWithDistinctFeedback() {
        EventStore store = newStore("cap.json", 2);
        Instant t0 = Instant.parse("2026-10-06T10:00:00Z");
        store.commitBusinessWithEvent("k-1", "agg-hot", "d", "p1", t0);
        store.commitBusinessWithEvent("k-2", "agg-hot", "d", "p2", t0);

        // 第 3 条触发上限：异常携带聚合、当前积压与上限，可与其他失败区分
        assertThatThrownBy(() -> store.commitBusinessWithEvent(
                "k-3", "agg-hot", "d", "p3", t0.plusSeconds(10)))
                .isInstanceOf(BacklogLimitExceededException.class)
                .satisfies(ex -> {
                    BacklogLimitExceededException be = (BacklogLimitExceededException) ex;
                    assertThat(be.aggregateId()).isEqualTo("agg-hot");
                    assertThat(be.currentBacklog()).isEqualTo(2);
                    assertThat(be.limit()).isEqualTo(2);
                });
        log.info("场景[触顶拒绝]: agg-hot 已有2条未处理, 第3条提交 -> BacklogLimitExceededException(current=2,limit=2), 判定: 反馈可区分非静默丢弃");
    }

    @Test
    void rejectionLeavesNoHalfCommittedBusinessOrEvent() {
        EventStore store = newStore("atomic-reject.json", 1);
        store.commitBusinessWithEvent("k-1", "agg-1", "biz-1", "p1", Instant.now());

        assertThatThrownBy(() -> store.commitBusinessWithEvent(
                "k-2", "agg-1", "biz-2", "p2", Instant.now()))
                .isInstanceOf(BacklogLimitExceededException.class);

        // 拒绝发生在任何写入之前：业务记录与事件都只有第 1 条
        assertThat(store.listEvents()).hasSize(1);
        assertThat(store.listBusinessRecords()).hasSize(1);
        assertThat(store.findEventByIdempotencyKey("k-2")).isEmpty();
        assertThat(store.listBusinessRecords().get(0).idempotencyKey).isEqualTo("k-1");
        log.info("场景[拒绝不留半截]: 拒绝后 events={} business={}, 不存在k-2的任何痕迹, 判定: 业务/事件同生共死",
                store.listEvents().size(), store.listBusinessRecords().size());
    }

    @Test
    void duplicateIdempotentSubmitIsNotRejectedByLimit() {
        EventStore store = newStore("idem-cap.json", 1);
        PipelineEvent first = store.commitBusinessWithEvent("k-dup", "agg-1", "d", "p", Instant.now());
        // 已到上限，但同一幂等键重试应返回原事件，而不是拒绝
        PipelineEvent again = store.commitBusinessWithEvent("k-dup", "agg-1", "d2", "p2", Instant.now());
        assertThat(again.eventId).isEqualTo(first.eventId);
        assertThat(store.listEvents()).hasSize(1);
        assertThat(store.listBusinessRecords()).hasSize(1);
        log.info("场景[幂等重试不被误拒]: 上限=1, 同幂等键重复提交返回同一事件 {}, 判定: 重复提交优先于限流",
                first.eventId);
    }

    @Test
    void capacityReleasedAfterEventsGetAcked() throws Exception {
        EventStore store = newStore("drain.json", 1);
        PipelineProperties props = new PipelineProperties();
        props.setWorkerThreads(1);
        props.setPollInterval(Duration.ofMillis(5));
        props.setMaxBacklogPerAggregate(1);
        EventPipeline pipeline = new EventPipeline(store, props);

        // 用闩把队头卡在处理中，保证“拒绝判定”时它确实占着积压名额（确定性，无轮询竞态）
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        pipeline.start((event, context) -> {
            entered.countDown();
            try {
                release.await(10, TimeUnit.SECONDS);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
            }
        });

        PipelineEvent e1 = pipeline.submit("k-1", "agg-1", "d", "p1");
        assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();
        awaitStatus(store, e1.eventId, EventStatus.IN_FLIGHT, 5_000);
        // e1 仍在途占着唯一名额：新提交必须被拒绝
        assertThatThrownBy(() -> pipeline.submit("k-2", "agg-1", "d", "p2"))
                .isInstanceOf(BacklogLimitExceededException.class);
        log.info("场景[容量恢复-满员]: 队头 {} 在途(占满limit=1), k-2 提交被拒, 判定: IN_FLIGHT 也计入积压",
                e1.eventId);

        // 放行让队头 ACKED，配额恢复后可继续提交
        release.countDown();
        awaitStatus(store, e1.eventId, EventStatus.ACKED, 5_000);
        PipelineEvent e2 = pipeline.submit("k-3", "agg-1", "d", "p3");
        pipeline.stop();
        assertThat(e2.status).isEqualTo(EventStatus.PENDING);
        log.info("场景[容量恢复-放行]: 队头 {} ACKED 后新提交 {} 成功, 判定: 积压下降即释放配额", e1.eventId, e2.eventId);
    }

    private static void awaitStatus(EventStore store, String eventId, EventStatus expected, long timeoutMillis)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        while (System.currentTimeMillis() < deadline) {
            if (store.findEvent(eventId).orElseThrow().status == expected) {
                return;
            }
            Thread.sleep(5);
        }
        assertThat(store.findEvent(eventId).orElseThrow().status).isEqualTo(expected);
    }

    @Test
    void backlogViewReportsPerAggregateAndGlobalCountsAndOldestAge() {
        EventStore store = newStore("view.json", 0);
        Instant t0 = Instant.parse("2026-10-06T10:00:00Z");
        Instant now = t0.plusSeconds(60);
        store.commitBusinessWithEvent("k-a1", "agg-A", "d", "p", t0);
        store.commitBusinessWithEvent("k-a2", "agg-A", "d", "p", t0.plusSeconds(20));
        store.commitBusinessWithEvent("k-b1", "agg-B", "d", "p", t0.plusSeconds(40));

        BacklogView view = store.backlog(now);
        assertThat(view.globalUnfinished()).isEqualTo(3);
        assertThat(view.globalOldestAge()).isEqualTo(Duration.ofSeconds(60));
        assertThat(view.aggregates()).hasSize(2);
        var aggA = view.aggregates().stream().filter(a -> a.aggregateId().equals("agg-A")).findFirst().orElseThrow();
        var aggB = view.aggregates().stream().filter(a -> a.aggregateId().equals("agg-B")).findFirst().orElseThrow();
        assertThat(aggA.unfinished()).isEqualTo(2);
        assertThat(aggA.oldestAge()).isEqualTo(Duration.ofSeconds(60));
        assertThat(aggB.unfinished()).isEqualTo(1);
        assertThat(aggB.oldestAge()).isEqualTo(Duration.ofSeconds(20));
        assertThat(aggA.paused()).isFalse();

        store.pauseAggregate("agg-A", now);
        BacklogView pausedView = store.backlog(now);
        var aggAPaused = pausedView.aggregates().stream()
                .filter(a -> a.aggregateId().equals("agg-A")).findFirst().orElseThrow();
        assertThat(aggAPaused.paused()).isTrue();
        assertThat(pausedView.globalUnfinished()).isEqualTo(3);
        log.info("场景[水位视图]: global(count={},oldestAge={}), aggA(count={},age={},paused={}), aggB(count={},age={}), 判定: 数量与最久积压按聚合/全局准确",
                view.globalUnfinished(), view.globalOldestAge(),
                aggA.unfinished(), aggA.oldestAge(), aggAPaused.paused(),
                aggB.unfinished(), aggB.oldestAge());
    }
}
