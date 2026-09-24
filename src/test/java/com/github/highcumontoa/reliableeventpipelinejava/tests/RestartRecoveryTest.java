package com.github.highcumontoa.reliableeventpipelinejava.tests;

import com.github.highcumontoa.reliableeventpipelinejava.event.EventStatus;
import com.github.highcumontoa.reliableeventpipelinejava.event.InProcessEventStore;
import com.github.highcumontoa.reliableeventpipelinejava.event.NewEvent;
import com.github.highcumontoa.reliableeventpipelinejava.event.StoredEvent;
import com.github.highcumontoa.reliableeventpipelinejava.pipeline.EventDispatcher;
import com.github.highcumontoa.reliableeventpipelinejava.pipeline.RetryPolicy;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** 进程重启后：未完成事件可继续处理，租约超时回收，消费标记不丢失。 */
class RestartRecoveryTest {

    @TempDir
    Path dir;

    private static final long CLAIM_TIMEOUT_MS = 5_000;

    @Test
    void unfinishedEventsSurviveRestartAndExpiredClaimIsReclaimed() {
        Path journal = dir.resolve("journal.jsonl");
        MutableClock clock = new MutableClock(0);

        // 第一次"进程"：提交两个事件，认领其中一个但尚未确认就崩溃
        InProcessEventStore first = new InProcessEventStore(journal, clock);
        first.commitBusinessAndEvent("op-1", () -> { }, new NewEvent("agg-r", "p0"));
        first.commitBusinessAndEvent("op-2", () -> { }, new NewEvent("agg-r", "p1"));
        String headId = first.findByAggregate("agg-r").get(0).getId();
        List<StoredEvent> claimed = first.claimBatch("crashed-owner", 10, clock.millis(), CLAIM_TIMEOUT_MS);
        assertEquals(1, claimed.size(), "每个聚合只认领队首事件");
        // 模拟崩溃：不再确认，直接丢弃该实例

        // 重启：重放 journal 恢复状态
        InProcessEventStore second = new InProcessEventStore(journal, clock);
        StoredEvent recovered = second.find(headId).orElseThrow();
        assertEquals(EventStatus.CLAIMED, recovered.getStatus(), "重启后认领状态可恢复");
        assertEquals("crashed-owner", recovered.getClaimOwner());

        // 租约未过期：他人不得抢占
        assertTrue(second.claimBatch("new-owner", 10, clock.millis(), CLAIM_TIMEOUT_MS).isEmpty());

        // 租约超时：可被重新认领并完成投递
        clock.advance(CLAIM_TIMEOUT_MS + 1);
        AtomicInteger delivered = new AtomicInteger();
        EventDispatcher dispatcher = new EventDispatcher(second,
                event -> delivered.incrementAndGet(), new RetryPolicy(3, 100, 2.0, 1000),
                CLAIM_TIMEOUT_MS, clock);
        dispatcher.runOnce(); // 投递队首 p0
        dispatcher.runOnce(); // 投递 p1

        assertEquals(2, delivered.get(), "重启后未完成事件可继续处理");
        assertEquals(2, second.stats().delivered());
        assertEquals(0, second.stats().pending());
        assertEquals(0, second.stats().claimed());
    }

    @Test
    void consumptionMarksSurviveRestart() {
        Path journal = dir.resolve("journal.jsonl");
        InProcessEventStore first = new InProcessEventStore(journal);
        first.commitBusinessAndEvent("op-1", () -> { }, new NewEvent("agg-c", "p"));
        String eventId = first.findByAggregate("agg-c").get(0).getId();

        AtomicInteger sideEffects = new AtomicInteger();
        assertTrue(first.consumeOnce(eventId, sideEffects::incrementAndGet));

        // 重启后重复消费：副作用不得再次生效
        InProcessEventStore second = new InProcessEventStore(journal);
        assertFalse(second.consumeOnce(eventId, sideEffects::incrementAndGet));
        assertEquals(1, sideEffects.get());
        assertEquals(1, second.stats().consumed());
    }

    @Test
    void committedOperationKeysSurviveRestart() {
        Path journal = dir.resolve("journal.jsonl");
        InProcessEventStore first = new InProcessEventStore(journal);
        first.commitBusinessAndEvent("op-1", () -> { }, new NewEvent("agg-k", "p"));

        InProcessEventStore second = new InProcessEventStore(journal);
        assertEquals(com.github.highcumontoa.reliableeventpipelinejava.event.CommitOutcome.DUPLICATE_IGNORED,
                second.commitBusinessAndEvent("op-1", () -> { }, new NewEvent("agg-k", "p")),
                "重启后重复提交仍被幂等忽略");
        assertEquals(1, second.findByAggregate("agg-k").size());
    }
}
