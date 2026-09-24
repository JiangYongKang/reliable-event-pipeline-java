package com.github.highcumontoa.reliableeventpipelinejava.tests;

import com.github.highcumontoa.reliableeventpipelinejava.event.DeadLetterReason;
import com.github.highcumontoa.reliableeventpipelinejava.event.EventStatus;
import com.github.highcumontoa.reliableeventpipelinejava.event.InProcessEventStore;
import com.github.highcumontoa.reliableeventpipelinejava.event.NewEvent;
import com.github.highcumontoa.reliableeventpipelinejava.event.StoredEvent;
import com.github.highcumontoa.reliableeventpipelinejava.pipeline.EventDispatcher;
import com.github.highcumontoa.reliableeventpipelinejava.pipeline.EventHandler;
import com.github.highcumontoa.reliableeventpipelinejava.pipeline.NonRetryableException;
import com.github.highcumontoa.reliableeventpipelinejava.pipeline.RetryPolicy;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** 可配置退避重试、重试耗尽与不可重试故障的死信隔离、死信重投。 */
class RetryDeadLetterTest {

    @TempDir
    Path dir;

    private static final long CLAIM_TIMEOUT_MS = 10_000;

    private EventDispatcher dispatcher(InProcessEventStore store, EventHandler handler, MutableClock clock) {
        // maxAttempts=3, 初始退避 100ms, 2 倍指数, 上限 1000ms
        return new EventDispatcher(store, handler, new RetryPolicy(3, 100, 2.0, 1000),
                CLAIM_TIMEOUT_MS, clock);
    }

    @Test
    void retryableFailureIsRetriedWithBackoffThenExhaustedToDead() {
        MutableClock clock = new MutableClock(1_000_000);
        InProcessEventStore store = new InProcessEventStore(dir.resolve("journal.jsonl"), clock);
        AtomicInteger calls = new AtomicInteger();
        EventDispatcher dispatcher = dispatcher(store, event -> {
            calls.incrementAndGet();
            throw new RuntimeException("transient db glitch");
        }, clock);

        store.commitBusinessAndEvent("op-1", () -> { }, new NewEvent("agg-1", "p"));
        String eventId = store.findByAggregate("agg-1").get(0).getId();

        dispatcher.runOnce(); // 第 1 次失败 -> 退避 100ms
        StoredEvent e = store.find(eventId).orElseThrow();
        assertEquals(EventStatus.PENDING, e.getStatus());
        assertEquals(1, e.getAttempts());
        assertEquals(1_000_100, e.getAvailableAtEpochMs(), "退避时间应符合指数策略");

        dispatcher.runOnce(); // 退避未到，不可认领
        assertEquals(1, calls.get(), "退避期内不得提前重试");

        clock.advance(100);
        dispatcher.runOnce(); // 第 2 次失败 -> 退避 200ms
        assertEquals(2, store.find(eventId).orElseThrow().getAttempts());

        clock.advance(200);
        dispatcher.runOnce(); // 第 3 次失败 -> 达到 maxAttempts=3 -> 死信
        e = store.find(eventId).orElseThrow();
        assertEquals(EventStatus.DEAD, e.getStatus());
        assertEquals(DeadLetterReason.MAX_RETRIES_EXCEEDED, e.getDeadReason());
        assertEquals(3, calls.get());
        assertEquals(1, store.stats().dead());
        assertEquals(1L, store.stats().deadByReason().get("MAX_RETRIES_EXCEEDED"));
    }

    @Test
    void nonRetryableFailureGoesStraightToDeadWithDistinctReason() {
        MutableClock clock = new MutableClock(0);
        InProcessEventStore store = new InProcessEventStore(dir.resolve("journal.jsonl"), clock);
        EventDispatcher dispatcher = dispatcher(store, event -> {
            throw new NonRetryableException("payload schema invalid");
        }, clock);

        store.commitBusinessAndEvent("op-1", () -> { }, new NewEvent("agg-2", "bad"));
        String eventId = store.findByAggregate("agg-2").get(0).getId();

        dispatcher.runOnce();
        StoredEvent e = store.find(eventId).orElseThrow();
        assertEquals(EventStatus.DEAD, e.getStatus());
        assertEquals(DeadLetterReason.NON_RETRYABLE_FAILURE, e.getDeadReason(),
                "不可重试故障须与重试耗尽可区分");
        assertEquals(1, e.getAttempts(), "不可重试故障不应多次尝试");
    }

    @Test
    void deadEventCanBeRequeuedAndIllegalRequeueIsRejected() {
        MutableClock clock = new MutableClock(0);
        InProcessEventStore store = new InProcessEventStore(dir.resolve("journal.jsonl"), clock);
        AtomicInteger calls = new AtomicInteger();
        EventDispatcher dispatcher = dispatcher(store, event -> {
            if (calls.incrementAndGet() == 1) {
                throw new NonRetryableException("poison");
            }
        }, clock);

        store.commitBusinessAndEvent("op-1", () -> { }, new NewEvent("agg-3", "p"));
        String eventId = store.findByAggregate("agg-3").get(0).getId();

        dispatcher.runOnce();
        assertEquals(EventStatus.DEAD, store.find(eventId).orElseThrow().getStatus());

        // 死信重投 -> 重新投递成功
        store.requeueDead(eventId);
        StoredEvent e = store.find(eventId).orElseThrow();
        assertEquals(EventStatus.PENDING, e.getStatus());
        assertEquals(0, e.getAttempts(), "重投后尝试计数清零");
        dispatcher.runOnce();
        assertEquals(EventStatus.DELIVERED, store.find(eventId).orElseThrow().getStatus());
        assertEquals(2, calls.get());

        // 非法重投：非 DEAD 状态必须被拒绝
        assertThrows(IllegalStateException.class, () -> store.requeueDead(eventId));
        store.commitBusinessAndEvent("op-2", () -> { }, new NewEvent("agg-3", "p2"));
        String pendingId = store.findByAggregate("agg-3").get(1).getId();
        assertThrows(IllegalStateException.class, () -> store.requeueDead(pendingId));
    }

    @Test
    void failedEventDoesNotBlockOtherAggregates() {
        MutableClock clock = new MutableClock(0);
        InProcessEventStore store = new InProcessEventStore(dir.resolve("journal.jsonl"), clock);
        EventDispatcher dispatcher = dispatcher(store, event -> {
            if (event.getAggregateId().equals("agg-bad")) {
                throw new NonRetryableException("poison");
            }
        }, clock);

        store.commitBusinessAndEvent("op-bad", () -> { }, new NewEvent("agg-bad", "p"));
        store.commitBusinessAndEvent("op-good", () -> { }, new NewEvent("agg-good", "p"));

        dispatcher.runOnce();
        assertEquals(EventStatus.DEAD, store.findByAggregate("agg-bad").get(0).getStatus());
        assertEquals(EventStatus.DELIVERED, store.findByAggregate("agg-good").get(0).getStatus(),
                "单个失败事件不得阻塞其他聚合");
    }
}
