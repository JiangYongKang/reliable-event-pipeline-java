package com.github.highcumontoa.reliableeventpipelinejava.tests;

import com.github.highcumontoa.reliableeventpipelinejava.event.CommitOutcome;
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
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** 并发重复投递、幂等消费与并发安全认领。 */
class ConcurrentDeliveryTest {

    @TempDir
    Path dir;

    @Test
    void concurrentDuplicateCommitsProduceSingleEvent() throws InterruptedException {
        InProcessEventStore store = new InProcessEventStore(dir.resolve("journal.jsonl"));
        int threads = 16;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        AtomicInteger committed = new AtomicInteger();
        AtomicInteger businessWrites = new AtomicInteger();

        for (int i = 0; i < threads; i++) {
            pool.submit(() -> {
                await(start);
                if (store.commitBusinessAndEvent("op-dup",
                        businessWrites::incrementAndGet, new NewEvent("agg-x", "p")) == CommitOutcome.COMMITTED) {
                    committed.incrementAndGet();
                }
            });
        }
        start.countDown();
        pool.shutdown();
        assertTrue(pool.awaitTermination(10, TimeUnit.SECONDS));

        assertEquals(1, committed.get(), "同一业务操作并发重复提交只能生效一次");
        assertEquals(1, businessWrites.get());
        assertEquals(1, store.findByAggregate("agg-x").size());
    }

    @Test
    void concurrentConsumeOnceAppliesSideEffectOnlyOnce() throws InterruptedException {
        InProcessEventStore store = new InProcessEventStore(dir.resolve("journal.jsonl"));
        store.commitBusinessAndEvent("op-1", () -> { }, new NewEvent("agg-y", "p"));
        String eventId = store.findByAggregate("agg-y").get(0).getId();

        int threads = 16;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        AtomicInteger sideEffects = new AtomicInteger();
        AtomicInteger wins = new AtomicInteger();

        for (int i = 0; i < threads; i++) {
            pool.submit(() -> {
                await(start);
                if (store.consumeOnce(eventId, sideEffects::incrementAndGet)) {
                    wins.incrementAndGet();
                }
            });
        }
        start.countDown();
        pool.shutdown();
        assertTrue(pool.awaitTermination(10, TimeUnit.SECONDS));

        assertEquals(1, sideEffects.get(), "并发重复消费时业务副作用只能生效一次");
        assertEquals(1, wins.get());
        assertEquals(1, store.stats().consumed());
    }

    @Test
    void concurrentDispatchersDeliverEachEventOnceAndKeepAggregateOrder() throws InterruptedException {
        MutableClock clock = new MutableClock(0);
        InProcessEventStore store = new InProcessEventStore(dir.resolve("journal.jsonl"), clock);
        int aggregates = 4;
        int eventsPerAggregate = 10;
        for (int a = 0; a < aggregates; a++) {
            for (int s = 0; s < eventsPerAggregate; s++) {
                store.commitBusinessAndEvent("op-" + a + "-" + s, () -> { },
                        new NewEvent("agg-" + a, "payload-" + s));
            }
        }

        Map<String, List<Long>> deliveredSeqByAggregate = new ConcurrentHashMap<>();
        AtomicInteger sideEffects = new AtomicInteger();
        RetryPolicy retryPolicy = new RetryPolicy(3, 100, 2.0, 1000);

        int dispatchers = 6;
        ExecutorService pool = Executors.newFixedThreadPool(dispatchers);
        CountDownLatch start = new CountDownLatch(1);
        for (int i = 0; i < dispatchers; i++) {
            pool.submit(() -> {
                await(start);
                EventDispatcher dispatcher = new EventDispatcher(store, event -> {
                    // 幂等消费：即便事件被多轮投递，副作用只生效一次
                    store.consumeOnce(event.getId(), () -> {
                        sideEffects.incrementAndGet();
                        deliveredSeqByAggregate
                                .computeIfAbsent(event.getAggregateId(), k -> new CopyOnWriteArrayList<>())
                                .add(event.getSeq());
                    });
                }, retryPolicy, 5_000, clock);
                for (int round = 0; round < 200; round++) {
                    dispatcher.runOnce();
                }
            });
        }
        start.countDown();
        pool.shutdown();
        assertTrue(pool.awaitTermination(30, TimeUnit.SECONDS));

        int total = aggregates * eventsPerAggregate;
        assertEquals(total, sideEffects.get(), "每个事件的副作用恰好生效一次");
        assertEquals(total, store.stats().delivered(), "全部事件最终确认投递");
        assertEquals(0, store.stats().pending());
        assertEquals(0, store.stats().claimed());
        assertEquals(total, store.stats().consumed());
        for (int a = 0; a < aggregates; a++) {
            List<Long> seqs = deliveredSeqByAggregate.get("agg-" + a);
            assertEquals(eventsPerAggregate, seqs.size());
            for (int s = 0; s < eventsPerAggregate; s++) {
                assertEquals((long) s, seqs.get(s), "同一聚合必须按提交顺序投递");
            }
        }
        // 状态与统计一致：每个事件都是 DELIVERED
        for (int a = 0; a < aggregates; a++) {
            for (StoredEvent e : store.findByAggregate("agg-" + a)) {
                assertEquals(EventStatus.DELIVERED, e.getStatus());
            }
        }
    }

    private static void await(CountDownLatch latch) {
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }
}
