package com.github.highcumontoa.reliableeventpipelinejava.tests;

import static org.assertj.core.api.Assertions.assertThat;

import com.github.highcumontoa.reliableeventpipelinejava.core.PipelineEvent;
import com.github.highcumontoa.reliableeventpipelinejava.core.PipelineStats;
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

/** 原子提交与幂等提交。 */
class AtomicCommitAndIdempotencyTest {

    private static final Logger log = LoggerFactory.getLogger(AtomicCommitAndIdempotencyTest.class);

    @TempDir
    Path tempDir;

    private EventStore newStore(String name) {
        EventStore store = new EventStore(tempDir.resolve(name));
        store.load();
        return store;
    }

    @Test
    void businessAndEventCommittedAtomically() {
        EventStore store = newStore("atomic.json");
        PipelineEvent event = store.commitBusinessWithEvent(
                "key-1", "agg-1", "order-created", "payload-1", Instant.now());

        assertThat(store.listBusinessRecords()).hasSize(1);
        assertThat(store.listEvents()).hasSize(1);
        assertThat(store.listBusinessRecords().get(0).idempotencyKey).isEqualTo("key-1");
        log.info("判定依据: 业务记录数=1 且事件数=1, eventId={} status={}",
                event.eventId, event.status);
    }

    @Test
    void duplicateIdempotencyKeyProducesNoDuplicateEvent() {
        EventStore store = newStore("idem.json");
        PipelineEvent first = store.commitBusinessWithEvent(
                "key-dup", "agg-1", "data-v1", "payload-1", Instant.now());
        PipelineEvent second = store.commitBusinessWithEvent(
                "key-dup", "agg-1", "data-v2", "payload-2", Instant.now());

        assertThat(second.eventId).isEqualTo(first.eventId);
        assertThat(store.listEvents()).hasSize(1);
        assertThat(store.listBusinessRecords()).hasSize(1);
        // 业务数据保留首次提交内容
        assertThat(store.listBusinessRecords().get(0).data).isEqualTo("data-v1");
        log.info("判定依据: 重复幂等键返回同一事件 eventId={} 且业务/事件各 1 条", first.eventId);
    }

    @Test
    void perAggregateDeliveryPreservesCommitOrder() throws Exception {
        EventStore store = newStore("order.json");
        PipelineProperties props = new PipelineProperties();
        props.setWorkerThreads(3);
        props.setPollInterval(Duration.ofMillis(5));
        EventPipeline pipeline = new EventPipeline(store, props);

        for (int i = 0; i < 5; i++) {
            pipeline.submit("k-a-" + i, "agg-A", "d", "p-a-" + i);
            pipeline.submit("k-b-" + i, "agg-B", "d", "p-b-" + i);
        }

        List<String> deliveredA = new CopyOnWriteArrayList<>();
        List<String> deliveredB = new CopyOnWriteArrayList<>();
        pipeline.start((event, context) -> {
            (event.aggregateId.equals("agg-A") ? deliveredA : deliveredB).add(event.payload);
        });

        long deadline = System.currentTimeMillis() + 10_000;
        while (pipeline.stats().acked() < 10 && System.currentTimeMillis() < deadline) {
            Thread.sleep(20);
        }
        pipeline.stop();

        PipelineStats stats = pipeline.stats();
        assertThat(stats.acked()).isEqualTo(10);
        assertThat(deliveredA).containsExactly("p-a-0", "p-a-1", "p-a-2", "p-a-3", "p-a-4");
        assertThat(deliveredB).containsExactly("p-b-0", "p-b-1", "p-b-2", "p-b-3", "p-b-4");
        log.info("判定依据: 聚合A投递顺序={} 聚合B投递顺序={} acked={}",
                deliveredA, deliveredB, stats.acked());
    }

    @Test
    void concurrentClaimGrantsDistinctEventsOnly() throws Exception {
        EventStore store = newStore("claim.json");
        for (int i = 0; i < 10; i++) {
            store.commitBusinessWithEvent("k-" + i, "agg-" + i, "d", "p-" + i, Instant.now());
        }
        List<PipelineEvent> claimed = new CopyOnWriteArrayList<>();
        Thread[] threads = new Thread[8];
        for (int t = 0; t < threads.length; t++) {
            int idx = t;
            threads[t] = new Thread(() -> {
                for (int i = 0; i < 5; i++) {
                    Optional<PipelineEvent> e = store.claimNext(
                            "owner-" + idx, Duration.ofSeconds(30), Instant.now());
                    e.ifPresent(claimed::add);
                }
            });
        }
        for (Thread t : threads) {
            t.start();
        }
        for (Thread t : threads) {
            t.join();
        }
        assertThat(claimed).hasSize(10);
        assertThat(claimed.stream().map(e -> e.eventId).distinct()).hasSize(10);
        log.info("判定依据: 并发认领 {} 次, 去重后 {} 个事件, 无重复认领",
                claimed.size(), claimed.stream().map(e -> e.eventId).distinct().count());
    }
}
