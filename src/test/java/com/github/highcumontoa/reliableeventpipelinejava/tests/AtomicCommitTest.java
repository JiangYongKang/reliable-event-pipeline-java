package com.github.highcumontoa.reliableeventpipelinejava.tests;

import com.github.highcumontoa.reliableeventpipelinejava.event.CommitOutcome;
import com.github.highcumontoa.reliableeventpipelinejava.event.InProcessEventStore;
import com.github.highcumontoa.reliableeventpipelinejava.event.NewEvent;
import com.github.highcumontoa.reliableeventpipelinejava.event.StoredEvent;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** 业务写入与事件入队的原子提交与幂等提交。 */
class AtomicCommitTest {

    @TempDir
    Path dir;

    @Test
    void duplicateOperationKeyProducesNoDuplicateEventOrBusinessWrite() {
        InProcessEventStore store = new InProcessEventStore(dir.resolve("journal.jsonl"));
        AtomicInteger businessWrites = new AtomicInteger();

        CommitOutcome first = store.commitBusinessAndEvent("op-1",
                businessWrites::incrementAndGet, new NewEvent("agg-A", "payload-1"));
        CommitOutcome second = store.commitBusinessAndEvent("op-1",
                businessWrites::incrementAndGet, new NewEvent("agg-A", "payload-1"));

        assertEquals(CommitOutcome.COMMITTED, first);
        assertEquals(CommitOutcome.DUPLICATE_IGNORED, second);
        assertEquals(1, businessWrites.get(), "业务副作用只能生效一次");
        assertEquals(1, store.findByAggregate("agg-A").size(), "重复提交不得产生重复事件");
    }

    @Test
    void failedBusinessWriteLeavesNoEventAndNoOperationKey() {
        InProcessEventStore store = new InProcessEventStore(dir.resolve("journal.jsonl"));

        assertThrows(RuntimeException.class, () -> store.commitBusinessAndEvent("op-2",
                () -> { throw new RuntimeException("business db down"); },
                new NewEvent("agg-B", "payload")));

        assertEquals(0, store.findByAggregate("agg-B").size(), "提交失败不得留下只有事件的中间状态");
        assertEquals(0, store.stats().pending());

        // 操作键未被占用，修复后可重试成功。
        CommitOutcome retry = store.commitBusinessAndEvent("op-2", () -> { }, new NewEvent("agg-B", "payload"));
        assertEquals(CommitOutcome.COMMITTED, retry);
        assertEquals(1, store.findByAggregate("agg-B").size());
    }

    @Test
    void aggregateSequencesAreAssignedInCommitOrder() {
        InProcessEventStore store = new InProcessEventStore(dir.resolve("journal.jsonl"));
        store.commitBusinessAndEvent("op-a", () -> { }, new NewEvent("agg-C", "p0"));
        store.commitBusinessAndEvent("op-b", () -> { }, new NewEvent("agg-C", "p1"));
        store.commitBusinessAndEvent("op-c", () -> { }, new NewEvent("agg-D", "p0"));

        List<StoredEvent> events = store.findByAggregate("agg-C");
        assertEquals(0, events.get(0).getSeq());
        assertEquals(1, events.get(1).getSeq());
        assertEquals(0, store.findByAggregate("agg-D").get(0).getSeq(), "序号按聚合独立分配");
    }
}
