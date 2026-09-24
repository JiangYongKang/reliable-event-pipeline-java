package com.github.highcumontoa.reliableeventpipelinejava.event;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.highcumontoa.reliableeventpipelinejava.pipeline.PipelineStats;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;

/**
 * 基于写前日志（journal 文件）的进程内可恢复事件存储。
 *
 * <p>所有状态变更先追加到 journal 再应用到内存，进程重启后重放 journal 恢复。
 * 所有公共方法在同一锁下串行化，保证并发安全。</p>
 */
public class InProcessEventStore implements EventStore {

    private static final Logger log = LoggerFactory.getLogger(InProcessEventStore.class);

    /** journal 记录：type ∈ {commit, update, consumed}。 */
    public static class JournalEntry {
        public String type;
        public String operationKey;
        public String eventId;
        public StoredEvent event;

        public JournalEntry() { }

        JournalEntry(String type) { this.type = type; }
    }

    private final Object lock = new Object();
    private final Path journalFile;
    private final Clock clock;
    private final ObjectMapper mapper = new ObjectMapper();

    /** 内存状态：事件、已提交操作键、已消费事件、各聚合下一个序号。 */
    private final Map<String, StoredEvent> events = new LinkedHashMap<>();
    private final Set<String> committedOps = new TreeSet<>();
    private final Set<String> consumed = new TreeSet<>();
    private final Map<String, Long> nextSeqByAggregate = new HashMap<>();

    public InProcessEventStore(Path journalFile) {
        this(journalFile, Clock.systemUTC());
    }

    public InProcessEventStore(Path journalFile, Clock clock) {
        this.journalFile = journalFile;
        this.clock = clock;
        replay();
    }

    private void replay() {
        try {
            if (Files.notExists(journalFile)) {
                Files.createDirectories(journalFile.toAbsolutePath().getParent());
                Files.createFile(journalFile);
                return;
            }
            for (String line : Files.readAllLines(journalFile)) {
                if (line.isBlank()) {
                    continue;
                }
                JournalEntry entry = mapper.readValue(line, JournalEntry.class);
                switch (entry.type) {
                    case "commit" -> {
                        committedOps.add(entry.operationKey);
                        putEvent(entry.event);
                    }
                    case "update" -> putEvent(entry.event);
                    case "consumed" -> consumed.add(entry.eventId);
                    default -> throw new IllegalStateException("unknown journal type: " + entry.type);
                }
            }
            log.info("replayed journal {}: events={}, committedOps={}, consumed={}",
                    journalFile, events.size(), committedOps.size(), consumed.size());
        } catch (IOException e) {
            throw new UncheckedIOException("failed to replay journal " + journalFile, e);
        }
    }

    private void putEvent(StoredEvent event) {
        events.put(event.getId(), event);
        nextSeqByAggregate.merge(event.getAggregateId(), event.getSeq() + 1, Math::max);
    }

    private void append(JournalEntry entry) {
        try {
            Files.writeString(journalFile, mapper.writeValueAsString(entry) + System.lineSeparator(),
                    StandardOpenOption.APPEND);
        } catch (IOException e) {
            throw new UncheckedIOException("failed to append journal " + journalFile, e);
        }
    }

    @Override
    public CommitOutcome commitBusinessAndEvent(String operationKey, Runnable businessWrite, NewEvent event) {
        synchronized (lock) {
            if (committedOps.contains(operationKey)) {
                log.info("commit ignored as duplicate: operationKey={}", operationKey);
                return CommitOutcome.DUPLICATE_IGNORED;
            }
            // 业务写入失败则整体失败：不记录操作键、不入队事件，无中间状态。
            businessWrite.run();

            long now = clock.millis();
            StoredEvent stored = new StoredEvent();
            stored.setId(UUID.randomUUID().toString());
            stored.setAggregateId(event.aggregateId());
            stored.setSeq(nextSeqByAggregate.getOrDefault(event.aggregateId(), 0L));
            stored.setPayload(event.payload());
            stored.setStatus(EventStatus.PENDING);
            stored.setAvailableAtEpochMs(now);
            stored.setCreatedAtEpochMs(now);
            stored.setUpdatedAtEpochMs(now);

            JournalEntry entry = new JournalEntry("commit");
            entry.operationKey = operationKey;
            entry.event = stored;
            append(entry);

            committedOps.add(operationKey);
            putEvent(stored);
            log.info("committed: operationKey={} eventId={} aggregate={} seq={}",
                    operationKey, stored.getId(), stored.getAggregateId(), stored.getSeq());
            return CommitOutcome.COMMITTED;
        }
    }

    @Override
    public List<StoredEvent> claimBatch(String owner, int max, long nowEpochMs, long claimTimeoutMs) {
        synchronized (lock) {
            // 每个聚合的队首 = 该聚合中序号最小的未完成（PENDING/CLAIMED）事件。
            Map<String, StoredEvent> headByAggregate = new HashMap<>();
            for (StoredEvent e : events.values()) {
                if (e.getStatus() != EventStatus.PENDING && e.getStatus() != EventStatus.CLAIMED) {
                    continue;
                }
                headByAggregate.merge(e.getAggregateId(), e,
                        (a, b) -> a.getSeq() <= b.getSeq() ? a : b);
            }
            List<StoredEvent> claimed = new ArrayList<>();
            for (StoredEvent head : headByAggregate.values()) {
                if (claimed.size() >= max) {
                    break;
                }
                boolean freshPending = head.getStatus() == EventStatus.PENDING
                        && head.getAvailableAtEpochMs() <= nowEpochMs;
                boolean expiredClaim = head.getStatus() == EventStatus.CLAIMED
                        && head.getClaimDeadlineEpochMs() <= nowEpochMs;
                if (!freshPending && !expiredClaim) {
                    continue;
                }
                head.setStatus(EventStatus.CLAIMED);
                head.setClaimOwner(owner);
                head.setClaimDeadlineEpochMs(nowEpochMs + claimTimeoutMs);
                head.setUpdatedAtEpochMs(nowEpochMs);
                JournalEntry entry = new JournalEntry("update");
                entry.event = head;
                append(entry);
                claimed.add(head);
                log.info("claimed: eventId={} aggregate={} seq={} owner={} expiredReclaim={}",
                        head.getId(), head.getAggregateId(), head.getSeq(), owner, expiredClaim);
            }
            return claimed;
        }
    }

    @Override
    public boolean acknowledge(String eventId, String owner) {
        synchronized (lock) {
            StoredEvent e = events.get(eventId);
            if (e == null || e.getStatus() != EventStatus.CLAIMED || !owner.equals(e.getClaimOwner())) {
                log.warn("ack rejected: eventId={} owner={} state={}", eventId, owner,
                        e == null ? "missing" : e.getStatus() + "/" + e.getClaimOwner());
                return false;
            }
            e.setStatus(EventStatus.DELIVERED);
            e.setClaimOwner(null);
            e.setClaimDeadlineEpochMs(0);
            e.setUpdatedAtEpochMs(clock.millis());
            JournalEntry entry = new JournalEntry("update");
            entry.event = e;
            append(entry);
            log.info("acknowledged: eventId={} aggregate={} seq={} attempts={}",
                    e.getId(), e.getAggregateId(), e.getSeq(), e.getAttempts());
            return true;
        }
    }

    @Override
    public boolean failRetryable(String eventId, String owner, long nextAvailableAtEpochMs, String error) {
        synchronized (lock) {
            StoredEvent e = events.get(eventId);
            if (e == null || e.getStatus() != EventStatus.CLAIMED || !owner.equals(e.getClaimOwner())) {
                return false;
            }
            e.setStatus(EventStatus.PENDING);
            e.setAttempts(e.getAttempts() + 1);
            e.setAvailableAtEpochMs(nextAvailableAtEpochMs);
            e.setClaimOwner(null);
            e.setClaimDeadlineEpochMs(0);
            e.setLastError(error);
            e.setUpdatedAtEpochMs(clock.millis());
            JournalEntry entry = new JournalEntry("update");
            entry.event = e;
            append(entry);
            log.info("retry scheduled: eventId={} attempts={} nextAvailableAt={} error={}",
                    e.getId(), e.getAttempts(), nextAvailableAtEpochMs, error);
            return true;
        }
    }

    @Override
    public boolean failToDead(String eventId, String owner, DeadLetterReason reason, String error) {
        synchronized (lock) {
            StoredEvent e = events.get(eventId);
            if (e == null || e.getStatus() != EventStatus.CLAIMED || !owner.equals(e.getClaimOwner())) {
                return false;
            }
            e.setStatus(EventStatus.DEAD);
            e.setAttempts(e.getAttempts() + 1);
            e.setDeadReason(reason);
            e.setClaimOwner(null);
            e.setClaimDeadlineEpochMs(0);
            e.setLastError(error);
            e.setUpdatedAtEpochMs(clock.millis());
            JournalEntry entry = new JournalEntry("update");
            entry.event = e;
            append(entry);
            log.info("dead-lettered: eventId={} reason={} attempts={} error={}",
                    e.getId(), reason, e.getAttempts(), error);
            return true;
        }
    }

    @Override
    public void requeueDead(String eventId) {
        synchronized (lock) {
            StoredEvent e = events.get(eventId);
            if (e == null) {
                throw new IllegalArgumentException("unknown event: " + eventId);
            }
            if (e.getStatus() != EventStatus.DEAD) {
                throw new IllegalStateException(
                        "only DEAD events can be requeued, eventId=" + eventId + " status=" + e.getStatus());
            }
            e.setStatus(EventStatus.PENDING);
            e.setAttempts(0);
            e.setDeadReason(null);
            e.setLastError(null);
            e.setAvailableAtEpochMs(clock.millis());
            e.setUpdatedAtEpochMs(clock.millis());
            JournalEntry entry = new JournalEntry("update");
            entry.event = e;
            append(entry);
            log.info("dead event requeued: eventId={} aggregate={} seq={}",
                    e.getId(), e.getAggregateId(), e.getSeq());
        }
    }

    @Override
    public boolean consumeOnce(String eventId, Runnable sideEffect) {
        synchronized (lock) {
            if (consumed.contains(eventId)) {
                log.info("consume skipped as duplicate: eventId={}", eventId);
                return false;
            }
            // 副作用失败则不记录消费标记，可再次消费。
            sideEffect.run();
            JournalEntry entry = new JournalEntry("consumed");
            entry.eventId = eventId;
            append(entry);
            consumed.add(eventId);
            return true;
        }
    }

    @Override
    public Optional<StoredEvent> find(String eventId) {
        synchronized (lock) {
            return Optional.ofNullable(events.get(eventId));
        }
    }

    @Override
    public List<StoredEvent> findByAggregate(String aggregateId) {
        synchronized (lock) {
            return events.values().stream()
                    .filter(e -> e.getAggregateId().equals(aggregateId))
                    .sorted(Comparator.comparingLong(StoredEvent::getSeq))
                    .toList();
        }
    }

    @Override
    public PipelineStats stats() {
        synchronized (lock) {
            long pending = 0, claimed = 0, delivered = 0, dead = 0;
            Map<String, Long> deadByReason = new HashMap<>();
            for (StoredEvent e : events.values()) {
                switch (e.getStatus()) {
                    case PENDING -> pending++;
                    case CLAIMED -> claimed++;
                    case DELIVERED -> delivered++;
                    case DEAD -> {
                        dead++;
                        deadByReason.merge(e.getDeadReason().name(), 1L, Long::sum);
                    }
                }
            }
            return new PipelineStats(pending, claimed, delivered, dead, consumed.size(), deadByReason);
        }
    }
}
