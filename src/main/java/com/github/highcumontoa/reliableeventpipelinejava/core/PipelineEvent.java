package com.github.highcumontoa.reliableeventpipelinejava.core;

import java.time.Instant;

/** 管道事件记录。 */
public class PipelineEvent {
    public String eventId;
    public String idempotencyKey;
    public String aggregateId;
    public long seq;
    public String payload;
    public EventStatus status = EventStatus.PENDING;
    public int deliveryAttempts;
    public int redeliveries;
    public DeadReason deadReason;
    public String lastError;
    public String claimedBy;
    public Instant claimExpiresAt;
    public Instant nextAttemptAt;
    public Instant createdAt;
    public Instant updatedAt;

    public PipelineEvent() {
    }
}
