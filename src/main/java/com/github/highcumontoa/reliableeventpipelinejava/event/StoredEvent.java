package com.github.highcumontoa.reliableeventpipelinejava.event;

/**
 * 已持久化的事件记录（含投递状态与租约信息）。
 */
public class StoredEvent {
    private String id;
    private String aggregateId;
    private long seq;
    private String payload;
    private EventStatus status;
    private int attempts;
    private String claimOwner;
    private long claimDeadlineEpochMs;
    private long availableAtEpochMs;
    private DeadLetterReason deadReason;
    private String lastError;
    private long createdAtEpochMs;
    private long updatedAtEpochMs;

    public String getId() { return id; }
    public void setId(String id) { this.id = id; }
    public String getAggregateId() { return aggregateId; }
    public void setAggregateId(String aggregateId) { this.aggregateId = aggregateId; }
    public long getSeq() { return seq; }
    public void setSeq(long seq) { this.seq = seq; }
    public String getPayload() { return payload; }
    public void setPayload(String payload) { this.payload = payload; }
    public EventStatus getStatus() { return status; }
    public void setStatus(EventStatus status) { this.status = status; }
    public int getAttempts() { return attempts; }
    public void setAttempts(int attempts) { this.attempts = attempts; }
    public String getClaimOwner() { return claimOwner; }
    public void setClaimOwner(String claimOwner) { this.claimOwner = claimOwner; }
    public long getClaimDeadlineEpochMs() { return claimDeadlineEpochMs; }
    public void setClaimDeadlineEpochMs(long v) { this.claimDeadlineEpochMs = v; }
    public long getAvailableAtEpochMs() { return availableAtEpochMs; }
    public void setAvailableAtEpochMs(long v) { this.availableAtEpochMs = v; }
    public DeadLetterReason getDeadReason() { return deadReason; }
    public void setDeadReason(DeadLetterReason deadReason) { this.deadReason = deadReason; }
    public String getLastError() { return lastError; }
    public void setLastError(String lastError) { this.lastError = lastError; }
    public long getCreatedAtEpochMs() { return createdAtEpochMs; }
    public void setCreatedAtEpochMs(long v) { this.createdAtEpochMs = v; }
    public long getUpdatedAtEpochMs() { return updatedAtEpochMs; }
    public void setUpdatedAtEpochMs(long v) { this.updatedAtEpochMs = v; }
}
