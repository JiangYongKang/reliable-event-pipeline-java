package com.github.highcumontoa.reliableeventpipelinejava.pipeline;

/**
 * 聚合积压超过配置上限时抛出。
 * 业务方可据此与其他失败原因区分（例如映射为 HTTP 429 / 明确的错误码），
 * 该拒绝发生在业务记录与事件写入之前，不会产生半截状态。
 */
public class BacklogLimitExceededException extends RuntimeException {

    private final String aggregateId;
    private final int currentBacklog;
    private final int limit;

    public BacklogLimitExceededException(String aggregateId, int currentBacklog, int limit) {
        super("聚合 " + aggregateId + " 积压已达上限: current=" + currentBacklog + " limit=" + limit);
        this.aggregateId = aggregateId;
        this.currentBacklog = currentBacklog;
        this.limit = limit;
    }

    public String aggregateId() {
        return aggregateId;
    }

    public int currentBacklog() {
        return currentBacklog;
    }

    public int limit() {
        return limit;
    }
}
