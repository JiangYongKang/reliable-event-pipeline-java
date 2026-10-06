package com.github.highcumontoa.reliableeventpipelinejava.pipeline;

/**
 * 背压拒绝：目标聚合的积压水位达到配置上限，提交被拒绝。
 * 拒绝时业务记录与事件都未写入（原子地整体失败），调用方可据此区分并重试或降级。
 */
public class BackpressureRejectedException extends RuntimeException {

    private final String aggregateId;
    private final long currentBacklog;
    private final long maxBacklog;

    public BackpressureRejectedException(String aggregateId, long currentBacklog, long maxBacklog) {
        super("聚合 " + aggregateId + " 积压 " + currentBacklog
                + " 已达到上限 " + maxBacklog + ", 提交被拒绝(业务与事件均未写入)");
        this.aggregateId = aggregateId;
        this.currentBacklog = currentBacklog;
        this.maxBacklog = maxBacklog;
    }

    public String getAggregateId() {
        return aggregateId;
    }

    public long getCurrentBacklog() {
        return currentBacklog;
    }

    public long getMaxBacklog() {
        return maxBacklog;
    }
}
