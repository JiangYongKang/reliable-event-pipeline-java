package com.github.highcumontoa.reliableeventpipelinejava.event;

/**
 * 待入队的新事件。聚合内序号由存储在提交时分配。
 */
public record NewEvent(String aggregateId, String payload) {
}
