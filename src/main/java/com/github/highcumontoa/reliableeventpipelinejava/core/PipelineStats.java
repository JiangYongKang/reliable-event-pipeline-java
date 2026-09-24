package com.github.highcumontoa.reliableeventpipelinejava.core;

/** 管道统计量，由事件实际状态推导。 */
public record PipelineStats(
        int totalEvents,
        int pending,
        int inFlight,
        int acked,
        int dead,
        int businessRecords,
        int effectsDone) {
}
