package com.github.highcumontoa.reliableeventpipelinejava.core;

import java.time.Duration;
import java.util.List;

/**
 * 积压水位视图：全局总量/最久积压时长 + 各聚合明细。
 */
public record BacklogView(
        int globalUnfinished,
        Duration globalOldestAge,
        List<AggregateBacklog> aggregates) {
}
