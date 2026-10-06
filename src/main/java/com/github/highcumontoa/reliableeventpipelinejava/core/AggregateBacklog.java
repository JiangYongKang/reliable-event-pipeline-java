package com.github.highcumontoa.reliableeventpipelinejava.core;

import java.time.Duration;
import java.time.Instant;

/**
 * 单个聚合的积压视图。
 * unfinished：尚未处理完（PENDING / IN_FLIGHT）的事件数；
 * oldestAge / oldestUnfinishedAt：其中最早一条的积压时长与产生时刻；
 * paused：该聚合当前是否处于暂停投递状态。
 */
public record AggregateBacklog(
        String aggregateId,
        int unfinished,
        Duration oldestAge,
        Instant oldestUnfinishedAt,
        boolean paused) {
}
