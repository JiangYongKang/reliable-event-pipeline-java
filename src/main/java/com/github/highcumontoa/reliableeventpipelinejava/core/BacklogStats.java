package com.github.highcumontoa.reliableeventpipelinejava.core;

import java.util.List;
import java.util.Map;

/**
 * 积压水位统计：未完成（PENDING + IN_FLIGHT）事件的数量与最久积压时长。
 * 同时给出全局视图与按聚合的明细。
 */
public record BacklogStats(
        /** 全局未完成事件数。 */
        long totalBacklog,
        /** 全局最久积压时长（毫秒）；无积压时为 null。 */
        Long oldestBacklogAgeMillis,
        /** 按聚合的积压明细，键为 aggregateId。 */
        Map<String, AggregateBacklog> byAggregate) {

    /** 单个聚合的积压水位。 */
    public record AggregateBacklog(
            /** 该聚合未完成事件数。 */
            long backlog,
            /** 该聚合最久积压时长（毫秒）。 */
            long oldestBacklogAgeMillis) {
    }

    /** 空积压。 */
    public static BacklogStats empty() {
        return new BacklogStats(0, null, Map.of());
    }
}
