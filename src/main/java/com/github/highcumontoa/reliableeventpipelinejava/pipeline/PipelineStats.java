package com.github.highcumontoa.reliableeventpipelinejava.pipeline;

import java.util.Map;

/**
 * 管道各阶段事件数量的快照，须始终与实际处理结果一致。
 */
public record PipelineStats(long pending,
                            long claimed,
                            long delivered,
                            long dead,
                            long consumed,
                            Map<String, Long> deadByReason) {
}
