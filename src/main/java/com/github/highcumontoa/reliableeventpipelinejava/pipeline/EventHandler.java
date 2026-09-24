package com.github.highcumontoa.reliableeventpipelinejava.pipeline;

import com.github.highcumontoa.reliableeventpipelinejava.event.StoredEvent;

/**
 * 事件处理器。抛出 {@link NonRetryableException} 表示不可重试故障，
 * 抛出其他异常表示可重试故障。
 */
@FunctionalInterface
public interface EventHandler {
    void handle(StoredEvent event) throws Exception;
}
