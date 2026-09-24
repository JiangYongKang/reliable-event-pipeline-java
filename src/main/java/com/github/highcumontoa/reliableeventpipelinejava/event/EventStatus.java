package com.github.highcumontoa.reliableeventpipelinejava.event;

/**
 * 事件在管道中的生命周期状态。
 */
public enum EventStatus {
    /** 已入队，等待被认领投递。 */
    PENDING,
    /** 已被某个消费者认领，正在投递中（带租约超时）。 */
    CLAIMED,
    /** 已投递并被确认。 */
    DELIVERED,
    /** 进入死信：重试耗尽或不可重试故障。 */
    DEAD
}
