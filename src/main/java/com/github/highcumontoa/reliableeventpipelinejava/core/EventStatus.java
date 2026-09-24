package com.github.highcumontoa.reliableeventpipelinejava.core;

/** 事件生命周期状态。 */
public enum EventStatus {
    /** 已入队，等待被认领。 */
    PENDING,
    /** 已被某个工作线程认领，正在处理。 */
    IN_FLIGHT,
    /** 处理成功，已确认。 */
    ACKED,
    /** 超过重试上限或不可重试，进入死信。 */
    DEAD
}
