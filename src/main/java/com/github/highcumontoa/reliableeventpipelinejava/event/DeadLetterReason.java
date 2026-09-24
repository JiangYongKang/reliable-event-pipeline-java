package com.github.highcumontoa.reliableeventpipelinejava.event;

/**
 * 事件进入死信的可区分原因。
 */
public enum DeadLetterReason {
    /** 可重试故障但重试次数耗尽。 */
    MAX_RETRIES_EXCEEDED,
    /** 不可重试故障，直接隔离。 */
    NON_RETRYABLE_FAILURE
}
