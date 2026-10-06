package com.github.highcumontoa.reliableeventpipelinejava.core;

/** 死信原因，可区分。 */
public enum DeadReason {
    /** 重试次数耗尽。 */
    RETRIES_EXHAUSTED,
    /** 不可重试故障。 */
    NON_RETRYABLE_FAILURE,
    /** 延迟等待超过配置上限（与处理失败类原因区分）。 */
    DELAY_LIMIT_EXCEEDED
}
