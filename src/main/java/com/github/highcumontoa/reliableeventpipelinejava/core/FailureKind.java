package com.github.highcumontoa.reliableeventpipelinejava.core;

/** 失败分类：决定走重试还是直接死信。 */
public enum FailureKind {
    /** 可重试故障：按退避策略重试，超过上限后进入死信。 */
    RETRYABLE,
    /** 不可重试故障：立即进入死信。 */
    NON_RETRYABLE
}
