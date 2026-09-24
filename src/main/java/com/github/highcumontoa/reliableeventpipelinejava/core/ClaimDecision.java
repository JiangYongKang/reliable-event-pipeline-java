package com.github.highcumontoa.reliableeventpipelinejava.core;

/** 副作用执行租约的认领结果。 */
public enum ClaimDecision {
    /** 获得执行权，调用方应执行业务副作用。 */
    EXECUTE,
    /** 副作用已完成，跳过。 */
    SKIP_ALREADY_DONE,
    /** 其他消费者正在执行，本次投递应稍后重试。 */
    BUSY
}
