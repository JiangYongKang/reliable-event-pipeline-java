package com.github.highcumontoa.reliableeventpipelinejava.core;

/** 业务副作用的执行状态（幂等账本）。 */
public enum EffectState {
    /** 尚未执行。 */
    NONE,
    /** 正在执行（持有执行租约）。 */
    EXECUTING,
    /** 已执行完成。 */
    DONE
}
