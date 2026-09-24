package com.github.highcumontoa.reliableeventpipelinejava.handler;

/** 幂等副作用执行上下文，由管道在投递时提供给使用。 */
public interface EffectContext {
    /** 尝试为当前事件认领副作用执行租约。 */
    com.github.highcumontoa.reliableeventpipelinejava.core.ClaimDecision tryBeginEffect();

    /** 标记当前事件的副作用已执行完成。 */
    void completeEffect();
}
