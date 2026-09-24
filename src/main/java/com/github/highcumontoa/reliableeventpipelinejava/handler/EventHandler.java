package com.github.highcumontoa.reliableeventpipelinejava.handler;

import com.github.highcumontoa.reliableeventpipelinejava.core.PipelineEvent;

/** 业务事件处理器：实现方在其中执行幂等的业务副作用。 */
public interface EventHandler {
    /**
     * 处理一个事件。
     *
     * @param event 已认领的事件
     * @param context 幂等副作用上下文
     * @throws EventProcessingException 携带失败分类的受控失败
     */
    void handle(PipelineEvent event, EffectContext context);
}
