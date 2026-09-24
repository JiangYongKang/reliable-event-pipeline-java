package com.github.highcumontoa.reliableeventpipelinejava.support;

import com.github.highcumontoa.reliableeventpipelinejava.core.ClaimDecision;
import com.github.highcumontoa.reliableeventpipelinejava.core.PipelineEvent;
import com.github.highcumontoa.reliableeventpipelinejava.handler.EffectContext;
import com.github.highcumontoa.reliableeventpipelinejava.handler.EventHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 默认处理器：演示幂等副作用用法，仅记录日志。
 * 实际业务应替换为自己的 EventHandler 实现。
 */
public class LoggingEventHandler implements EventHandler {

    private static final Logger log = LoggerFactory.getLogger(LoggingEventHandler.class);

    @Override
    public void handle(PipelineEvent event, EffectContext context) {
        ClaimDecision decision = context.tryBeginEffect();
        switch (decision) {
            case EXECUTE -> {
                log.info("执行业务副作用 eventId={} aggregate={} seq={} payload={}",
                        event.eventId, event.aggregateId, event.seq, event.payload);
                context.completeEffect();
            }
            case SKIP_ALREADY_DONE -> log.info("副作用已完成，跳过 eventId={}", event.eventId);
            case BUSY -> throw com.github.highcumontoa.reliableeventpipelinejava.handler
                    .EventProcessingException.retryable("副作用租约被占用 eventId=" + event.eventId);
        }
    }
}
