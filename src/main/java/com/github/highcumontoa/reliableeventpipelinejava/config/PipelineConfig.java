package com.github.highcumontoa.reliableeventpipelinejava.config;

import com.github.highcumontoa.reliableeventpipelinejava.event.EventStore;
import com.github.highcumontoa.reliableeventpipelinejava.event.InProcessEventStore;
import com.github.highcumontoa.reliableeventpipelinejava.pipeline.EventDispatcher;
import com.github.highcumontoa.reliableeventpipelinejava.pipeline.EventHandler;
import com.github.highcumontoa.reliableeventpipelinejava.pipeline.RetryPolicy;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.nio.file.Path;

/**
 * 管道组件的 Spring 装配。
 */
@Configuration
public class PipelineConfig {

    @Bean
    public EventStore eventStore(@Value("${pipeline.journal-file:./data/event-journal.jsonl}") String journalFile) {
        return new InProcessEventStore(Path.of(journalFile));
    }

    @Bean
    public RetryPolicy retryPolicy(@Value("${pipeline.retry.max-attempts:3}") int maxAttempts,
                                   @Value("${pipeline.retry.initial-backoff-ms:100}") long initialBackoffMs,
                                   @Value("${pipeline.retry.multiplier:2.0}") double multiplier,
                                   @Value("${pipeline.retry.max-backoff-ms:5000}") long maxBackoffMs) {
        return new RetryPolicy(maxAttempts, initialBackoffMs, multiplier, maxBackoffMs);
    }

    @Bean
    public EventHandler loggingEventHandler() {
        return event -> { /* 默认处理器：仅打印，由业务方替换 */ };
    }

    @Bean
    public EventDispatcher eventDispatcher(EventStore store, EventHandler handler, RetryPolicy retryPolicy,
                                           @Value("${pipeline.claim-timeout-ms:30000}") long claimTimeoutMs) {
        return new EventDispatcher(store, handler, retryPolicy, claimTimeoutMs);
    }
}
