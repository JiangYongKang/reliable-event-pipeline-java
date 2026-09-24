package com.github.highcumontoa.reliableeventpipelinejava.support;

import com.github.highcumontoa.reliableeventpipelinejava.handler.EventHandler;
import com.github.highcumontoa.reliableeventpipelinejava.pipeline.EventPipeline;
import com.github.highcumontoa.reliableeventpipelinejava.pipeline.PipelineProperties;
import com.github.highcumontoa.reliableeventpipelinejava.store.EventStore;
import java.nio.file.Path;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.SmartLifecycle;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/** 管道装配：存储、管道与生命周期。 */
@Configuration
@EnableConfigurationProperties(PipelineProperties.class)
public class PipelineConfig {

    @Bean
    public EventStore eventStore(PipelineProperties properties) {
        EventStore store = new EventStore(Path.of(properties.getStoreFile()));
        store.load();
        return store;
    }

    @Bean
    public EventPipeline eventPipeline(EventStore store, PipelineProperties properties) {
        return new EventPipeline(store, properties);
    }

    @Bean
    @ConditionalOnMissingBean
    public EventHandler eventHandler() {
        return new LoggingEventHandler();
    }

    /** 随应用启停管道派发线程。 */
    @Bean
    public SmartLifecycle pipelineLifecycle(EventPipeline pipeline, EventHandler handler) {
        return new SmartLifecycle() {
            private volatile boolean running;

            @Override
            public void start() {
                pipeline.start(handler);
                running = true;
            }

            @Override
            public void stop() {
                pipeline.stop();
                running = false;
            }

            @Override
            public boolean isRunning() {
                return running;
            }
        };
    }
}
