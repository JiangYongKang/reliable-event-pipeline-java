package com.github.highcumontoa.reliableeventpipelinejava.web;

import com.github.highcumontoa.reliableeventpipelinejava.core.PipelineEvent;
import com.github.highcumontoa.reliableeventpipelinejava.core.PipelineStats;
import com.github.highcumontoa.reliableeventpipelinejava.pipeline.EventPipeline;
import java.util.List;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/** 本地验证用 REST 接口。 */
@RestController
@RequestMapping("/api/pipeline")
public class PipelineController {

    private final EventPipeline pipeline;

    public PipelineController(EventPipeline pipeline) {
        this.pipeline = pipeline;
    }

    public record SubmitRequest(String idempotencyKey, String aggregateId,
            String businessData, String payload) {
    }

    @PostMapping("/events")
    public PipelineEvent submit(@RequestBody SubmitRequest request) {
        return pipeline.submit(request.idempotencyKey(), request.aggregateId(),
                request.businessData(), request.payload());
    }

    @PostMapping("/events/{eventId}/redeliver")
    public PipelineEvent redeliver(@PathVariable String eventId) {
        return pipeline.redeliver(eventId);
    }

    @GetMapping("/events")
    public List<PipelineEvent> listEvents() {
        return pipeline.listEvents();
    }

    @GetMapping("/events/{eventId}")
    public PipelineEvent getEvent(@PathVariable String eventId) {
        return pipeline.findEvent(eventId).orElse(null);
    }

    @GetMapping("/stats")
    public PipelineStats stats() {
        return pipeline.stats();
    }
}
