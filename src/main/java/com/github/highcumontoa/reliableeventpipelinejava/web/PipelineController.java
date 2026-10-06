package com.github.highcumontoa.reliableeventpipelinejava.web;

import com.github.highcumontoa.reliableeventpipelinejava.core.BacklogStats;
import com.github.highcumontoa.reliableeventpipelinejava.core.PipelineEvent;
import com.github.highcumontoa.reliableeventpipelinejava.core.PipelineStats;
import com.github.highcumontoa.reliableeventpipelinejava.pipeline.BackpressureRejectedException;
import com.github.highcumontoa.reliableeventpipelinejava.pipeline.EventPipeline;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
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

    /**
     * @param deliverAt 可选，ISO-8601 时间；事件在该时间之前不可被认领
     */
    public record SubmitRequest(String idempotencyKey, String aggregateId,
            String businessData, String payload, Instant deliverAt) {
    }

    @PostMapping("/events")
    public PipelineEvent submit(@RequestBody SubmitRequest request) {
        return pipeline.submit(request.idempotencyKey(), request.aggregateId(),
                request.businessData(), request.payload(), request.deliverAt());
    }

    @PostMapping("/events/{eventId}/redeliver")
    public PipelineEvent redeliver(@PathVariable String eventId) {
        return pipeline.redeliver(eventId);
    }

    @PostMapping("/aggregates/{aggregateId}/pause")
    public Map<String, Object> pause(@PathVariable String aggregateId) {
        pipeline.pauseAggregate(aggregateId);
        return Map.of("aggregateId", aggregateId, "paused", true);
    }

    @PostMapping("/aggregates/{aggregateId}/resume")
    public Map<String, Object> resume(@PathVariable String aggregateId) {
        pipeline.resumeAggregate(aggregateId);
        return Map.of("aggregateId", aggregateId, "paused", false);
    }

    @GetMapping("/backlog")
    public BacklogStats backlog() {
        return pipeline.backlogStats();
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

    /** 背压拒绝：返回 429 与可区分的错误体，调用方可据此重试或降级。 */
    @ExceptionHandler(BackpressureRejectedException.class)
    public ResponseEntity<Map<String, Object>> backpressure(BackpressureRejectedException e) {
        return ResponseEntity.status(HttpStatus.TOO_MANY_REQUESTS).body(Map.of(
                "error", "BACKPRESSURE_REJECTED",
                "aggregateId", e.getAggregateId(),
                "currentBacklog", e.getCurrentBacklog(),
                "maxBacklog", e.getMaxBacklog(),
                "message", e.getMessage()));
    }
}
