package com.github.highcumontoa.reliableeventpipelinejava.web;

import com.github.highcumontoa.reliableeventpipelinejava.core.PipelineEvent;
import com.github.highcumontoa.reliableeventpipelinejava.core.PipelineStats;
import com.github.highcumontoa.reliableeventpipelinejava.core.BacklogView;
import com.github.highcumontoa.reliableeventpipelinejava.pipeline.EventPipeline;
import java.time.Instant;
import java.util.List;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;
import com.github.highcumontoa.reliableeventpipelinejava.pipeline.BacklogLimitExceededException;

/** 本地验证用 REST 接口。 */
@RestController
@RequestMapping("/api/pipeline")
public class PipelineController {

    private final EventPipeline pipeline;

    public PipelineController(EventPipeline pipeline) {
        this.pipeline = pipeline;
    }

    public record SubmitRequest(String idempotencyKey, String aggregateId,
            String businessData, String payload, Instant notBefore) {
    }

    public record BacklogRejectedResponse(String error, String aggregateId,
            int currentBacklog, int limit) {
    }

    /** 积压触顶拒绝：返回 429 与可区分的错误码，不静默丢弃。 */
    @ExceptionHandler(BacklogLimitExceededException.class)
    @ResponseStatus(HttpStatus.TOO_MANY_REQUESTS)
    public BacklogRejectedResponse handleBacklogLimit(BacklogLimitExceededException ex) {
        return new BacklogRejectedResponse("BACKLOG_LIMIT_EXCEEDED",
                ex.aggregateId(), ex.currentBacklog(), ex.limit());
    }

    @PostMapping("/events")
    public PipelineEvent submit(@RequestBody SubmitRequest request) {
        if (request.notBefore() != null) {
            return pipeline.submitDelayed(request.idempotencyKey(), request.aggregateId(),
                    request.businessData(), request.payload(), request.notBefore());
        }
        return pipeline.submit(request.idempotencyKey(), request.aggregateId(),
                request.businessData(), request.payload());
    }

    /** 暂停某聚合的投递。 */
    @PostMapping("/aggregates/{aggregateId}/pause")
    public String pause(@PathVariable String aggregateId) {
        pipeline.pauseAggregate(aggregateId);
        return "PAUSED:" + aggregateId;
    }

    /** 恢复某聚合的投递。 */
    @PostMapping("/aggregates/{aggregateId}/resume")
    public String resume(@PathVariable String aggregateId) {
        pipeline.resumeAggregate(aggregateId);
        return "RESUMED:" + aggregateId;
    }

    @GetMapping("/aggregates/{aggregateId}/paused")
    public boolean paused(@PathVariable String aggregateId) {
        return pipeline.isAggregatePaused(aggregateId);
    }

    /** 积压水位：全局与各聚合的未完成数量、最久积压时长。 */
    @GetMapping("/backlog")
    public BacklogView backlog() {
        return pipeline.backlog();
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
