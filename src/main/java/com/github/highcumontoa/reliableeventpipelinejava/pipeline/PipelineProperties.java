package com.github.highcumontoa.reliableeventpipelinejava.pipeline;

import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;

/** 管道可配置参数。 */
@ConfigurationProperties(prefix = "pipeline")
public class PipelineProperties {
    /** 最大投递尝试次数（含首次），超过后进入死信。 */
    private int maxDeliveryAttempts = 3;
    /** 退避基数：第 n 次重试延迟 = backoffBase * 2^(n-1)。 */
    private Duration backoffBase = Duration.ofMillis(100);
    /** 单次认领租约时长，超时未确认可被重新认领。 */
    private Duration claimTimeout = Duration.ofSeconds(5);
    /** 副作用执行租约时长。 */
    private Duration effectLeaseTimeout = Duration.ofSeconds(10);
    /** 派发工作线程数。 */
    private int workerThreads = 4;
    /** 派发轮询间隔。 */
    private Duration pollInterval = Duration.ofMillis(20);
    /** 延迟事件等待时长上限：自提交时刻起超过该值仍未能投递的延迟事件进入死信（DELAY_LIMIT_EXCEEDED）。0 表示不限制。 */
    private Duration maxDelay = Duration.ofHours(24);
    /** 单聚合未完成（PENDING/IN_FLIGHT）事件数上限：超过后新提交被拒绝。0 表示不限制。 */
    private int maxBacklogPerAggregate = 0;
    /** 状态文件路径（进程内可恢复存储）。 */
    private String storeFile = "data/pipeline-store.json";

    public int getMaxDeliveryAttempts() {
        return maxDeliveryAttempts;
    }

    public void setMaxDeliveryAttempts(int maxDeliveryAttempts) {
        this.maxDeliveryAttempts = maxDeliveryAttempts;
    }

    public Duration getBackoffBase() {
        return backoffBase;
    }

    public void setBackoffBase(Duration backoffBase) {
        this.backoffBase = backoffBase;
    }

    public Duration getClaimTimeout() {
        return claimTimeout;
    }

    public void setClaimTimeout(Duration claimTimeout) {
        this.claimTimeout = claimTimeout;
    }

    public Duration getEffectLeaseTimeout() {
        return effectLeaseTimeout;
    }

    public void setEffectLeaseTimeout(Duration effectLeaseTimeout) {
        this.effectLeaseTimeout = effectLeaseTimeout;
    }

    public int getWorkerThreads() {
        return workerThreads;
    }

    public void setWorkerThreads(int workerThreads) {
        this.workerThreads = workerThreads;
    }

    public Duration getPollInterval() {
        return pollInterval;
    }

    public void setPollInterval(Duration pollInterval) {
        this.pollInterval = pollInterval;
    }

    public String getStoreFile() {
        return storeFile;
    }

    public void setStoreFile(String storeFile) {
        this.storeFile = storeFile;
    }

    public Duration getMaxDelay() {
        return maxDelay;
    }

    public void setMaxDelay(Duration maxDelay) {
        this.maxDelay = maxDelay;
    }

    public int getMaxBacklogPerAggregate() {
        return maxBacklogPerAggregate;
    }

    public void setMaxBacklogPerAggregate(int maxBacklogPerAggregate) {
        this.maxBacklogPerAggregate = maxBacklogPerAggregate;
    }
}
