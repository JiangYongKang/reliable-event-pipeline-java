package com.github.highcumontoa.reliableeventpipelinejava.pipeline;

/**
 * 可配置的重试退避策略：指数退避 + 上限。
 */
public class RetryPolicy {
    private final int maxAttempts;
    private final long initialBackoffMs;
    private final double multiplier;
    private final long maxBackoffMs;

    public RetryPolicy(int maxAttempts, long initialBackoffMs, double multiplier, long maxBackoffMs) {
        if (maxAttempts < 1) {
            throw new IllegalArgumentException("maxAttempts must be >= 1");
        }
        this.maxAttempts = maxAttempts;
        this.initialBackoffMs = initialBackoffMs;
        this.multiplier = multiplier;
        this.maxBackoffMs = maxBackoffMs;
    }

    public int maxAttempts() { return maxAttempts; }

    /** 第 attempt 次失败后的退避毫秒数（attempt 从 1 开始）。 */
    public long backoffMs(int attempt) {
        double backoff = initialBackoffMs * Math.pow(multiplier, Math.max(0, attempt - 1));
        return Math.min(maxBackoffMs, (long) backoff);
    }
}
