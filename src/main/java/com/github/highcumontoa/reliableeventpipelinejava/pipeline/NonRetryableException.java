package com.github.highcumontoa.reliableeventpipelinejava.pipeline;

/**
 * 不可重试故障：事件将直接进入死信，不再重试。
 */
public class NonRetryableException extends RuntimeException {
    public NonRetryableException(String message) {
        super(message);
    }
}
