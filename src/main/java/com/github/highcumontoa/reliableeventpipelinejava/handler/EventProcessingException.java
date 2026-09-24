package com.github.highcumontoa.reliableeventpipelinejava.handler;

import com.github.highcumontoa.reliableeventpipelinejava.core.FailureKind;

/** 处理器抛出的失败，携带失败分类以决定重试或死信。 */
public class EventProcessingException extends RuntimeException {
    private final FailureKind kind;

    public EventProcessingException(FailureKind kind, String message) {
        super(message);
        this.kind = kind;
    }

    public EventProcessingException(FailureKind kind, String message, Throwable cause) {
        super(message, cause);
        this.kind = kind;
    }

    public FailureKind kind() {
        return kind;
    }

    public static EventProcessingException retryable(String message) {
        return new EventProcessingException(FailureKind.RETRYABLE, message);
    }

    public static EventProcessingException nonRetryable(String message) {
        return new EventProcessingException(FailureKind.NON_RETRYABLE, message);
    }
}
