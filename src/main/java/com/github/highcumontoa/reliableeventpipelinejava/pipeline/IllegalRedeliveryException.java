package com.github.highcumontoa.reliableeventpipelinejava.pipeline;

/** 死信重投时状态非法（事件不处于 DEAD 状态）。 */
public class IllegalRedeliveryException extends RuntimeException {
    public IllegalRedeliveryException(String message) {
        super(message);
    }
}
