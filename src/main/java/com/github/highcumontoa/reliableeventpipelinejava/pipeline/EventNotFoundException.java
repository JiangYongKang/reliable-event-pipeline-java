package com.github.highcumontoa.reliableeventpipelinejava.pipeline;

/** 事件不存在。 */
public class EventNotFoundException extends RuntimeException {
    public EventNotFoundException(String message) {
        super(message);
    }
}
