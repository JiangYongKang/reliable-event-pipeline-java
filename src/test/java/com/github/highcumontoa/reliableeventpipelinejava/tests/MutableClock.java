package com.github.highcumontoa.reliableeventpipelinejava.tests;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;

/** 测试用可变时钟。 */
class MutableClock extends Clock {
    private long millis;

    MutableClock(long initialMillis) {
        this.millis = initialMillis;
    }

    void advance(long deltaMs) {
        millis += deltaMs;
    }

    @Override
    public long millis() {
        return millis;
    }

    @Override
    public Instant instant() {
        return Instant.ofEpochMilli(millis);
    }

    @Override
    public ZoneId getZone() {
        return ZoneId.of("UTC");
    }

    @Override
    public Clock withZone(ZoneId zone) {
        return this;
    }
}
