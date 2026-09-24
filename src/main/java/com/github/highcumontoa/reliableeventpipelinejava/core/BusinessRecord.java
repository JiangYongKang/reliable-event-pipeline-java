package com.github.highcumontoa.reliableeventpipelinejava.core;

import java.time.Instant;

/** 业务记录，与事件在同一事务中原子提交。 */
public class BusinessRecord {
    public String idempotencyKey;
    public String aggregateId;
    public String data;
    public Instant createdAt;

    public BusinessRecord() {
    }
}
