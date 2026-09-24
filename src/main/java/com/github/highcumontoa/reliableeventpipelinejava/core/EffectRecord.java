package com.github.highcumontoa.reliableeventpipelinejava.core;

import java.time.Instant;

/** 副作用账本记录：保证同一事件的业务副作用只生效一次。 */
public class EffectRecord {
    public String eventId;
    public EffectState state = EffectState.NONE;
    public String leaseOwner;
    public Instant leaseExpiresAt;
    public Instant completedAt;

    public EffectRecord() {
    }
}
