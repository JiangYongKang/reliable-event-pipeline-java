package com.github.highcumontoa.reliableeventpipelinejava.event;

/**
 * 业务写入 + 事件入队原子提交的结果。
 */
public enum CommitOutcome {
    /** 本次提交生效。 */
    COMMITTED,
    /** 相同操作键已提交过，本次被幂等忽略（不产生重复事件）。 */
    DUPLICATE_IGNORED
}
