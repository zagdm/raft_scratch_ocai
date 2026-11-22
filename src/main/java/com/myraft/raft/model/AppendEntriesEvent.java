package com.myraft.raft.model;

/**
 * Решение узла по входящему {@code AppendEntries}.
 */
public enum AppendEntriesEvent {
    /** Терм лидера выше текущего: узел обновил term/vote и принял лидера. */
    ACCEPTED_TERM_ADVANCED,
    /** Лидер обнаружен впервые или сменился (роль/leaderId изменились). */
    ACCEPTED_LEADER_DISCOVERED,
    /** Повторный heartbeat от уже известного лидера: состояние не изменилось. */
    ACCEPTED_IDEMPOTENT,
    /** Терм лидера ниже текущего. */
    REJECTED_STALE_TERM,
    /** Равный терм, но лидер уже известен и это другой узел: heartbeat игнорируется. */
    REJECTED_FOREIGN_LEADER
}
