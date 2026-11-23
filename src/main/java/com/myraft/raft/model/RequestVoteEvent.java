package com.myraft.raft.model;

/**
 * Решение узла по входящему {@code RequestVote}.
 */
public enum RequestVoteEvent {
    /** Голос выдан впервые в текущем терме. */
    GRANTED,
    /** Повторный голос тому же кандидату в том же терме (идемпотентный повтор). */
    GRANTED_REPEAT,
    /** Голос выдан, терм кандидата выше текущего (узел шагнул вниз). */
    GRANTED_TERM_ADVANCED,
    /** Терм кандидата ниже текущего. */
    REJECTED_STALE_TERM,
    /** В этом терме уже голосовали за другого кандидата. */
    REJECTED_ALREADY_VOTED,
    /** Лог голосующего свежее лога кандидата (§5.4.1 статьи Raft). */
    REJECTED_LOG_NOT_UP_TO_DATE
}
