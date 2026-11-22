package com.myraft.raft.model;

/**
 * Снапшоты состояния узла до и после одного перехода.
 * <p>
 * Собираются под локом узла, поэтому описывают именно этот переход и позволяют
 * логировать решение без повторного чтения живого {@link RaftState}.
 */
public record StateTransition(RaftState.RaftStateSnapshot before, RaftState.RaftStateSnapshot after) {
}
