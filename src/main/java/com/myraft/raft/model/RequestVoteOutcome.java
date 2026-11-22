package com.myraft.raft.model;

/**
 * Результат обработки входящего {@code RequestVote} узлом: решение и переход состояния.
 */
public record RequestVoteOutcome(RequestVoteEvent event, StateTransition state) {

    public boolean granted() {
        return event == RequestVoteEvent.GRANTED
                || event == RequestVoteEvent.GRANTED_REPEAT
                || event == RequestVoteEvent.GRANTED_TERM_ADVANCED;
    }
}
