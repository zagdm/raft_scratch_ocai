package com.myraft.raft.model;

/**
 * Результат обработки входящего {@code AppendEntries} узлом: решение и переход состояния.
 */
public record AppendEntriesOutcome(AppendEntriesEvent event, StateTransition state) {

    public boolean accepted() {
        return event == AppendEntriesEvent.ACCEPTED_TERM_ADVANCED
                || event == AppendEntriesEvent.ACCEPTED_LEADER_DISCOVERED
                || event == AppendEntriesEvent.ACCEPTED_IDEMPOTENT;
    }
}
