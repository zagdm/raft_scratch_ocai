package com.myraft.raft.model;

/**
 * Результат обработки входящего {@code AppendEntries} узлом: решение,
 * переход состояния и детали изменения лога для ответа/логирования.
 */
public record AppendEntriesOutcome(AppendEntriesEvent event, StateTransition state, Details details) {

    /** Детали репликации, из которых адаптер строит ответ и логи. */
    public record Details(int appended, boolean truncated, long lastNewIndex,
                          int conflictTerm, long conflictIndex) {
        public static final Details EMPTY = new Details(0, false, 0, 0, 0);
    }

    public AppendEntriesOutcome(AppendEntriesEvent event, StateTransition state) {
        this(event, state, Details.EMPTY);
    }

    public boolean accepted() {
        return event == AppendEntriesEvent.ACCEPTED_TERM_ADVANCED
                || event == AppendEntriesEvent.ACCEPTED_LEADER_DISCOVERED
                || event == AppendEntriesEvent.ACCEPTED_IDEMPOTENT
                || event == AppendEntriesEvent.ACCEPTED_LOG_APPENDED
                || event == AppendEntriesEvent.ACCEPTED_LOG_TRUNCATED;
    }
}
