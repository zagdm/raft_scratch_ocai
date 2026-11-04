package com.myraft.raft.storage;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/**
 * Часть состояния Raft, которая обязана переживать перезапуск узла.
 * По спецификации Raft это currentTerm и votedFor (для каждого узла).
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record PersistedState(int term, String votedFor) {
    public static final PersistedState EMPTY = new PersistedState(0, null);
}
