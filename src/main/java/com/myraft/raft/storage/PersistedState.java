package com.myraft.raft.storage;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.myraft.raft.model.LogEntry;

import java.util.List;

/**
 * Персистентная часть состояния Raft: term, votedFor и лог.
 * По спецификации Raft все три компонента обязаны переживать перезапуск узла.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record PersistedState(int term, String votedFor, List<LogEntry> log) {

    public static final PersistedState EMPTY = new PersistedState(0, null, List.of());

    public PersistedState {
        // Старые файлы содержали только term/votedFor — отсутствующий лог считаем пустым.
        log = log == null ? List.of() : List.copyOf(log);
    }

    public int logSize() {
        return log.size();
    }
}
