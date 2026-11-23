package com.myraft.raft.storage;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.myraft.raft.model.LogEntry;

import java.util.List;

/**
 * Персистентная часть состояния Raft: версия формата, term, votedFor и лог.
 * По спецификации Raft term/vote/log обязаны переживать перезапуск узла.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record PersistedState(int formatVersion, int term, String votedFor, List<LogEntry> log) {

    /** Текущий формат: RaftCommand-envelope. Старые файлы не читаются (fail-fast). */
    public static final int CURRENT_FORMAT_VERSION = 2;

    public static final PersistedState EMPTY = of(0, null, List.of());

    public PersistedState {
        log = log == null ? List.of() : List.copyOf(log);
    }

    public static PersistedState of(int term, String votedFor, List<LogEntry> log) {
        return new PersistedState(CURRENT_FORMAT_VERSION, term, votedFor, log);
    }

    public int logSize() {
        return log.size();
    }
}
