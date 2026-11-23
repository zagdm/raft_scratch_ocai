package com.myraft.raft.model;

import java.util.Objects;

/**
 * Запись реплицируемого лога. Индексы начинаются с 1 (как в Figure 2 статьи Raft).
 */
public record LogEntry(long index, int term, RaftCommand command) {

    public LogEntry {
        if (index < 1) {
            throw new IllegalArgumentException("log index must be >= 1");
        }
        if (term < 0) {
            throw new IllegalArgumentException("log term must be >= 0");
        }
        Objects.requireNonNull(command, "command");
    }
}
