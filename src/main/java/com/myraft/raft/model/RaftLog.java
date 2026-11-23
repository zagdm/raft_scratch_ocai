package com.myraft.raft.model;

import java.util.ArrayList;
import java.util.List;

/**
 * Неизменяемое представление лога. Операции {@link #append(LogEntry)} и
 * {@link #truncateFrom(long)} возвращают новое значение — это позволяет фиксировать
 * состояние в хранилище до публикации изменения (persist-before-visible).
 */
public record RaftLog(List<LogEntry> entries) {

    public static final RaftLog EMPTY = new RaftLog(List.of());

    public RaftLog {
        entries = entries == null ? List.of() : List.copyOf(entries);
        for (int i = 0; i < entries.size(); i++) {
            LogEntry entry = entries.get(i);
            if (entry.index() != i + 1L) {
                throw new IllegalArgumentException("log indices must be contiguous starting from 1");
            }
        }
    }

    /** Индекс последней записи; 0 для пустого лога. */
    public long lastIndex() {
        return entries.size();
    }

    /** Терм последней записи; 0 для пустого лога. */
    public int lastTerm() {
        return lastIndex() == 0 ? 0 : entries.get((int) lastIndex() - 1).term();
    }

    /** Терм записи с индексом {@code index}; 0 для индекса 0 и отсутствующих индексов. */
    public int termAt(long index) {
        if (index < 1 || index > lastIndex()) {
            return 0;
        }
        return entries.get((int) index - 1).term();
    }

    public LogEntry entryAt(long index) {
        if (index < 1 || index > lastIndex()) {
            return null;
        }
        return entries.get((int) index - 1);
    }

    public boolean isEmpty() {
        return entries.isEmpty();
    }

    public RaftLog append(LogEntry entry) {
        if (entry.index() != lastIndex() + 1) {
            throw new IllegalArgumentException("expected index " + (lastIndex() + 1)
                    + " but got " + entry.index());
        }
        List<LogEntry> next = new ArrayList<>(entries);
        next.add(entry);
        return new RaftLog(next);
    }

    /** Удаляет запись с индексом {@code index} и все последующие. */
    public RaftLog truncateFrom(long index) {
        if (index <= 1) {
            return EMPTY;
        }
        if (index > lastIndex()) {
            return this;
        }
        return new RaftLog(entries.subList(0, (int) index - 1));
    }

    /**
     * Сравнение «свежести» логов по §5.4.1 статьи Raft: сначала терм последней записи,
     * затем длина.
     */
    public static boolean isAtLeastAsUpToDate(int candidateLastLogTerm, long candidateLastLogIndex,
                                              int localLastLogTerm, long localLastLogIndex) {
        if (candidateLastLogTerm != localLastLogTerm) {
            return candidateLastLogTerm > localLastLogTerm;
        }
        return candidateLastLogIndex >= localLastLogIndex;
    }
}
