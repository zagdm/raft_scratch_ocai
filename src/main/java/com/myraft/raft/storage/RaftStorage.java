package com.myraft.raft.storage;

/**
 * Хранилище персистентной части состояния Raft.
 * <p>
 * Контракт: {@link #persist(PersistedState)} должен завершиться (данные зафиксированы)
 * до того, как узел ответит на RPC или отправит собственный RPC, меняющий терм.
 */
public interface RaftStorage {

    /**
     * Загружает сохранённое состояние. При отсутствии состояния возвращается
     * {@link PersistedState#EMPTY}.
     */
    PersistedState load();

    /**
     * Атомарно сохраняет term, votedFor и лог.
     */
    void persist(PersistedState state);
}
