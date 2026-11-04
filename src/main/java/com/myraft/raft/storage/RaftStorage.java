package com.myraft.raft.storage;

/**
 * Хранилище персистентной части состояния Raft.
 * <p>
 * Контракт: {@link #persist(int, String)} должен завершиться (данные на диске)
 * до того, как узел ответит на RPC или отправит собственный RPC, меняющий терм.
 */
public interface RaftStorage {

    /**
     * Загружает сохранённое состояние. При отсутствии состояния возвращает {@link PersistedState#EMPTY}.
     */
    PersistedState load();

    /**
     * Атомарно сохраняет терм и голос.
     */
    void persist(int term, String votedFor);
}
