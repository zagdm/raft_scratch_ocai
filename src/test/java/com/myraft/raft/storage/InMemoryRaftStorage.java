package com.myraft.raft.storage;

/**
 * Хранилище в памяти для тестов.
 */
public class InMemoryRaftStorage implements RaftStorage {
    private PersistedState state = PersistedState.EMPTY;

    @Override
    public PersistedState load() {
        return state;
    }

    @Override
    public void persist(int term, String votedFor) {
        state = new PersistedState(term, votedFor);
    }
}
