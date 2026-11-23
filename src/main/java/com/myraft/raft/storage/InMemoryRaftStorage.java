package com.myraft.raft.storage;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

@Component
@ConditionalOnProperty(name = "raft.storage.type", havingValue = "memory", matchIfMissing = true)
public class InMemoryRaftStorage implements RaftStorage {
    private volatile PersistedState state = PersistedState.EMPTY;

    @Override
    public synchronized PersistedState load() {
        return state;
    }

    @Override
    public synchronized void persist(PersistedState state) {
        this.state = state;
    }
}
