package com.myraft.raft.storage;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

@Component
@ConditionalOnProperty(name = "raft.storage.type", havingValue = "memory")
public class InMemoryRaftStorage implements RaftStorage {
    private volatile PersistedState state = PersistedState.EMPTY;

    @Override
    public synchronized PersistedState load() {
        return state;
    }

    @Override
    public synchronized void persist(int term, String votedFor) {
        this.state = new PersistedState(term, votedFor);
    }
}
