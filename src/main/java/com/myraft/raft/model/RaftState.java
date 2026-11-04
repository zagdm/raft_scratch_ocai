package com.myraft.raft.model;

import com.myraft.raft.storage.PersistedState;
import com.myraft.raft.storage.RaftStorage;
import org.springframework.stereotype.Component;

import java.util.concurrent.locks.ReentrantReadWriteLock;

@Component
public class RaftState {
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private final RaftStorage storage;

    private volatile int currentTerm;
    private volatile String votedFor;
    private volatile NodeRole role = NodeRole.FOLLOWER;
    private volatile String leaderId = null;
    private final long startTime = System.currentTimeMillis();

    public RaftState(RaftStorage storage) {
        this.storage = storage;
        PersistedState persisted = storage.load();
        this.currentTerm = persisted.term();
        this.votedFor = persisted.votedFor();
    }

    public int getCurrentTerm() {
        lock.readLock().lock();
        try {
            return currentTerm;
        } finally {
            lock.readLock().unlock();
        }
    }

    public String getVotedFor() {
        lock.readLock().lock();
        try {
            return votedFor;
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Единственный способ изменить {@code currentTerm}/{@code votedFor}.
     * <p>
     * Сначала состояние фиксируется в хранилище, и только потом становится видимым.
     * Если запись не удалась, состояние не меняется и исключение уходит наверх:
     * узел не должен ни голосовать, ни отвечать, пока состояние не на диске.
     */
    public void updateTermAndVote(int term, String votedFor) {
        lock.writeLock().lock();
        try {
            storage.persist(term, votedFor);
            this.currentTerm = term;
            this.votedFor = votedFor;
        } finally {
            lock.writeLock().unlock();
        }
    }

    public NodeRole getRole() {
        lock.readLock().lock();
        try {
            return role;
        } finally {
            lock.readLock().unlock();
        }
    }

    public void setRole(NodeRole role) {
        lock.writeLock().lock();
        try {
            this.role = role;
        } finally {
            lock.writeLock().unlock();
        }
    }

    public String getLeaderId() {
        lock.readLock().lock();
        try {
            return leaderId;
        } finally {
            lock.readLock().unlock();
        }
    }

    public void setLeaderId(String leaderId) {
        lock.writeLock().lock();
        try {
            this.leaderId = leaderId;
        } finally {
            lock.writeLock().unlock();
        }
    }

    public long getStartTime() {
        return startTime;
    }

    public ReentrantReadWriteLock getLock() {
        return lock;
    }
}
