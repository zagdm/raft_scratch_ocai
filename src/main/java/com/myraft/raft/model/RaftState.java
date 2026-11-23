package com.myraft.raft.model;

import com.myraft.raft.storage.PersistedState;
import com.myraft.raft.storage.RaftStorage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.concurrent.locks.ReentrantReadWriteLock;

@Component
public class RaftState {
    private static final Logger log = LoggerFactory.getLogger(RaftState.class);

    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private final RaftStorage storage;

    private volatile int currentTerm;
    private volatile String votedFor;
    private volatile NodeRole role = NodeRole.FOLLOWER;
    private volatile String leaderId = null;
    private volatile RaftLog raftLog;
    // Volatile по Figure 2: не персистируются и не восстанавливаются при перезапуске.
    private volatile long commitIndex;
    private volatile long lastApplied;
    private final long startTime = System.currentTimeMillis();

    public RaftState(RaftStorage storage) {
        this.storage = storage;
        PersistedState persisted = storage.load();
        this.currentTerm = persisted.term();
        this.votedFor = persisted.votedFor();
        this.raftLog = new RaftLog(persisted.log());
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

    public record GrantVoteResult(boolean granted, boolean termIncreased, int term) {}

    /**
     * Согласованный снимок состояния узла: все поля читаются под одним read-lock,
     * поэтому набор значений относится к одному моменту времени.
     */
    public record RaftStateSnapshot(int currentTerm, String votedFor, NodeRole role, String leaderId,
                                    long startTime, long lastLogIndex, int lastLogTerm) {}

    public RaftStateSnapshot snapshot() {
        lock.readLock().lock();
        try {
            return new RaftStateSnapshot(currentTerm, votedFor, role, leaderId, startTime,
                    raftLog.lastIndex(), raftLog.lastTerm());
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Фиксирует term, votedFor и лог одной атомарной записью до публикации изменения.
     */
    private void persistLocked(int term, String votedFor, RaftLog nextLog) {
        try {
            storage.persist(PersistedState.of(term, votedFor, nextLog.entries()));
        } catch (RuntimeException e) {
            log.error("log persist failed term={} votedFor={} logSize={}",
                    term, votedFor, nextLog.lastIndex(), e);
            throw e;
        }
    }

    /**
     * Атомарно начинает выборы: инкрементирует терм и голосует за себя.
     * Сначала состояние фиксируется в хранилище, и только потом становится видимым.
     */
    public int startElection(String selfId) {
        lock.writeLock().lock();
        try {
            int newTerm = currentTerm + 1;
            persistLocked(newTerm, selfId, raftLog);
            this.currentTerm = newTerm;
            this.votedFor = selfId;
            return newTerm;
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Атомарно решает, можно ли выдать голос кандидату, с соблюдением монотонности терма.
     */
    public GrantVoteResult grantVote(int candidateTerm, String candidateId) {
        lock.writeLock().lock();
        try {
            if (candidateTerm < currentTerm) {
                return new GrantVoteResult(false, false, currentTerm);
            }
            if (candidateTerm > currentTerm) {
                persistLocked(candidateTerm, candidateId, raftLog);
                this.currentTerm = candidateTerm;
                this.votedFor = candidateId;
                return new GrantVoteResult(true, true, currentTerm);
            }
            if (votedFor == null || votedFor.equals(candidateId)) {
                persistLocked(currentTerm, candidateId, raftLog);
                this.votedFor = candidateId;
                return new GrantVoteResult(true, false, currentTerm);
            }
            return new GrantVoteResult(false, false, currentTerm);
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Единственный способ изменить {@code currentTerm}/{@code votedFor}.
     * <p>
     * Сначала состояние фиксируется в хранилище, и только потом становится видимым.
     * Если запись не удалась, состояние не меняется и исключение уходит наверх:
     * узел не должен ни голосовать, ни отвечать, пока состояние не на диске.
     * Монотонно: терм меньше текущего игнорируется (no-op).
     */
    public void updateTermAndVote(int term, String votedFor) {
        lock.writeLock().lock();
        try {
            if (term < currentTerm) {
                return;
            }
            persistLocked(term, votedFor, raftLog);
            this.currentTerm = term;
            this.votedFor = votedFor;
        } finally {
            lock.writeLock().unlock();
        }
    }

    /** Добавляет запись в лог; persist выполняется до публикации изменения. */
    public void appendLogEntry(LogEntry entry) {
        lock.writeLock().lock();
        try {
            RaftLog next = raftLog.append(entry);
            persistLocked(currentTerm, votedFor, next);
            raftLog = next;
        } finally {
            lock.writeLock().unlock();
        }
    }

    /** Удаляет запись с индексом {@code index} и все последующие. */
    public void truncateLogFrom(long index) {
        lock.writeLock().lock();
        try {
            RaftLog next = raftLog.truncateFrom(index);
            if (next == raftLog) {
                return;
            }
            persistLocked(currentTerm, votedFor, next);
            raftLog = next;
        } finally {
            lock.writeLock().unlock();
        }
    }

    public long lastLogIndex() {
        lock.readLock().lock();
        try {
            return raftLog.lastIndex();
        } finally {
            lock.readLock().unlock();
        }
    }

    public int lastLogTerm() {
        lock.readLock().lock();
        try {
            return raftLog.lastTerm();
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Volatile-указатель коммита. Монотонно и не может опережать конец лога
     * (правило §5.3: лидер коммитит только записи, которые у него есть).
     */
    public void advanceCommitIndex(long newCommitIndex) {
        lock.writeLock().lock();
        try {
            if (newCommitIndex > commitIndex) {
                commitIndex = Math.min(newCommitIndex, raftLog.lastIndex());
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    public long getCommitIndex() {
        lock.readLock().lock();
        try {
            return commitIndex;
        } finally {
            lock.readLock().unlock();
        }
    }

    /** Монотонно продвигает указатель применённых записей. */
    public void advanceLastApplied(long newLastApplied) {
        lock.writeLock().lock();
        try {
            if (newLastApplied > lastApplied) {
                lastApplied = newLastApplied;
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    public long getLastApplied() {
        lock.readLock().lock();
        try {
            return lastApplied;
        } finally {
            lock.readLock().unlock();
        }
    }

    /** Неизменяемый снимок лога для чтения под nodeLock без дополнительных локов. */
    public RaftLog logSnapshot() {
        lock.readLock().lock();
        try {
            return raftLog;
        } finally {
            lock.readLock().unlock();
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
}
