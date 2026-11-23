package com.myraft.raft.rpc.dto;

public class AppendEntriesResponse {
    private int term;
    private boolean success;
    private long lastIndex = 0;
    private int conflictTerm = 0;
    private long conflictIndex = 0;

    public AppendEntriesResponse() {
    }

    public AppendEntriesResponse(int term, boolean success) {
        this(term, success, 0, 0, 0);
    }

    public AppendEntriesResponse(int term, boolean success, long lastIndex,
                                 int conflictTerm, long conflictIndex) {
        this.term = term;
        this.success = success;
        this.lastIndex = lastIndex;
        this.conflictTerm = conflictTerm;
        this.conflictIndex = conflictIndex;
    }

    public int getTerm() {
        return term;
    }

    public void setTerm(int term) {
        this.term = term;
    }

    public boolean isSuccess() {
        return success;
    }

    public void setSuccess(boolean success) {
        this.success = success;
    }

    public long getLastIndex() {
        return lastIndex;
    }

    public void setLastIndex(long lastIndex) {
        this.lastIndex = lastIndex;
    }

    public int getConflictTerm() {
        return conflictTerm;
    }

    public void setConflictTerm(int conflictTerm) {
        this.conflictTerm = conflictTerm;
    }

    public long getConflictIndex() {
        return conflictIndex;
    }

    public void setConflictIndex(long conflictIndex) {
        this.conflictIndex = conflictIndex;
    }
}
