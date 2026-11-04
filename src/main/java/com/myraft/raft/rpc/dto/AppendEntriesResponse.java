package com.myraft.raft.rpc.dto;

public class AppendEntriesResponse {
    private int term;
    private boolean success;

    public AppendEntriesResponse() {
    }

    public AppendEntriesResponse(int term, boolean success) {
        this.term = term;
        this.success = success;
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
}
