package com.myraft.raft.rpc.dto;

public class RequestVoteRequest {
    private int term;
    private String candidateId;
    private long lastLogIndex = 0;
    private int lastLogTerm = 0;

    public RequestVoteRequest() {
    }

    public RequestVoteRequest(int term, String candidateId) {
        this.term = term;
        this.candidateId = candidateId;
        this.lastLogIndex = 0;
        this.lastLogTerm = 0;
    }

    public int getTerm() {
        return term;
    }

    public void setTerm(int term) {
        this.term = term;
    }

    public String getCandidateId() {
        return candidateId;
    }

    public void setCandidateId(String candidateId) {
        this.candidateId = candidateId;
    }

    public long getLastLogIndex() {
        return lastLogIndex;
    }

    public void setLastLogIndex(long lastLogIndex) {
        this.lastLogIndex = lastLogIndex;
    }

    public int getLastLogTerm() {
        return lastLogTerm;
    }

    public void setLastLogTerm(int lastLogTerm) {
        this.lastLogTerm = lastLogTerm;
    }
}
