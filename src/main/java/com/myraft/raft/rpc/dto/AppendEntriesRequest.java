package com.myraft.raft.rpc.dto;

import java.util.ArrayList;
import java.util.List;

public class AppendEntriesRequest {
    private int term;
    private String leaderId;
    private long prevLogIndex = 0;
    private int prevLogTerm = 0;
    private List<Object> entries = new ArrayList<>();
    private long leaderCommit = 0;

    public AppendEntriesRequest() {
    }

    public AppendEntriesRequest(int term, String leaderId) {
        this.term = term;
        this.leaderId = leaderId;
        this.entries = new ArrayList<>();
    }

    public int getTerm() {
        return term;
    }

    public void setTerm(int term) {
        this.term = term;
    }

    public String getLeaderId() {
        return leaderId;
    }

    public void setLeaderId(String leaderId) {
        this.leaderId = leaderId;
    }

    public long getPrevLogIndex() {
        return prevLogIndex;
    }

    public void setPrevLogIndex(long prevLogIndex) {
        this.prevLogIndex = prevLogIndex;
    }

    public int getPrevLogTerm() {
        return prevLogTerm;
    }

    public void setPrevLogTerm(int prevLogTerm) {
        this.prevLogTerm = prevLogTerm;
    }

    public List<Object> getEntries() {
        return entries;
    }

    public void setEntries(List<Object> entries) {
        this.entries = entries;
    }

    public long getLeaderCommit() {
        return leaderCommit;
    }

    public void setLeaderCommit(long leaderCommit) {
        this.leaderCommit = leaderCommit;
    }
}
