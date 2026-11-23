package com.myraft.raft.rpc.dto;

import com.myraft.raft.model.LogEntry;

import java.util.ArrayList;
import java.util.List;

public class AppendEntriesRequest {
    private int term;
    private String leaderId;
    private long prevLogIndex = 0;
    private int prevLogTerm = 0;
    private List<LogEntry> entries = new ArrayList<>();
    private long leaderCommit = 0;

    public AppendEntriesRequest() {
    }

    public AppendEntriesRequest(int term, String leaderId) {
        this(term, leaderId, 0, 0, List.of(), 0);
    }

    public AppendEntriesRequest(int term, String leaderId, long prevLogIndex, int prevLogTerm,
                                List<LogEntry> entries, long leaderCommit) {
        this.term = term;
        this.leaderId = leaderId;
        this.prevLogIndex = prevLogIndex;
        this.prevLogTerm = prevLogTerm;
        this.entries = entries == null ? new ArrayList<>() : new ArrayList<>(entries);
        this.leaderCommit = leaderCommit;
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

    public List<LogEntry> getEntries() {
        return entries;
    }

    public void setEntries(List<LogEntry> entries) {
        this.entries = entries == null ? new ArrayList<>() : new ArrayList<>(entries);
    }

    public long getLeaderCommit() {
        return leaderCommit;
    }

    public void setLeaderCommit(long leaderCommit) {
        this.leaderCommit = leaderCommit;
    }
}
