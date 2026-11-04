package com.myraft.api.dto;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public class StatusResponse {
    private String nodeId;
    private String role;
    private int currentTerm;
    private String leaderId;
    private List<String> peers;
    private boolean isLeader;
    private String uptime;

    public StatusResponse() {
    }

    public StatusResponse(String nodeId, String role, int currentTerm, String leaderId, List<String> peers, boolean isLeader, String uptime) {
        this.nodeId = nodeId;
        this.role = role;
        this.currentTerm = currentTerm;
        this.leaderId = leaderId;
        this.peers = peers;
        this.isLeader = isLeader;
        this.uptime = uptime;
    }

    public String getNodeId() {
        return nodeId;
    }

    public void setNodeId(String nodeId) {
        this.nodeId = nodeId;
    }

    public String getRole() {
        return role;
    }

    public void setRole(String role) {
        this.role = role;
    }

    public int getCurrentTerm() {
        return currentTerm;
    }

    public void setCurrentTerm(int currentTerm) {
        this.currentTerm = currentTerm;
    }

    public String getLeaderId() {
        return leaderId;
    }

    public void setLeaderId(String leaderId) {
        this.leaderId = leaderId;
    }

    public List<String> getPeers() {
        return peers;
    }

    public void setPeers(List<String> peers) {
        this.peers = peers;
    }

    @JsonProperty("isLeader")
    public boolean isLeader() {
        return isLeader;
    }

    @JsonProperty("isLeader")
    public void setLeader(boolean leader) {
        isLeader = leader;
    }

    public String getUptime() {
        return uptime;
    }

    public void setUptime(String uptime) {
        this.uptime = uptime;
    }
}
