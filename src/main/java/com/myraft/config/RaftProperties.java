package com.myraft.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "raft")
public class RaftProperties {
    private String nodeId = "node-1";
    private int electionTimeoutMinMs = 150;
    private int electionTimeoutMaxMs = 300;
    private int heartbeatIntervalMs = 50;
    private String storagePath = "data/raft-state.json";

    public String getNodeId() {
        return nodeId;
    }

    public void setNodeId(String nodeId) {
        this.nodeId = nodeId;
    }

    public String getStoragePath() {
        return storagePath;
    }

    public void setStoragePath(String storagePath) {
        this.storagePath = storagePath;
    }

    public int getElectionTimeoutMinMs() {
        return electionTimeoutMinMs;
    }

    public void setElectionTimeoutMinMs(int electionTimeoutMinMs) {
        this.electionTimeoutMinMs = electionTimeoutMinMs;
    }

    public int getElectionTimeoutMaxMs() {
        return electionTimeoutMaxMs;
    }

    public void setElectionTimeoutMaxMs(int electionTimeoutMaxMs) {
        this.electionTimeoutMaxMs = electionTimeoutMaxMs;
    }

    public int getHeartbeatIntervalMs() {
        return heartbeatIntervalMs;
    }

    public void setHeartbeatIntervalMs(int heartbeatIntervalMs) {
        this.heartbeatIntervalMs = heartbeatIntervalMs;
    }
}
