package com.myraft.api;

/** Узел не лидер: клиент должен повторить запрос на указанном лидере. */
public class NotLeaderException extends RuntimeException {
    private final String leaderId;

    public NotLeaderException(String leaderId) {
        super("node is not leader (leaderId=" + leaderId + ")");
        this.leaderId = leaderId;
    }

    public String getLeaderId() {
        return leaderId;
    }
}
