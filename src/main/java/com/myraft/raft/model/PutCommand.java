package com.myraft.raft.model;

import com.fasterxml.jackson.databind.JsonNode;

/**
 * Пользовательская команда записи значения в KV state machine.
 */
public record PutCommand(String key, JsonNode value) implements Command {

    public PutCommand {
        if (key == null || key.isBlank()) {
            throw new IllegalArgumentException("key must not be blank");
        }
    }
}
