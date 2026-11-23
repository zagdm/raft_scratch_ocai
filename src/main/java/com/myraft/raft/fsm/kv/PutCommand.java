package com.myraft.raft.fsm.kv;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.NullNode;
import com.myraft.raft.fsm.Command;

/**
 * Команда записи значения в KV state machine (первая конкретизация StateMachine).
 */
public record PutCommand(String key, JsonNode value) implements Command {

    public PutCommand {
        if (key == null || key.isBlank()) {
            throw new IllegalArgumentException("key must not be blank");
        }
        // JSON null допустим; в JsonNode он представлен NullNode, а не Java null.
        value = value == null ? NullNode.instance : value;
    }
}
