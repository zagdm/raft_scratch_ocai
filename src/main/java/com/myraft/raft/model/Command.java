package com.myraft.raft.model;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

/**
 * Команда, применяемая к state machine. Полиморфизм нужен для сохранения лога
 * в том же персистентном состоянии, что и term/votedFor.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
@JsonSubTypes({
        @JsonSubTypes.Type(value = PutCommand.class, name = "PUT"),
        @JsonSubTypes.Type(value = NoOpCommand.class, name = "NOOP")
})
public sealed interface Command permits PutCommand, NoOpCommand {
}
