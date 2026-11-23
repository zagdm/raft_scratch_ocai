package com.myraft.raft.model;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

/**
 * Конверт команды реплицируемого лога.
 * <p>
 * Пользовательские команды несут непрозрачный для ядра payload state machine;
 * {@link NoOpCommand} — Raft-уровневый маркер, который не доходит до state machine.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
@JsonSubTypes({
        @JsonSubTypes.Type(value = UserCommand.class, name = "USER"),
        @JsonSubTypes.Type(value = NoOpCommand.class, name = "NOOP")
})
public sealed interface RaftCommand permits UserCommand, NoOpCommand {
}
