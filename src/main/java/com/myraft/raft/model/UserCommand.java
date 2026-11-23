package com.myraft.raft.model;

import com.myraft.raft.fsm.Command;

import java.util.Objects;

/** Пользовательская запись лога с командой для state machine. */
public record UserCommand(Command payload) implements RaftCommand {

    public UserCommand {
        Objects.requireNonNull(payload, "payload");
    }
}
