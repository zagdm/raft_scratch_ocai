package com.myraft.raft.model;

/**
 * Служебная запись лидера: не меняет state machine и не отдаётся клиенту.
 */
public record NoOpCommand() implements Command {
}
