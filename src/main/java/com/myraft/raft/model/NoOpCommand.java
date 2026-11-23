package com.myraft.raft.model;

/**
 * Raft-уровневая служебная запись лидера: фиксирует commitIndex текущего терма,
 * не меняет state machine и не отдаётся клиенту.
 */
public record NoOpCommand() implements RaftCommand {
}
