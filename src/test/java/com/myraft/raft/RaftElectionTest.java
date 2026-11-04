package com.myraft.raft;

import com.myraft.raft.model.NodeRole;
import com.myraft.raft.model.RaftState;
import com.myraft.raft.storage.InMemoryRaftStorage;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

public class RaftElectionTest {

    @Test
    public void followerInitialState() {
        RaftState state = new RaftState(new InMemoryRaftStorage());
        assertEquals(NodeRole.FOLLOWER, state.getRole());
        assertEquals(0, state.getCurrentTerm());
        assertNull(state.getVotedFor());
    }

    @Test
    public void termAndVoteArePersisted() {
        InMemoryRaftStorage storage = new InMemoryRaftStorage();
        RaftState state = new RaftState(storage);

        state.updateTermAndVote(3, "node-2");

        assertEquals(3, state.getCurrentTerm());
        assertEquals("node-2", state.getVotedFor());
        assertEquals(3, storage.load().term());
        assertEquals("node-2", storage.load().votedFor());
    }

    @Test
    public void stateIsRestoredFromStorage() {
        InMemoryRaftStorage storage = new InMemoryRaftStorage();
        storage.persist(7, "node-3");

        RaftState restored = new RaftState(storage);

        assertEquals(7, restored.getCurrentTerm());
        assertEquals("node-3", restored.getVotedFor());
        assertEquals(NodeRole.FOLLOWER, restored.getRole());
    }
}
