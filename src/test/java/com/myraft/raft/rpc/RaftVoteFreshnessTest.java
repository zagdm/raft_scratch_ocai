package com.myraft.raft.rpc;

import com.myraft.config.ClusterProperties;
import com.myraft.config.RaftProperties;
import com.myraft.raft.RaftNode;
import com.myraft.raft.fsm.kv.KvStateMachine;
import com.myraft.raft.model.LogEntry;
import com.myraft.raft.model.NoOpCommand;
import com.myraft.raft.model.RaftState;
import com.myraft.raft.model.RequestVoteEvent;
import com.myraft.raft.rpc.dto.RequestVoteRequest;
import com.myraft.raft.storage.InMemoryRaftStorage;
import com.myraft.raft.timer.ElectionTimer;
import org.junit.jupiter.api.Test;
import org.springframework.web.client.RestTemplate;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Проверки ограничения свежести лога при голосовании (§5.4.1 статьи Raft).
 */
class RaftVoteFreshnessTest {

    private static RaftProperties props() {
        RaftProperties p = new RaftProperties();
        p.setNodeId("node-1");
        p.setElectionTimeoutMinMs(10_000);
        p.setElectionTimeoutMaxMs(10_000);
        p.setHeartbeatIntervalMs(10_000);
        return p;
    }

    private static RaftState stateWithLog() {
        RaftState state = new RaftState(new InMemoryRaftStorage());
        state.updateTermAndVote(3, null);
        state.appendLogEntry(new LogEntry(1, 3, new NoOpCommand()));
        return state;
    }

    private static RaftNode node(RaftState state, RaftProperties props) {
        return new RaftNode(state, props, new RaftRpcClient(new RestTemplate(), new ClusterProperties()),
                new ElectionTimer(props), new KvStateMachine());
    }

    @Test
    void nodeRejectsCandidateWithOlderLogAtSameTerm() {
        RaftState state = stateWithLog();
        RaftNode node = node(state, props());

        var outcome = node.handleRequestVote(new RequestVoteRequest(3, "cand", 0, 0));

        assertEquals(RequestVoteEvent.REJECTED_LOG_NOT_UP_TO_DATE, outcome.event());
        assertFalse(outcome.granted());
        assertEquals(3, outcome.state().after().currentTerm());
        assertNull(state.getVotedFor());
    }

    @Test
    void nodeStepsDownButDoesNotGrantWhenHigherTermCandidateHasOlderLog() {
        RaftState state = stateWithLog();
        RaftNode node = node(state, props());

        var outcome = node.handleRequestVote(new RequestVoteRequest(6, "cand", 0, 0));

        assertEquals(RequestVoteEvent.REJECTED_LOG_NOT_UP_TO_DATE, outcome.event());
        assertFalse(outcome.granted());
        assertEquals(6, outcome.state().after().currentTerm());
        assertNull(state.getVotedFor(), "vote must not be granted to an outdated candidate");
    }

    @Test
    void servicePrefiltersOutdatedCandidateAtSameTerm() {
        RaftState state = stateWithLog();
        RaftProperties config = props();
        RaftRpcService service = new RaftRpcService(node(state, config), state, config);

        var response = service.handleRequestVote(new RequestVoteRequest(3, "cand", 0, 0));

        assertFalse(response.isVoteGranted());
        assertEquals(3, response.getTerm());
        assertNull(state.getVotedFor());
    }

    @Test
    void upToDateCandidateStillGetsVote() {
        RaftState state = stateWithLog();
        RaftProperties config = props();
        RaftRpcService service = new RaftRpcService(node(state, config), state, config);

        var response = service.handleRequestVote(new RequestVoteRequest(3, "cand", 1, 3));

        assertTrue(response.isVoteGranted());
        assertEquals("cand", state.getVotedFor());
    }

    @Test
    void emptyLocalLogAcceptsCandidateWithZeroLogFields() {
        RaftState state = new RaftState(new InMemoryRaftStorage());
        RaftProperties config = props();
        RaftRpcService service = new RaftRpcService(node(state, config), state, config);

        assertTrue(service.handleRequestVote(new RequestVoteRequest(5, "cand-a")).isVoteGranted());
    }
}
