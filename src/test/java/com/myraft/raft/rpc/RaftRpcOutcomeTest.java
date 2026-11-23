package com.myraft.raft.rpc;

import com.myraft.config.ClusterProperties;
import com.myraft.config.RaftProperties;
import com.myraft.raft.RaftNode;
import com.myraft.raft.fsm.kv.KvStateMachine;
import com.myraft.raft.model.AppendEntriesEvent;
import com.myraft.raft.model.AppendEntriesOutcome;
import com.myraft.raft.model.NodeRole;
import com.myraft.raft.model.RaftState;
import com.myraft.raft.model.RaftState.RaftStateSnapshot;
import com.myraft.raft.model.RequestVoteEvent;
import com.myraft.raft.model.RequestVoteOutcome;
import com.myraft.raft.rpc.dto.AppendEntriesRequest;
import com.myraft.raft.rpc.dto.AppendEntriesResponse;
import com.myraft.raft.rpc.dto.RequestVoteRequest;
import com.myraft.raft.rpc.dto.RequestVoteResponse;
import com.myraft.raft.storage.InMemoryRaftStorage;
import com.myraft.raft.timer.ElectionTimer;
import org.junit.jupiter.api.Test;
import org.springframework.web.client.RestTemplate;

import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Проверки outcome/event для входящих RPC и того, что сервис собирает ответ
 * из outcome, не перечитывая живое состояние узла.
 */
class RaftRpcOutcomeTest {

    private static RaftProperties props(String nodeId) {
        RaftProperties p = new RaftProperties();
        p.setNodeId(nodeId);
        p.setElectionTimeoutMinMs(10_000);
        p.setElectionTimeoutMaxMs(10_000);
        p.setHeartbeatIntervalMs(10_000);
        return p;
    }

    private static RaftNode newNode(RaftState state, RaftProperties props) {
        return new RaftNode(state, props, new RaftRpcClient(new RestTemplate(), new ClusterProperties()),
                new ElectionTimer(props), new KvStateMachine());
    }

    private static RaftRpcService newService(RaftNode node, RaftState state, RaftProperties props) {
        return new RaftRpcService(node, state, props);
    }

    @Test
    void requestVoteEventsCoverAllDecisions() {
        RaftState state = new RaftState(new InMemoryRaftStorage());
        RaftNode node = newNode(state, props("node-1"));

        RequestVoteOutcome advanced = node.handleRequestVote(new RequestVoteRequest(5, "cand-a"));
        assertEquals(RequestVoteEvent.GRANTED_TERM_ADVANCED, advanced.event());
        assertTrue(advanced.granted());
        assertEquals(0, advanced.state().before().currentTerm());
        assertEquals(5, advanced.state().after().currentTerm());
        assertEquals("cand-a", advanced.state().after().votedFor());
        assertEquals(NodeRole.FOLLOWER, advanced.state().after().role());

        // Узел возвращает GRANTED и для первого, и для повторного голоса;
        // GRANTED_REPEAT классифицируется сервисом по before.votedFor.
        RequestVoteOutcome repeat = node.handleRequestVote(new RequestVoteRequest(5, "cand-a"));
        assertEquals(RequestVoteEvent.GRANTED, repeat.event());
        assertTrue(repeat.granted());
        assertEquals("cand-a", repeat.state().before().votedFor());
        assertEquals(5, repeat.state().after().currentTerm());

        RequestVoteOutcome alreadyVoted = node.handleRequestVote(new RequestVoteRequest(5, "cand-b"));
        assertEquals(RequestVoteEvent.REJECTED_ALREADY_VOTED, alreadyVoted.event());
        assertFalse(alreadyVoted.granted());
        assertEquals("cand-a", state.getVotedFor());
        assertEquals(5, alreadyVoted.state().after().currentTerm());

        RequestVoteOutcome stale = node.handleRequestVote(new RequestVoteRequest(3, "cand-c"));
        assertEquals(RequestVoteEvent.REJECTED_STALE_TERM, stale.event());
        assertFalse(stale.granted());
        assertEquals(5, stale.state().after().currentTerm());

        RaftState fresh = new RaftState(new InMemoryRaftStorage());
        fresh.updateTermAndVote(5, null);
        RequestVoteOutcome firstVote = newNode(fresh, props("node-1"))
                .handleRequestVote(new RequestVoteRequest(5, "cand-d"));
        assertEquals(RequestVoteEvent.GRANTED, firstVote.event());
        assertTrue(firstVote.granted());
        assertNull(firstVote.state().before().votedFor());
        assertEquals("cand-d", firstVote.state().after().votedFor());
    }

    @Test
    void appendEntriesEventsCoverAllDecisions() {
        RaftState state = new RaftState(new InMemoryRaftStorage());
        RaftNode node = newNode(state, props("node-1"));

        AppendEntriesOutcome advanced = node.handleAppendEntries(new AppendEntriesRequest(5, "leader-a"));
        assertEquals(AppendEntriesEvent.ACCEPTED_TERM_ADVANCED, advanced.event());
        assertTrue(advanced.accepted());
        assertEquals(5, advanced.state().after().currentTerm());
        assertEquals("leader-a", advanced.state().after().leaderId());
        assertEquals(NodeRole.FOLLOWER, advanced.state().after().role());

        AppendEntriesOutcome idempotent = node.handleAppendEntries(new AppendEntriesRequest(5, "leader-a"));
        assertEquals(AppendEntriesEvent.ACCEPTED_IDEMPOTENT, idempotent.event());
        assertTrue(idempotent.accepted());

        AppendEntriesOutcome otherLeader = node.handleAppendEntries(new AppendEntriesRequest(5, "leader-b"));
        assertEquals(AppendEntriesEvent.REJECTED_FOREIGN_LEADER, otherLeader.event());
        assertFalse(otherLeader.accepted());
        assertEquals("leader-a", otherLeader.state().after().leaderId());
        assertSameTransition(otherLeader);

        // Смена роли при том же лидере: leaderId известен, запрос от него же.
        state.setRole(NodeRole.CANDIDATE);
        AppendEntriesOutcome discovered = node.handleAppendEntries(new AppendEntriesRequest(5, "leader-a"));
        assertEquals(AppendEntriesEvent.ACCEPTED_LEADER_DISCOVERED, discovered.event());
        assertEquals(NodeRole.CANDIDATE, discovered.state().before().role());
        assertEquals(NodeRole.FOLLOWER, discovered.state().after().role());

        AppendEntriesOutcome stale = node.handleAppendEntries(new AppendEntriesRequest(3, "old-leader"));
        assertEquals(AppendEntriesEvent.REJECTED_STALE_TERM, stale.event());
        assertFalse(stale.accepted());
        assertEquals(5, stale.state().after().currentTerm());
        assertEquals("leader-a", stale.state().after().leaderId());
        assertEquals(5, state.getCurrentTerm());
    }

    @Test
    void serviceBuildsResponseFromOutcome() {
        RaftState state = new RaftState(new InMemoryRaftStorage());
        RaftProperties config = props("node-1");
        RaftRpcService service = newService(newNode(state, config), state, config);

        RequestVoteResponse vote = service.handleRequestVote(new RequestVoteRequest(5, "cand-a"));
        assertTrue(vote.isVoteGranted());
        assertEquals(5, vote.getTerm());

        RequestVoteResponse repeat = service.handleRequestVote(new RequestVoteRequest(5, "cand-a"));
        assertTrue(repeat.isVoteGranted(), "repeat vote must stay granted");
        assertEquals(5, repeat.getTerm());

        RequestVoteResponse alreadyVoted = service.handleRequestVote(new RequestVoteRequest(5, "cand-b"));
        assertFalse(alreadyVoted.isVoteGranted(), "another candidate at same term must be rejected by prefilter");
        assertEquals(5, alreadyVoted.getTerm());

        AppendEntriesResponse accepted = service.handleAppendEntries(new AppendEntriesRequest(5, "leader-a"));
        assertTrue(accepted.isSuccess());
        assertEquals(5, accepted.getTerm());

        AppendEntriesResponse foreign = service.handleAppendEntries(new AppendEntriesRequest(5, "leader-b"));
        assertFalse(foreign.isSuccess(), "foreign leader at same term must be rejected");
        assertEquals(5, foreign.getTerm());

        AppendEntriesResponse stale = service.handleAppendEntries(new AppendEntriesRequest(2, "old-leader"));
        assertFalse(stale.isSuccess());
        assertEquals(5, stale.getTerm());
        assertEquals(5, state.getCurrentTerm());
        assertEquals("leader-a", state.getLeaderId());
    }

    @Test
    void serviceDoesNotReadLiveStateAfterNodeCall() {
        CountingRaftState state = new CountingRaftState();
        RaftProperties config = props("node-1");
        RaftRpcService service = newService(newNode(state, config), state, config);

        // service: 1 snapshot (pre) + node: 2 snapshot (before/after) = 3
        service.handleRequestVote(new RequestVoteRequest(5, "cand-a"));
        assertEquals(3, state.snapshots(), "RequestVote path must use only pre + outcome snapshots");

        // prefilter: только pre-снапшот, узел не вызывается
        service.handleRequestVote(new RequestVoteRequest(3, "stale"));
        assertEquals(4, state.snapshots(), "stale RequestVote must be rejected without node call");

        service.handleRequestVote(new RequestVoteRequest(5, "cand-b"));
        assertEquals(5, state.snapshots(), "already-voted RequestVote must be rejected without node call");

        // pre + node before/after + node re-check под локом при GRANTED_REPEAT
        service.handleRequestVote(new RequestVoteRequest(5, "cand-a"));
        assertEquals(9, state.snapshots(), "repeat vote must use outcome snapshots plus guarded timer reset");

        CountingRaftState aeState = new CountingRaftState();
        RaftRpcService aeService = newService(newNode(aeState, config), aeState, config);
        aeService.handleAppendEntries(new AppendEntriesRequest(5, "leader-a"));
        assertEquals(3, aeState.snapshots(), "AppendEntries path must use only pre + outcome snapshots");

        aeService.handleAppendEntries(new AppendEntriesRequest(5, "leader-b"));
        assertEquals(4, aeState.snapshots(), "foreign-leader heartbeat must be rejected without node call");
    }

    @Test
    void serviceClassifiesRepeatVoteAndResetsTimerViaNode() {
        RaftState state = new RaftState(new InMemoryRaftStorage());
        RaftProperties config = props("node-1");
        CountingElectionTimer timer = new CountingElectionTimer(config);
        RaftNode node = new RaftNode(state, config,
                new RaftRpcClient(new RestTemplate(), new ClusterProperties()), timer, new KvStateMachine());
        RaftRpcService service = newService(node, state, config);

        assertTrue(service.handleRequestVote(new RequestVoteRequest(5, "cand-a")).isVoteGranted());
        assertEquals(1, timer.resets(), "first grant resets timer inside node");

        assertTrue(service.handleRequestVote(new RequestVoteRequest(5, "cand-a")).isVoteGranted());
        assertEquals(2, timer.resets(), "GRANTED_REPEAT must reset timer via node");

        assertFalse(service.handleRequestVote(new RequestVoteRequest(5, "cand-b")).isVoteGranted());
        assertEquals(2, timer.resets(), "rejected vote must not reset timer");
    }

    /** Отклонённый запрос не должен менять состояние: before и after — один снапшот. */
    private static void assertSameTransition(AppendEntriesOutcome outcome) {
        assertSame(outcome.state().before(), outcome.state().after());
    }

    /** Считает сбросы election timer (узел + сервис через узел). */
    private static final class CountingElectionTimer extends ElectionTimer {
        private final AtomicInteger resets = new AtomicInteger();

        private CountingElectionTimer(RaftProperties props) {
            super(props);
        }

        @Override
        public void reset() {
            resets.incrementAndGet();
            super.reset();
        }

        private int resets() {
            return resets.get();
        }
    }

    /**
     * Считает обращения к {@link RaftState#snapshot()}: после вызова узла сервис
     * не должен читать живое состояние.
     */
    private static final class CountingRaftState extends RaftState {
        private final AtomicInteger snapshots = new AtomicInteger();

        private CountingRaftState() {
            super(new InMemoryRaftStorage());
        }

        @Override
        public RaftStateSnapshot snapshot() {
            snapshots.incrementAndGet();
            return super.snapshot();
        }

        private int snapshots() {
            return snapshots.get();
        }
    }
}
