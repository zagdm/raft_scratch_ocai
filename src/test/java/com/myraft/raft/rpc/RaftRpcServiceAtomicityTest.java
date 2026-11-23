package com.myraft.raft.rpc;

import com.myraft.config.ClusterProperties;
import com.myraft.config.RaftProperties;
import com.myraft.raft.RaftNode;
import com.myraft.raft.fsm.kv.KvStateMachine;
import com.myraft.raft.model.NodeRole;
import com.myraft.raft.model.RaftState;
import com.myraft.raft.rpc.dto.AppendEntriesRequest;
import com.myraft.raft.rpc.dto.AppendEntriesResponse;
import com.myraft.raft.rpc.dto.RequestVoteRequest;
import com.myraft.raft.rpc.dto.RequestVoteResponse;
import com.myraft.raft.storage.InMemoryRaftStorage;
import com.myraft.raft.timer.ElectionTimer;
import org.junit.jupiter.api.Test;
import org.springframework.web.client.RestTemplate;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class RaftRpcServiceAtomicityTest {

    private static RaftProperties props(String nodeId) {
        RaftProperties p = new RaftProperties();
        p.setNodeId(nodeId);
        p.setElectionTimeoutMinMs(10_000);
        p.setElectionTimeoutMaxMs(10_000);
        p.setHeartbeatIntervalMs(10_000);
        return p;
    }

    private static RaftRpcService newService(RaftState state, RaftProperties props) {
        RaftRpcClient client = new RaftRpcClient(new RestTemplate(), new ClusterProperties());
        RaftNode node = new RaftNode(state, props, client, new ElectionTimer(props), new KvStateMachine());
        return new RaftRpcService(node, state, props);
    }

    @Test
    void concurrentHandleRequestVoteGrantsExactlyOnce() throws Exception {
        RaftState state = new RaftState(new InMemoryRaftStorage());
        RaftRpcService service = newService(state, props("node-1"));

        int threads = 2;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        try {
            CyclicBarrier barrier = new CyclicBarrier(threads);
            Callable<RequestVoteResponse> t1 = () -> {
                barrier.await(5, TimeUnit.SECONDS);
                return service.handleRequestVote(new RequestVoteRequest(5, "cand-a"));
            };
            Callable<RequestVoteResponse> t2 = () -> {
                barrier.await(5, TimeUnit.SECONDS);
                return service.handleRequestVote(new RequestVoteRequest(5, "cand-b"));
            };
            Future<RequestVoteResponse> f1 = pool.submit(t1);
            Future<RequestVoteResponse> f2 = pool.submit(t2);
            RequestVoteResponse r1 = f1.get(5, TimeUnit.SECONDS);
            RequestVoteResponse r2 = f2.get(5, TimeUnit.SECONDS);

            int granted = (r1.isVoteGranted() ? 1 : 0) + (r2.isVoteGranted() ? 1 : 0);
            assertEquals(1, granted, "exactly one RequestVote must be granted");
            assertEquals(5, state.getCurrentTerm());
            assertNotNull(state.getVotedFor());
        } finally {
            pool.shutdownNow();
        }
    }

    @Test
    void startElectionDoesNotRollbackWhenTermRaisedConcurrently() throws Exception {
        InMemoryRaftStorage storage = new InMemoryRaftStorage();
        RaftState state = new RaftState(storage);
        state.updateTermAndVote(1, null);

        CountDownLatch electionEntered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);

        // Simulate race: another RPC raises term to 7 while "startElection" is about to run.
        // We coordinate deterministically by raising term before calling startElection after latch.
        Thread raiser = new Thread(() -> {
            try {
                electionEntered.await(5, TimeUnit.SECONDS);
                state.updateTermAndVote(7, "node-x");
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } finally {
                release.countDown();
            }
        });
        raiser.start();

        // Deterministic sequence: signal that we are at election point, let raiser bump term, then start election.
        electionEntered.countDown();
        assertTrue(release.await(5, TimeUnit.SECONDS));
        int electionTerm = state.startElection("node-1");

        raiser.join(2000);

        assertEquals(8, electionTerm, "election must increment from highest observed term under lock");
        assertEquals(8, state.getCurrentTerm(), "term must not roll back");
        assertEquals("node-1", state.getVotedFor());
    }

    @Test
    void staleAppendEntriesDoesNotChangeVote() {
        RaftState state = new RaftState(new InMemoryRaftStorage());
        state.updateTermAndVote(5, "node-v");
        RaftRpcService service = newService(state, props("node-1"));

        AppendEntriesResponse resp = service.handleAppendEntries(new AppendEntriesRequest(3, "old-leader"));

        assertFalse(resp.isSuccess());
        assertEquals(5, resp.getTerm());
        assertEquals(5, state.getCurrentTerm());
        assertEquals("node-v", state.getVotedFor());
    }
}
