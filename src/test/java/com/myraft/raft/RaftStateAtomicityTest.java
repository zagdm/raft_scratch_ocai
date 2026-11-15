package com.myraft.raft;

import com.myraft.raft.model.RaftState;
import com.myraft.raft.storage.InMemoryRaftStorage;
import org.junit.jupiter.api.Test;

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

class RaftStateAtomicityTest {

    @Test
    void updateTermAndVoteDoesNotRollbackTerm() {
        InMemoryRaftStorage storage = new InMemoryRaftStorage();
        RaftState state = new RaftState(storage);
        state.updateTermAndVote(5, "node-a");

        state.updateTermAndVote(3, "node-b");

        assertEquals(5, state.getCurrentTerm());
        assertEquals("node-a", state.getVotedFor());
        assertEquals(5, storage.load().term());
        assertEquals("node-a", storage.load().votedFor());
    }

    @Test
    void startElectionMonotonicallyIncrementsAndVotesForSelf() {
        InMemoryRaftStorage storage = new InMemoryRaftStorage();
        RaftState state = new RaftState(storage);
        state.updateTermAndVote(2, "node-x");

        int term = state.startElection("node-1");

        assertEquals(3, term);
        assertEquals(3, state.getCurrentTerm());
        assertEquals("node-1", state.getVotedFor());
        assertEquals(3, storage.load().term());
        assertEquals("node-1", storage.load().votedFor());
    }

    @Test
    void concurrentGrantVoteAllowsExactlyOneVotePerTerm() throws Exception {
        int threads = 8;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        try {
            InMemoryRaftStorage storage = new InMemoryRaftStorage();
            RaftState state = new RaftState(storage);
            state.updateTermAndVote(5, null);

            CyclicBarrier barrier = new CyclicBarrier(threads);
            List<Callable<Boolean>> tasks = new ArrayList<>();
            for (int i = 0; i < threads; i++) {
                String candidate = "cand-" + i;
                tasks.add(() -> {
                    barrier.await(5, TimeUnit.SECONDS);
                    return state.grantVote(5, candidate).granted();
                });
            }
            List<Future<Boolean>> futures = pool.invokeAll(tasks);
            int granted = 0;
            for (Future<Boolean> f : futures) {
                if (f.get(5, TimeUnit.SECONDS)) {
                    granted++;
                }
            }
            assertEquals(1, granted, "exactly one candidate must get a vote");
            assertEquals(5, state.getCurrentTerm());
            assertNotNull(state.getVotedFor());
        } finally {
            pool.shutdownNow();
        }
    }
}
