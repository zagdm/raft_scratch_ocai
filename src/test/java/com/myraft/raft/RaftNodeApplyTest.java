package com.myraft.raft;

import com.fasterxml.jackson.databind.node.TextNode;
import com.myraft.config.ClusterProperties;
import com.myraft.config.RaftProperties;
import com.myraft.raft.fsm.ApplyResult;
import com.myraft.raft.fsm.Command;
import com.myraft.raft.fsm.StateMachine;
import com.myraft.raft.fsm.kv.PutCommand;
import com.myraft.raft.model.LogEntry;
import com.myraft.raft.model.NoOpCommand;
import com.myraft.raft.model.RaftState;
import com.myraft.raft.model.UserCommand;
import com.myraft.raft.rpc.RaftRpcClient;
import com.myraft.raft.storage.InMemoryRaftStorage;
import com.myraft.raft.timer.ElectionTimer;
import org.junit.jupiter.api.Test;
import org.springframework.web.client.RestTemplate;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Apply-цикл этапа 2: StateMachine подменяется без изменения ядра,
 * NoOpCommand не доходит до state machine, commitIndex задаётся вручную
 * (продвижение по большинству — этап 3).
 */
class RaftNodeApplyTest {

    @Test
    void appliesOnlyCommittedEntriesAndSkipsNoOp() {
        RaftState state = stateWithThreeEntries();
        RecordingStateMachine sm = new RecordingStateMachine();
        RaftNode node = node(state, sm);

        assertNull(node.appliedResult(1), "nothing applied before commit");
        assertEquals(0, state.getLastApplied());

        state.advanceCommitIndex(2);
        node.applyCommittedEntries();

        assertEquals(List.of(1L), sm.indexes, "NoOpCommand must not reach state machine");
        assertEquals(2, state.getLastApplied());
        assertEquals("value-1", node.appliedResult(1).value());
        assertNull(node.appliedResult(2), "NoOp has no ApplyResult");

        state.advanceCommitIndex(3);
        node.applyCommittedEntries();

        assertEquals(List.of(1L, 3L), sm.indexes, "entries applied in index order");
        assertEquals(3, state.getLastApplied());
        assertEquals("value-3", node.appliedResult(3).value());
    }

    @Test
    void repeatedApplyDoesNothing() {
        RaftState state = stateWithThreeEntries();
        RecordingStateMachine sm = new RecordingStateMachine();
        RaftNode node = node(state, sm);
        state.advanceCommitIndex(3);

        node.applyCommittedEntries();
        node.applyCommittedEntries();

        assertEquals(List.of(1L, 3L), sm.indexes);
        assertEquals(3, state.getLastApplied());
    }

    @Test
    void failedApplyDoesNotAdvanceLastApplied() {
        RaftState state = stateWithThreeEntries();
        RecordingStateMachine sm = new RecordingStateMachine();
        sm.failAt = 3L;
        RaftNode node = node(state, sm);
        state.advanceCommitIndex(3);

        assertThrows(IllegalStateException.class, node::applyCommittedEntries);

        assertEquals(List.of(1L, 3L), sm.indexes);
        assertEquals(2, state.getLastApplied(), "failed entry must remain unapplied");
    }

    @Test
    void commitIndexIsMonotonicAndCappedByLogEnd() {
        RaftState state = stateWithThreeEntries();

        state.advanceCommitIndex(99);
        assertEquals(3, state.getCommitIndex());
        state.advanceCommitIndex(1);
        assertEquals(3, state.getCommitIndex());
    }

    private static RaftState stateWithThreeEntries() {
        RaftState state = new RaftState(new InMemoryRaftStorage());
        state.updateTermAndVote(2, null);
        state.appendLogEntry(new LogEntry(1, 2,
                new UserCommand(new PutCommand("a", TextNode.valueOf("1")))));
        state.appendLogEntry(new LogEntry(2, 2, new NoOpCommand()));
        state.appendLogEntry(new LogEntry(3, 2,
                new UserCommand(new PutCommand("b", TextNode.valueOf("2")))));
        return state;
    }

    private static RaftNode node(RaftState state, StateMachine<?> sm) {
        RaftProperties props = new RaftProperties();
        props.setNodeId("node-1");
        props.setElectionTimeoutMinMs(10_000);
        props.setElectionTimeoutMaxMs(10_000);
        props.setHeartbeatIntervalMs(10_000);
        return new RaftNode(state, props,
                new RaftRpcClient(new RestTemplate(), new ClusterProperties()),
                new ElectionTimer(props), sm);
    }

    private static class RecordingStateMachine implements StateMachine<String> {
        private final List<Long> indexes = new ArrayList<>();
        private Long failAt;

        @Override
        public ApplyResult<String> apply(long index, Command command) {
            indexes.add(index);
            if (failAt != null && index == failAt) {
                throw new IllegalStateException("simulated apply failure");
            }
            // Ядро передаёт в SM payload из UserCommand, а не сам конверт.
            assertInstanceOf(PutCommand.class, command);
            return new ApplyResult<>("previous-" + index, "value-" + index, index);
        }
    }
}
