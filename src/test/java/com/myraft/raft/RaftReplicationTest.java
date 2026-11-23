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
import com.myraft.raft.model.NodeRole;
import com.myraft.raft.model.RaftLog;
import com.myraft.raft.model.RaftState;
import com.myraft.raft.model.UserCommand;
import com.myraft.raft.rpc.RaftRpcClient;
import com.myraft.raft.rpc.RaftRpcService;
import com.myraft.raft.rpc.dto.AppendEntriesRequest;
import com.myraft.raft.rpc.dto.AppendEntriesResponse;
import com.myraft.raft.rpc.dto.RequestVoteRequest;
import com.myraft.raft.rpc.dto.RequestVoteResponse;
import com.myraft.raft.storage.InMemoryRaftStorage;
import com.myraft.raft.timer.ElectionTimer;
import org.junit.jupiter.api.Test;
import org.springframework.web.client.RestTemplate;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.BooleanSupplier;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Этап 3: репликация лога, truncate, NoOp и коммит по большинству на трёх узлах.
 * Сеть заменена прямой маршрутизацией RPC между узлами, таймер — ручным.
 */
class RaftReplicationTest {

    @Test
    void leaderAppendsNoOpAndCommitsItOnMajority() {
        try (Cluster cluster = new Cluster(3)) {
            Node leader = cluster.node("node-1");
            leader.fireElection();

            assertEquals(NodeRole.LEADER, leader.state.getRole());
            assertEquals(1, leader.log().lastIndex());
            LogEntry noOp = leader.log().entryAt(1);
            assertEquals(1, noOp.term());
            assertInstanceOf(NoOpCommand.class, noOp.command());

            leader.replicate();
            await(() -> cluster.node("node-2").log().entryAt(1) != null
                    && cluster.node("node-3").log().entryAt(1) != null,
                    "NoOp must be replicated to both followers");
            await(() -> leader.state.getCommitIndex() == 1, "leader must commit NoOp at term 1");

            leader.replicate();
            await(() -> cluster.node("node-2").state.getCommitIndex() == 1
                    && cluster.node("node-3").state.getCommitIndex() == 1,
                    "followers must learn leaderCommit");

            for (Node node : cluster.nodes.values()) {
                assertEquals(1, node.state.getLastApplied());
                assertTrue(node.log().entryAt(1).command() instanceof NoOpCommand);
                assertTrue(node.sm.indexes.isEmpty(), "NoOp must not reach state machine");
            }
        }
    }

    @Test
    void followerRejectsStalePrevLogThenTruncatesConflictingTail() {
        try (Cluster cluster = new Cluster(3)) {
            Node n1 = cluster.node("node-1");
            Node n2 = cluster.node("node-2");
            Node n3 = cluster.node("node-3");
            PutCommand common = new PutCommand("common", TextNode.valueOf("1"));
            PutCommand fresh = new PutCommand("fresh", TextNode.valueOf("2"));
            PutCommand stale = new PutCommand("stale", TextNode.valueOf("old"));

            // Терм 2 в логах: выборы пройдут в терме 3, а запись 2 конфликтует по терму.
            for (Node node : cluster.nodes.values()) {
                node.state.updateTermAndVote(2, null);
            }
            n1.seed(new LogEntry(1, 1, new UserCommand(common)),
                    new LogEntry(2, 2, new UserCommand(fresh)));
            n2.seed(new LogEntry(1, 1, new UserCommand(common)),
                    new LogEntry(2, 1, new UserCommand(stale)));
            n3.seed(new LogEntry(1, 1, new UserCommand(common)));

            n1.fireElection();
            assertEquals(NodeRole.LEADER, n1.state.getRole());

            // prevLogIndex=2/term=2 не совпадает с local term=1: пир откажет,
            // лидер откатит nextIndex, следующий раунд вырежет конфликтующий хвост.
            for (int i = 0; i < 8 && !sameLog(n1, n2); i++) {
                n1.replicate();
                sleep(10);
            }

            assertEquals(n1.log(), n2.log(), "follower log must converge with leader");
            assertEquals(n1.log(), n3.log());
            assertEquals(3, n2.log().lastIndex());
            assertEquals(2, n2.log().entryAt(2).term());
            assertEquals(new UserCommand(fresh), n2.log().entryAt(2).command());
            assertInstanceOf(NoOpCommand.class, n2.log().entryAt(3).command());

            n1.replicate();
            await(() -> n2.state.getCommitIndex() == 3 && n3.state.getCommitIndex() == 3,
                    "all followers must commit through index 3");
            assertEquals(List.of(1L, 2L), n2.sm.indexes, "NoOp is skipped, user entries applied in order");
        }
    }

    @Test
    void oldTermEntryIsNotCommittedWithoutCurrentTermMajority() {
        try (Cluster cluster = new Cluster(2)) {
            Node n1 = cluster.node("node-1");
            Node n2 = cluster.node("node-2");
            PutCommand old = new PutCommand("old", TextNode.valueOf("value"));
            for (Node node : cluster.nodes.values()) {
                node.state.updateTermAndVote(1, null);
                node.seed(new LogEntry(1, 1, new UserCommand(old)));
            }

            // Второй узел физически держит старую запись, но лидер не знает об этом
            // до успешного ответа: коммит старого терма запрещён.
            n1.client.block(n2.id);
            n1.fireElection();
            assertEquals(NodeRole.LEADER, n1.state.getRole());
            assertEquals(2, n1.log().entryAt(2).term());
            assertEquals(0, n1.state.getCommitIndex(), "must not commit an old-term entry directly");

            n1.client.unblock(n2.id);
            n1.replicate();
            await(() -> n1.state.getCommitIndex() == 2, "NoOp of current term must commit index 2");
            n1.replicate(); // следующий раунд несёт leaderCommit=2
            await(() -> n2.state.getCommitIndex() == 2, "follower must learn leaderCommit");
            assertEquals(List.of(1L), n1.sm.indexes);
            assertEquals(List.of(1L), n2.sm.indexes);
        }
    }

    @Test
    void reElectionKeepsCommittedUserEntryAndConvergesOldLeader() {
        try (Cluster cluster = new Cluster(3)) {
            Node n1 = cluster.node("node-1");
            Node n2 = cluster.node("node-2");
            Node n3 = cluster.node("node-3");

            n1.fireElection();
            PutCommand value = new PutCommand("key", TextNode.valueOf("value"));
            long index = n1.append(value);
            assertEquals(2, index);
            n1.replicate();
            await(() -> n2.log().entryAt(2) != null && n3.log().entryAt(2) != null,
                    "user entry must be replicated before re-election");
            await(() -> n1.state.getCommitIndex() == 2, "user entry must be committed");
            n1.replicate();
            await(() -> n2.state.getCommitIndex() == 2 && n3.state.getCommitIndex() == 2,
                    "followers must apply the user entry");

            // Старый лидер больше не рассылает heartbeat, но продолжает принимать RPC.
            n1.client.block(n2.id);
            n1.client.block(n3.id);
            n2.fireElection();
            assertEquals(NodeRole.LEADER, n2.state.getRole());
            assertEquals(n2.state.getCurrentTerm(), n1.state.getCurrentTerm(),
                    "old leader must observe the new term from RequestVote");
            assertEquals(new UserCommand(value), n2.log().entryAt(2).command(),
                    "new leader must keep the committed entry");

            for (int i = 0; i < 8 && n1.state.getRole() != NodeRole.FOLLOWER; i++) {
                n2.replicate();
                sleep(10);
            }
            await(() -> n1.state.getRole() == NodeRole.FOLLOWER, "old leader must step down");
            await(() -> sameLog(n1, n2) && sameLog(n3, n2), "all logs must converge after re-election");

            for (Node node : cluster.nodes.values()) {
                assertEquals(List.of(2L), node.sm.indexes,
                        "NoOp is skipped; committed user entry stays applied everywhere");
            }
        }
    }

    @Test
    void mismatchResponseCarriesConflictHint() {
        try (Cluster cluster = new Cluster(1)) {
            Node node = cluster.node("node-1");
            node.seed(new LogEntry(1, 2, new UserCommand(new PutCommand("k", TextNode.valueOf("v")))));
            node.state.updateTermAndVote(4, null);

            AppendEntriesResponse response = node.service.handleAppendEntries(
                    new AppendEntriesRequest(4, "leader", 5, 2, List.of(), 0));

            assertFalse(response.isSuccess());
            assertEquals(4, response.getTerm());
            assertEquals(1, response.getLastIndex());
            assertEquals(0, response.getConflictTerm(), "no local entry with prevLogTerm=2");
            assertEquals(2, response.getConflictIndex(), "first missing index is returned");
        }
    }

    private static boolean sameLog(Node a, Node b) {
        return a.log().equals(b.log());
    }

    private static void await(BooleanSupplier condition, String message) {
        long deadline = System.currentTimeMillis() + 3000;
        while (!condition.getAsBoolean()) {
            if (System.currentTimeMillis() >= deadline) {
                fail(message);
            }
            sleep(5);
        }
    }

    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError(e);
        }
    }

    /** Узлы кластера; сеть — прямые вызовы сервисов, без HTTP. */
    private static final class Cluster implements AutoCloseable {
        private final Map<String, Node> nodes = new LinkedHashMap<>();
        private final List<String> allIds;

        private Cluster(int size) {
            List<String> ids = new ArrayList<>();
            for (int i = 1; i <= size; i++) {
                ids.add("node-" + i);
            }
            this.allIds = List.copyOf(ids);
            for (String id : allIds) {
                create(id);
            }
        }

        private void create(String id) {
            RaftProperties props = new RaftProperties();
            props.setNodeId(id);
            props.setElectionTimeoutMinMs(60_000);
            props.setElectionTimeoutMaxMs(60_000);
            props.setHeartbeatIntervalMs(60_000);

            ClusterProperties peers = new ClusterProperties();
            peers.setPeers(allIds.stream().filter(peer -> !peer.equals(id)).collect(Collectors.toList()));

            RaftState state = new RaftState(new InMemoryRaftStorage());
            ManualTimer timer = new ManualTimer(props);
            RoutingClient client = new RoutingClient(nodes, peers);
            RecordingStateMachine sm = new RecordingStateMachine();
            RaftNode node = new RaftNode(state, props, client, timer, sm);
            RaftRpcService service = new RaftRpcService(node, state, props);

            nodes.put(id, new Node(id, state, node, service, timer, client, sm));
            node.init();
        }

        private Node node(String id) {
            return nodes.get(id);
        }

        @Override
        public void close() {
            for (Node node : nodes.values()) {
                node.node.shutdown();
            }
        }
    }

    private static final class Node {
        private final String id;
        private final RaftState state;
        private final RaftNode node;
        private final RaftRpcService service;
        private final ManualTimer timer;
        private final RoutingClient client;
        private final RecordingStateMachine sm;

        private Node(String id, RaftState state, RaftNode node, RaftRpcService service,
                     ManualTimer timer, RoutingClient client, RecordingStateMachine sm) {
            this.id = id;
            this.state = state;
            this.node = node;
            this.service = service;
            this.timer = timer;
            this.client = client;
            this.sm = sm;
        }

        private RaftLog log() {
            return state.logSnapshot();
        }

        private void fireElection() {
            timer.fire();
        }

        private void replicate() {
            node.replicateNow();
        }

        private long append(Command command) {
            return node.appendUserCommand(command);
        }

        private void seed(LogEntry... entries) {
            state.appendLogEntries(List.of(entries));
        }
    }

    /** Таймер без реальных ожиданий: выборы инициируются тестом. */
    private static final class ManualTimer extends ElectionTimer {
        private Runnable callback;

        private ManualTimer(RaftProperties props) {
            super(props);
        }

        @Override
        public void setTimeoutCallback(Runnable callback) {
            this.callback = callback;
            super.setTimeoutCallback(callback);
        }

        @Override
        public void start() {
            //автотаймера нет
        }

        @Override
        public void reset() {
            //автотаймера нет
        }

        private void fire() {
            if (callback != null) {
                callback.run();
            }
        }
    }

    /** Маршрутизация RPC по nodeId с возможностью simulated offline. */
    private static final class RoutingClient extends RaftRpcClient {
        private final Map<String, Node> nodes;
        private final Set<String> blocked = ConcurrentHashMap.newKeySet();

        private RoutingClient(Map<String, Node> nodes, ClusterProperties peers) {
            super(new RestTemplate(), peers);
            this.nodes = nodes;
        }

        private void block(String peer) {
            blocked.add(peer);
        }

        private void unblock(String peer) {
            blocked.remove(peer);
        }

        @Override
        public CompletableFuture<RequestVoteResponse> requestVote(String peer, RequestVoteRequest request) {
            Node target = nodes.get(peer);
            return CompletableFuture.completedFuture(
                    target == null ? null : target.service.handleRequestVote(request));
        }

        @Override
        public CompletableFuture<AppendEntriesResponse> appendEntries(String peer, AppendEntriesRequest request) {
            Node target = blocked.contains(peer) ? null : nodes.get(peer);
            return CompletableFuture.completedFuture(
                    target == null ? null : target.service.handleAppendEntries(request));
        }
    }

    private static final class RecordingStateMachine implements StateMachine<String> {
        private final List<Long> indexes = new CopyOnWriteArrayList<>();

        @Override
        public ApplyResult<String> apply(long index, Command command) {
            indexes.add(index);
            return new ApplyResult<>(null, "value-" + index, index);
        }
    }
}
