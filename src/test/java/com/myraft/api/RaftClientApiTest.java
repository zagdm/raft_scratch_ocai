package com.myraft.api;

import com.myraft.config.ClusterProperties;
import com.myraft.config.RaftProperties;
import com.myraft.raft.ApplyTimeoutException;
import com.myraft.raft.RaftNode;
import com.myraft.raft.fsm.kv.KvStateMachine;
import com.myraft.raft.model.NodeRole;
import com.myraft.raft.model.RaftState;
import com.myraft.raft.rpc.RaftRpcClient;
import com.myraft.raft.rpc.RaftRpcService;
import com.myraft.raft.rpc.dto.AppendEntriesRequest;
import com.myraft.raft.rpc.dto.AppendEntriesResponse;
import com.myraft.raft.rpc.dto.RequestVoteRequest;
import com.myraft.raft.rpc.dto.RequestVoteResponse;
import com.myraft.raft.storage.InMemoryRaftStorage;
import com.myraft.raft.timer.ElectionTimer;
import org.junit.jupiter.api.Test;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.client.RestTemplate;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BooleanSupplier;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.springframework.http.MediaType.APPLICATION_JSON;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/** Этап 4: клиентский API поверх in-memory кластера (сеть заменена прямыми вызовами RPC). */
class RaftClientApiTest {

    @Test
    void writeThenReadThroughApi() throws Exception {
        try (Cluster cluster = new Cluster(1)) {
            Node node = cluster.node("node-1");
            node.fireElection();
            assertEquals(NodeRole.LEADER, node.state.getRole());
            MockMvc mvc = node.mvc();

            mvc.perform(post("/raft/log")
                            .contentType(APPLICATION_JSON)
                            .content("{\"key\":\"k\",\"value\":\"v\"}"))
                    .andExpect(status().isOk())
                    .andExpect(jsonPath("$.previousValue").value(nullValue()))
                    .andExpect(jsonPath("$.value").value("v"))
                    .andExpect(jsonPath("$.index").value(2));

            // NoOp на индексе 1 — Raft-маркер и не должен попадать в клиентский лог.
            mvc.perform(get("/raft/log"))
                    .andExpect(status().isOk())
                    .andExpect(jsonPath("$", hasSize(1)))
                    .andExpect(jsonPath("$[0].index").value(2))
                    .andExpect(jsonPath("$[0].key").value("k"))
                    .andExpect(jsonPath("$[0].value").value("v"));

            mvc.perform(get("/raft/kv").param("key", "k"))
                    .andExpect(status().isOk())
                    .andExpect(jsonPath("$.found").value(true))
                    .andExpect(jsonPath("$.value").value("v"));

            mvc.perform(post("/raft/log")
                            .contentType(APPLICATION_JSON)
                            .content("{\"key\":\"k\",\"value\":{\"n\":1}}"))
                    .andExpect(status().isOk())
                    .andExpect(jsonPath("$.previousValue").value("v"))
                    .andExpect(jsonPath("$.value.n").value(1))
                    .andExpect(jsonPath("$.index").value(3));

            mvc.perform(get("/raft/log"))
                    .andExpect(status().isOk())
                    .andExpect(jsonPath("$", hasSize(2)))
                    .andExpect(jsonPath("$[1].key").value("k"));
        }
    }

    @Test
    void jsonNullValueIsAValidPut() throws Exception {
        try (Cluster cluster = new Cluster(1)) {
            Node node = cluster.node("node-1");
            node.fireElection();
            MockMvc mvc = node.mvc();

            mvc.perform(post("/raft/log")
                            .contentType(APPLICATION_JSON)
                            .content("{\"key\":\"nil\",\"value\":null}"))
                    .andExpect(status().isOk())
                    .andExpect(jsonPath("$.value").value(nullValue()));

            mvc.perform(get("/raft/kv").param("key", "nil"))
                    .andExpect(status().isOk())
                    .andExpect(jsonPath("$.found").value(true))
                    .andExpect(jsonPath("$.value").value(nullValue()));
        }
    }

    @Test
    void followerWriteReturnsRedirectWithLeaderId() throws Exception {
        try (Cluster cluster = new Cluster(2)) {
            Node leader = cluster.node("node-1");
            Node follower = cluster.node("node-2");
            leader.fireElection();
            leader.replicate();
            await(() -> "node-1".equals(follower.state.getLeaderId()),
                    "follower must learn the current leader");

            follower.mvc().perform(post("/raft/log")
                            .contentType(APPLICATION_JSON)
                            .content("{\"key\":\"k\",\"value\":\"v\"}"))
                    .andExpect(status().isServiceUnavailable())
                    .andExpect(jsonPath("$.leaderId").value("node-1"))
                    .andExpect(jsonPath("$.error").value("node is not leader (leaderId=node-1)"));
        }
    }

    @Test
    void blankKeyIsRejected() throws Exception {
        try (Cluster cluster = new Cluster(1)) {
            Node node = cluster.node("node-1");
            node.fireElection();

            node.mvc().perform(post("/raft/log")
                            .contentType(APPLICATION_JSON)
                            .content("{\"key\":\"  \",\"value\":\"v\"}"))
                    .andExpect(status().isBadRequest())
                    .andExpect(jsonPath("$.error").value("key must not be blank"));
        }
    }

    @Test
    void awaitAppliedTimesOutForUnappliedIndex() {
        try (Cluster cluster = new Cluster(1)) {
            Node node = cluster.node("node-1");
            assertThrows(ApplyTimeoutException.class, () -> node.node.awaitApplied(42, 10));
        }
    }

    private static void await(BooleanSupplier condition, String message) {
        long deadline = System.currentTimeMillis() + 2000;
        while (!condition.getAsBoolean()) {
            if (System.currentTimeMillis() >= deadline) {
                throw new AssertionError(message);
            }
            try {
                Thread.sleep(5);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new AssertionError(e);
            }
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
            KvStateMachine sm = new KvStateMachine();
            RaftNode node = new RaftNode(state, props, client, timer, sm);
            RaftRpcService service = new RaftRpcService(node, state, props);
            RaftClientService clientService = new RaftClientService(node, state, sm, 2000);
            RaftClientController controller = new RaftClientController(clientService, state, props);
            MockMvc mvc = MockMvcBuilders.standaloneSetup(controller).build();

            nodes.put(id, new Node(id, state, node, service, timer, client, mvc));
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
        private final MockMvc mvc;

        private Node(String id, RaftState state, RaftNode node, RaftRpcService service,
                     ManualTimer timer, RoutingClient client, MockMvc mvc) {
            this.id = id;
            this.state = state;
            this.node = node;
            this.service = service;
            this.timer = timer;
            this.client = client;
            this.mvc = mvc;
        }

        private void fireElection() {
            timer.fire();
        }

        private void replicate() {
            node.replicateNow();
        }

        private MockMvc mvc() {
            return mvc;
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
        }

        @Override
        public void reset() {
        }

        private void fire() {
            if (callback != null) {
                callback.run();
            }
        }
    }

    /** Маршрутизация RPC по nodeId, без реального HTTP. */
    private static final class RoutingClient extends RaftRpcClient {
        private final Map<String, Node> nodes;
        private final Set<String> blocked = ConcurrentHashMap.newKeySet();

        private RoutingClient(Map<String, Node> nodes, ClusterProperties peers) {
            super(new RestTemplate(), peers);
            this.nodes = nodes;
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
}
