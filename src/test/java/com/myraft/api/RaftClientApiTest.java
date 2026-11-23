package com.myraft.api;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
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
import com.myraft.raft.storage.RaftStorage;
import com.myraft.raft.timer.ElectionTimer;
import org.junit.jupiter.api.Test;
import org.springframework.mock.web.MockHttpServletResponse;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
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
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.springframework.http.MediaType.APPLICATION_JSON;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * Этап 4 + план расширения клиентских тестов: API поверх in-memory кластера.
 * Сеть заменена прямыми вызовами RPC, таймер — ручным; узлы можно останавливать
 * и перезапускать поверх того же {@code RaftStorage}.
 */
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
            awaitLeaderId(follower, "node-1", "follower must learn the current leader");

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

    @Test
    void writeWithoutKnownLeaderIsRedirectedWithNullLeaderId() throws Exception {
        try (Cluster cluster = new Cluster(1)) {
            Node node = cluster.node("node-1");
            MockMvc mvc = node.mvc();

            MvcResult result = mvc.perform(post("/raft/log")
                            .contentType(APPLICATION_JSON)
                            .content("{\"key\":\"k\",\"value\":\"v\"}"))
                    .andReturn();
            assertApiError(result, 503, "node is not leader", null);

            mvc.perform(get("/raft/log"))
                    .andExpect(status().isOk())
                    .andExpect(jsonPath("$", hasSize(0)));

            node.fireElection();
            mvc.perform(post("/raft/log")
                            .contentType(APPLICATION_JSON)
                            .content("{\"key\":\"k\",\"value\":\"v\"}"))
                    .andExpect(status().isOk())
                    .andExpect(jsonPath("$.value").value("v"))
                    .andExpect(jsonPath("$.index").value(2));
        }
    }

    @Test
    void applyTimeoutReturns504AndEntryIsAppliedAfterQuorumRestored() throws Exception {
        try (Cluster cluster = new Cluster(3, 100)) {
            Node leader = cluster.node("node-1");
            Node follower = cluster.node("node-2");
            leader.fireElection();
            for (int i = 0; i < 8 && (leader.state.getCommitIndex() < 1
                    || follower.state.getCommitIndex() < 1); i++) {
                leader.replicate();
                sleep(5);
            }
            awaitCommit(leader, 1, "NoOp must be committed on the leader");
            awaitCommit(follower, 1, "NoOp must be committed on the follower");

            cluster.stop("node-2");
            cluster.stop("node-3");

            MvcResult result = leader.mvc().perform(post("/raft/log")
                            .contentType(APPLICATION_JSON)
                            .content("{\"key\":\"pending\",\"value\":\"v\"}"))
                    .andReturn();
            assertApiError(result, 504, "apply timeout", null);

            assertEquals(1, leader.state.getCommitIndex(), "entry must stay uncommitted without quorum");
            leader.mvc().perform(get("/raft/log"))
                    .andExpect(status().isOk())
                    .andExpect(jsonPath("$", hasSize(1)))
                    .andExpect(jsonPath("$[0].index").value(2))
                    .andExpect(jsonPath("$[0].key").value("pending"));
            leader.mvc().perform(get("/raft/kv").param("key", "pending"))
                    .andExpect(status().isOk())
                    .andExpect(jsonPath("$.found").value(false));

            Node revived = cluster.restart("node-2");
            for (int i = 0; i < 12 && revived.state.getCommitIndex() < 2; i++) {
                leader.replicate();
                sleep(5);
            }
            awaitCommit(revived, 2, "restored follower must catch up and commit");
            revived.mvc().perform(get("/raft/kv").param("key", "pending"))
                    .andExpect(status().isOk())
                    .andExpect(jsonPath("$.found").value(true))
                    .andExpect(jsonPath("$.value").value("v"));
        }
    }

    @Test
    void missingKeyIsDistinguishedFromStoredJsonNull() throws Exception {
        try (Cluster cluster = new Cluster(1)) {
            Node node = cluster.node("node-1");
            node.fireElection();
            MockMvc mvc = node.mvc();

            mvc.perform(get("/raft/kv").param("key", "absent"))
                    .andExpect(status().isOk())
                    .andExpect(jsonPath("$.found").value(false))
                    .andExpect(jsonPath("$.value").value(nullValue()));

            mvc.perform(post("/raft/log")
                            .contentType(APPLICATION_JSON)
                            .content("{\"key\":\"nil\",\"value\":null}"))
                    .andExpect(status().isOk())
                    .andExpect(jsonPath("$.index").value(2));

            mvc.perform(get("/raft/kv").param("key", "nil"))
                    .andExpect(status().isOk())
                    .andExpect(jsonPath("$.found").value(true))
                    .andExpect(jsonPath("$.value").value(nullValue()));
        }
    }

    @Test
    void clientLogAndKvSurviveLeaderChangeAndRestart() throws Exception {
        // Фоновая репликация нужна, чтобы POST дождался apply на кворуме.
        try (Cluster cluster = new Cluster(3, 2000, 10)) {
            Node n1 = cluster.node("node-1");
            Node n2 = cluster.node("node-2");
            Node n3 = cluster.node("node-3");
            n1.fireElection();
            n1.mvc().perform(post("/raft/log")
                            .contentType(APPLICATION_JSON)
                            .content("{\"key\":\"k1\",\"value\":\"v1\"}"))
                    .andExpect(status().isOk());
            n1.mvc().perform(post("/raft/log")
                            .contentType(APPLICATION_JSON)
                            .content("{\"key\":\"k2\",\"value\":\"v2\"}"))
                    .andExpect(status().isOk());
            for (int i = 0; i < 8 && (n2.state.getCommitIndex() < 3 || n3.state.getCommitIndex() < 3); i++) {
                n1.replicate();
                sleep(5);
            }
            awaitCommit(n2, 3, "follower 2 must commit both writes");
            awaitCommit(n3, 3, "follower 3 must commit both writes");

            cluster.stop("node-1");
            n2.fireElection();
            assertEquals(NodeRole.LEADER, n2.state.getRole());
            n2.mvc().perform(post("/raft/log")
                            .contentType(APPLICATION_JSON)
                            .content("{\"key\":\"k3\",\"value\":\"v3\"}"))
                    .andExpect(status().isOk());
            for (int i = 0; i < 8 && n3.state.getCommitIndex() < 5; i++) {
                n2.replicate();
                sleep(5);
            }
            awaitCommit(n3, 5, "new follower must learn the third write");

            assertUserLog(n2.mvc());

            Node revived = cluster.restart("node-1");
            for (int i = 0; i < 12 && revived.state.getCommitIndex() < 5; i++) {
                n2.replicate();
                sleep(5);
            }
            awaitCommit(revived, 5, "restarted node must catch up with the new leader");
            assertUserLog(revived.mvc());
            for (String key : List.of("k1", "k2", "k3")) {
                revived.mvc().perform(get("/raft/kv").param("key", key))
                        .andExpect(status().isOk())
                        .andExpect(jsonPath("$.found").value(true))
                        .andExpect(jsonPath("$.value").value("v" + key.charAt(1)));
            }
        }
    }

    @Test
    void jsonValueTypesRoundTripThroughApi() throws Exception {
        try (Cluster cluster = new Cluster(1)) {
            Node node = cluster.node("node-1");
            node.fireElection();
            MockMvc mvc = node.mvc();

            mvc.perform(put("number", "42")).andExpect(status().isOk()).andExpect(jsonPath("$.index").value(2));
            mvc.perform(put("flag", "true")).andExpect(status().isOk()).andExpect(jsonPath("$.index").value(3));
            mvc.perform(put("array", "[1,\"x\"]")).andExpect(status().isOk()).andExpect(jsonPath("$.index").value(4));
            mvc.perform(put("object", "{\"n\":{\"m\":2}}"))
                    .andExpect(status().isOk()).andExpect(jsonPath("$.index").value(5));
            mvc.perform(put("nil", "null")).andExpect(status().isOk()).andExpect(jsonPath("$.index").value(6));

            mvc.perform(get("/raft/kv").param("key", "number"))
                    .andExpect(status().isOk()).andExpect(jsonPath("$.value").value(42));
            mvc.perform(get("/raft/kv").param("key", "flag"))
                    .andExpect(status().isOk()).andExpect(jsonPath("$.value").value(true));
            mvc.perform(get("/raft/kv").param("key", "array"))
                    .andExpect(status().isOk())
                    .andExpect(jsonPath("$.value[0]").value(1))
                    .andExpect(jsonPath("$.value[1]").value("x"));
            mvc.perform(get("/raft/kv").param("key", "object"))
                    .andExpect(status().isOk()).andExpect(jsonPath("$.value.n.m").value(2));
            mvc.perform(get("/raft/kv").param("key", "nil"))
                    .andExpect(status().isOk()).andExpect(jsonPath("$.value").value(nullValue()));

            mvc.perform(get("/raft/log")).andExpect(status().isOk()).andExpect(jsonPath("$", hasSize(5)));

            mvc.perform(put("number", "\"str\""))
                    .andExpect(status().isOk())
                    .andExpect(jsonPath("$.previousValue").value(42))
                    .andExpect(jsonPath("$.value").value("str"));
        }
    }

    private static org.springframework.test.web.servlet.request.MockHttpServletRequestBuilder put(
            String key, String valueJson) {
        return post("/raft/log")
                .contentType(APPLICATION_JSON)
                .content("{\"key\":\"" + key + "\",\"value\":" + valueJson + "}");
    }

    private static void assertUserLog(MockMvc mvc) throws Exception {
        mvc.perform(get("/raft/log"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$", hasSize(3)))
                .andExpect(jsonPath("$[0].index").value(2))
                .andExpect(jsonPath("$[0].term").value(1))
                .andExpect(jsonPath("$[0].key").value("k1"))
                .andExpect(jsonPath("$[1].index").value(3))
                .andExpect(jsonPath("$[1].term").value(1))
                .andExpect(jsonPath("$[1].key").value("k2"))
                .andExpect(jsonPath("$[2].index").value(5))
                .andExpect(jsonPath("$[2].term").value(2))
                .andExpect(jsonPath("$[2].key").value("k3"));
    }

    private static void awaitCommit(Node node, long index, String message) {
        await(() -> node.state.getCommitIndex() >= index, message);
    }

    private static void awaitLeaderId(Node node, String leaderId, String message) {
        await(() -> leaderId.equals(node.state.getLeaderId()), message);
    }

    private static void assertApiError(MvcResult result, int expectedStatus, String errorContains,
                                       String leaderId) throws Exception {
        MockHttpServletResponse response = result.getResponse();
        assertEquals(expectedStatus, response.getStatus(), response.getContentAsString());
        JsonNode body = new ObjectMapper().readTree(response.getContentAsString());
        assertTrue(body.path("error").asText().contains(errorContains), body.toString());
        if (leaderId == null) {
            assertTrue(body.path("leaderId").isNull(), body.toString());
        } else {
            assertEquals(leaderId, body.path("leaderId").asText(), body.toString());
        }
    }

    private static void await(BooleanSupplier condition, String message) {
        long deadline = System.currentTimeMillis() + 2000;
        while (!condition.getAsBoolean()) {
            if (System.currentTimeMillis() >= deadline) {
                throw new AssertionError(message);
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
        // Параллельно читается heartbeat-потоками и меняется тестом при stop/restart.
        private final Map<String, Node> nodes = new ConcurrentHashMap<>();
        private final Map<String, RaftStorage> storages = new LinkedHashMap<>();
        private final Set<String> offline = ConcurrentHashMap.newKeySet();
        private final List<Node> stopped = new ArrayList<>();
        private final List<String> allIds;
        private final long applyTimeoutMs;
        private final int heartbeatIntervalMs;

        private Cluster(int size) {
            this(size, 2000);
        }

        private Cluster(int size, long applyTimeoutMs) {
            this(size, applyTimeoutMs, 60_000);
        }

        /**
         * @param heartbeatIntervalMs короткий интервал включает фоновую репликацию:
         *                            клиентский {@code POST} ждёт apply и не может
         *                            вызвать {@code replicate()} из того же потока
         */
        private Cluster(int size, long applyTimeoutMs, int heartbeatIntervalMs) {
            List<String> ids = new ArrayList<>();
            for (int i = 1; i <= size; i++) {
                ids.add("node-" + i);
            }
            this.allIds = List.copyOf(ids);
            this.applyTimeoutMs = applyTimeoutMs;
            this.heartbeatIntervalMs = heartbeatIntervalMs;
            for (String id : allIds) {
                storages.put(id, new InMemoryRaftStorage());
                create(id);
            }
        }

        private void create(String id) {
            Node node = newNode(id);
            nodes.put(id, node);
            node.node.init();
        }

        /** Новый экземпляр узла поверх того же storage (имитация перезапуска). */
        private Node newNode(String id) {
            RaftProperties props = new RaftProperties();
            props.setNodeId(id);
            props.setElectionTimeoutMinMs(60_000);
            props.setElectionTimeoutMaxMs(60_000);
            props.setHeartbeatIntervalMs(heartbeatIntervalMs);

            ClusterProperties peers = new ClusterProperties();
            peers.setPeers(allIds.stream().filter(peer -> !peer.equals(id)).collect(Collectors.toList()));

            RaftState state = new RaftState(storages.get(id));
            ManualTimer timer = new ManualTimer(props);
            RoutingClient client = new RoutingClient(nodes, peers, offline);
            KvStateMachine sm = new KvStateMachine();
            RaftNode node = new RaftNode(state, props, client, timer, sm);
            RaftRpcService service = new RaftRpcService(node, state, props);
            RaftClientService clientService = new RaftClientService(node, state, sm, applyTimeoutMs);
            RaftClientController controller = new RaftClientController(clientService, state, props);
            MockMvc mvc = MockMvcBuilders.standaloneSetup(controller).build();

            return new Node(id, state, node, service, timer, client, mvc);
        }

        private Node node(String id) {
            return nodes.get(id);
        }

        /** Полная гибель узла: RPC в обе стороны отбрасываются. */
        private Node stop(String id) {
            Node node = nodes.remove(id);
            node.node.shutdown();
            offline.add(id);
            stopped.add(node);
            return node;
        }

        private Node restart(String id) {
            Node node = newNode(id);
            nodes.put(id, node);
            offline.remove(id);
            node.node.init();
            return node;
        }

        @Override
        public void close() {
            for (Node node : nodes.values()) {
                node.node.shutdown();
            }
            for (Node node : stopped) {
                node.node.shutdown();
            }
        }
    }

    private static final class Node {
        // Не-final: компоненты подменяются новыми при имитации перезапуска узла.
        private String id;
        private RaftState state;
        private RaftNode node;
        private RaftRpcService service;
        private ManualTimer timer;
        private RoutingClient client;
        private MockMvc mvc;

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

    /** Маршрутизация RPC по nodeId с возможностью simulated offline. */
    private static final class RoutingClient extends RaftRpcClient {
        private final Map<String, Node> nodes;
        private final Set<String> offline;
        private final Set<String> blocked = ConcurrentHashMap.newKeySet();

        private RoutingClient(Map<String, Node> nodes, ClusterProperties peers, Set<String> offline) {
            super(new RestTemplate(), peers);
            this.nodes = nodes;
            this.offline = offline;
        }

        @Override
        public CompletableFuture<RequestVoteResponse> requestVote(String peer, RequestVoteRequest request) {
            Node target = offline.contains(peer) ? null : nodes.get(peer);
            return CompletableFuture.completedFuture(
                    target == null ? null : target.service.handleRequestVote(request));
        }

        @Override
        public CompletableFuture<AppendEntriesResponse> appendEntries(String peer, AppendEntriesRequest request) {
            Node target = offline.contains(peer) || blocked.contains(peer) ? null : nodes.get(peer);
            return CompletableFuture.completedFuture(
                    target == null ? null : target.service.handleAppendEntries(request));
        }
    }
}
