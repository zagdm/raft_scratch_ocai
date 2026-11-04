package com.myraft.raft;

import com.myraft.config.RaftProperties;
import com.myraft.raft.model.NodeRole;
import com.myraft.raft.model.RaftState;
import com.myraft.raft.rpc.RaftRpcClient;
import com.myraft.raft.rpc.dto.AppendEntriesRequest;
import com.myraft.raft.rpc.dto.AppendEntriesResponse;
import com.myraft.raft.rpc.dto.RequestVoteRequest;
import com.myraft.raft.rpc.dto.RequestVoteResponse;
import com.myraft.raft.timer.ElectionTimer;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

@Component
public class RaftNode {
    private static final Logger log = LoggerFactory.getLogger(RaftNode.class);
    private final RaftState raftState;
    private final RaftProperties raftProperties;
    private final RaftRpcClient rpcClient;
    private final ElectionTimer electionTimer;
    private final ScheduledExecutorService heartbeatScheduler = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "raft-heartbeat");
        t.setDaemon(true);
        return t;
    });

    private volatile ScheduledFuture<?> heartbeatTask;

    public RaftNode(RaftState raftState, RaftProperties raftProperties, RaftRpcClient rpcClient, ElectionTimer electionTimer) {
        this.raftState = raftState;
        this.raftProperties = raftProperties;
        this.rpcClient = rpcClient;
        this.electionTimer = electionTimer;
    }

    @PostConstruct
    public void init() {
        updateMdc();
        try {
            raftState.setRole(NodeRole.FOLLOWER);
            raftState.setLeaderId(null);
            electionTimer.setTimeoutCallback(this::onElectionTimeout);
            electionTimer.start();
            log.info("node started term={} votedFor={} peers={}",
                    raftState.getCurrentTerm(), raftState.getVotedFor(), rpcClient.getPeers());
        } finally {
            clearMdc();
        }
    }

    private void onElectionTimeout() {
        updateMdc();
        try {
            if (raftState.getRole() == NodeRole.LEADER) {
                electionTimer.reset();
                return;
            }
            startElection();
        } finally {
            clearMdc();
        }
    }

    private void startElection() {
        updateMdc();
        try {
            int newTerm = raftState.getCurrentTerm() + 1;
            raftState.updateTermAndVote(newTerm, raftProperties.getNodeId());
            raftState.setRole(NodeRole.CANDIDATE);
            raftState.setLeaderId(null);
            updateMdc();
            log.info("starting election term={}", newTerm);
            log.debug("voted for self");
            electionTimer.reset();

            List<String> peers = rpcClient.getPeers();
            AtomicInteger votes = new AtomicInteger(1); // vote for self
            int majority = majority(peers.size() + 1);

            // Кластер из одного узла (или узел-единственный доступный) не получит
            // ни одного ответа, поэтому кворум нужно проверить до рассылки.
            maybeBecomeLeader(votes, majority);

            for (String peer : peers) {
                RequestVoteRequest req = new RequestVoteRequest(raftState.getCurrentTerm(), raftProperties.getNodeId());
                rpcClient.requestVote(peer, req).thenAccept(resp -> {
                    if (resp == null) {
                        return;
                    }
                    handleRequestVoteResponse(resp, votes, majority);
                });
            }
        } finally {
            clearMdc();
        }
    }

    private static int majority(int clusterSize) {
        return clusterSize / 2 + 1;
    }

    private void maybeBecomeLeader(AtomicInteger votes, int majority) {
        if (votes.get() >= majority) {
            becomeLeader();
        }
    }

    private synchronized void handleRequestVoteResponse(RequestVoteResponse resp, AtomicInteger votes, int majority) {
        updateMdc();
        try {
            if (resp.getTerm() > raftState.getCurrentTerm()) {
                log.info("stepping down (outgoing RequestVote response has higher term) newTerm={}", resp.getTerm());
                raftState.updateTermAndVote(resp.getTerm(), null);
                raftState.setRole(NodeRole.FOLLOWER);
                raftState.setLeaderId(null);
                stopHeartbeats();
                electionTimer.reset();
                return;
            }
            if (raftState.getRole() != NodeRole.CANDIDATE) {
                return;
            }
            if (raftState.getCurrentTerm() != resp.getTerm()) {
                return;
            }
            if (resp.isVoteGranted()) {
                int v = votes.incrementAndGet();
                log.info("vote granted ({}/{}) from peer response", v, majority);
                if (v >= majority) {
                    becomeLeader();
                }
            } else {
                log.debug("vote rejected from peer response");
            }
        } finally {
            clearMdc();
        }
    }

    private synchronized void becomeLeader() {
        if (raftState.getRole() != NodeRole.CANDIDATE) {
            return;
        }
        updateMdc();
        try {
            raftState.setRole(NodeRole.LEADER);
            raftState.setLeaderId(raftProperties.getNodeId());
            updateMdc();
            log.info("became leader term={}", raftState.getCurrentTerm());
            startHeartbeats();
        } finally {
            clearMdc();
        }
    }

    private synchronized void startHeartbeats() {
        stopHeartbeats();
        long interval = raftProperties.getHeartbeatIntervalMs();
        heartbeatTask = heartbeatScheduler.scheduleWithFixedDelay(
                this::sendHeartbeats, 0, interval, TimeUnit.MILLISECONDS);
    }

    private synchronized void stopHeartbeats() {
        ScheduledFuture<?> task = heartbeatTask;
        heartbeatTask = null;
        if (task != null) {
            task.cancel(false);
        }
    }

    private void sendHeartbeats() {
        if (raftState.getRole() != NodeRole.LEADER) {
            stopHeartbeats();
            return;
        }
        updateMdc();
        try {
            List<String> peers = rpcClient.getPeers();
            AppendEntriesRequest req = new AppendEntriesRequest(raftState.getCurrentTerm(), raftProperties.getNodeId());
            for (String peer : peers) {
                rpcClient.appendEntries(peer, req).thenAccept(resp -> {
                    if (resp == null) {
                        return;
                    }
                    handleAppendEntriesResponse(resp);
                });
            }
        } finally {
            clearMdc();
        }
    }

    private synchronized void handleAppendEntriesResponse(AppendEntriesResponse resp) {
        if (raftState.getRole() != NodeRole.LEADER) {
            return;
        }
        if (resp.getTerm() > raftState.getCurrentTerm()) {
            log.info("stepping down (outgoing AppendEntries response has higher term) newTerm={}", resp.getTerm());
            raftState.updateTermAndVote(resp.getTerm(), null);
            raftState.setRole(NodeRole.FOLLOWER);
            raftState.setLeaderId(null);
            stopHeartbeats();
            electionTimer.reset();
        }
    }

    private void updateMdc() {
        try {
            if (raftProperties != null) {
                MDC.put("nodeId", raftProperties.getNodeId());
            }
            if (raftState != null) {
                MDC.put("term", String.valueOf(raftState.getCurrentTerm()));
                MDC.put("role", String.valueOf(raftState.getRole()));
            }
        } catch (Exception ignored) {
        }
    }

    private void clearMdc() {
        try {
            MDC.remove("nodeId");
            MDC.remove("term");
            MDC.remove("role");
        } catch (Exception ignored) {
        }
    }

    @PreDestroy
    public void shutdown() {
        electionTimer.stop();
        heartbeatScheduler.shutdownNow();
    }
}
