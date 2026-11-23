package com.myraft.raft;

import com.myraft.config.RaftProperties;
import com.myraft.raft.fsm.ApplyResult;
import com.myraft.raft.fsm.StateMachine;
import com.myraft.raft.model.AppendEntriesEvent;
import com.myraft.raft.model.AppendEntriesOutcome;
import com.myraft.raft.model.LogEntry;
import com.myraft.raft.model.NodeRole;
import com.myraft.raft.model.RaftCommand;
import com.myraft.raft.model.RaftLog;
import com.myraft.raft.model.RaftState;
import com.myraft.raft.model.RaftState.RaftStateSnapshot;
import com.myraft.raft.model.RequestVoteEvent;
import com.myraft.raft.model.RequestVoteOutcome;
import com.myraft.raft.model.StateTransition;
import com.myraft.raft.model.UserCommand;
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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Единственная точка принятия решений и мутаций состояния узла.
 * <p>
 * Все переходы ({@code role}, {@code leaderId}, election timer, heartbeats) выполняются
 * под единым {@link #nodeLock}. Порядок блокировок: сначала {@code nodeLock}, затем
 * внутренний лок {@link RaftState}; обратный порядок запрещён. Сетевые вызовы
 * выполняются вне {@code nodeLock} на основе зафиксированного контекста.
 */
@Component
public class RaftNode {
    private static final Logger log = LoggerFactory.getLogger(RaftNode.class);

    private final Object nodeLock = new Object();
    private final RaftState raftState;
    private final RaftProperties raftProperties;
    private final RaftRpcClient rpcClient;
    private final ElectionTimer electionTimer;
    private final StateMachine<?> stateMachine;

    /** Результаты apply по индексу; ожидающие клиенты появятся на этапе 4. */
    private final Map<Long, ApplyResult<?>> appliedResults = new HashMap<>();
    private final ScheduledExecutorService heartbeatScheduler = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "raft-heartbeat");
        t.setDaemon(true);
        return t;
    });

    private ScheduledFuture<?> heartbeatTask;

    public RaftNode(RaftState raftState, RaftProperties raftProperties, RaftRpcClient rpcClient,
                    ElectionTimer electionTimer, StateMachine<?> stateMachine) {
        this.raftState = raftState;
        this.raftProperties = raftProperties;
        this.rpcClient = rpcClient;
        this.electionTimer = electionTimer;
        this.stateMachine = stateMachine;
    }

    @PostConstruct
    public void init() {
        updateMdc();
        try {
            synchronized (nodeLock) {
                becomeFollowerLocked(null, false);
                electionTimer.setTimeoutCallback(this::onElectionTimeout);
                electionTimer.start();
            }
            log.info("node started as FOLLOWER term={} votedFor={} peers={}",
                    raftState.getCurrentTerm(), raftState.getVotedFor(), rpcClient.getPeers());
        } finally {
            clearMdc();
        }
    }

    // ---------------------------------------------------------------- inbound RPC

    /**
     * Обрабатывает входящий {@code RequestVote} и возвращает решение вместе со снапшотами.
     * Логирование и построение ответа — ответственность {@code RaftRpcService}.
     */
    public RequestVoteOutcome handleRequestVote(RequestVoteRequest request) {
        synchronized (nodeLock) {
            RaftStateSnapshot before = raftState.snapshot();
            if (request.getTerm() < before.currentTerm()) {
                return new RequestVoteOutcome(RequestVoteEvent.REJECTED_STALE_TERM,
                        new StateTransition(before, before));
            }
            // §5.4.1: голосующий отказывает, если его лог свежее лога кандидата.
            boolean upToDate = RaftLog.isAtLeastAsUpToDate(
                    request.getLastLogTerm(), request.getLastLogIndex(),
                    before.lastLogTerm(), before.lastLogIndex());
            if (!upToDate) {
                if (request.getTerm() > before.currentTerm()) {
                    raftState.updateTermAndVote(request.getTerm(), null);
                    becomeFollowerLocked(null, true);
                }
                RaftStateSnapshot after = raftState.snapshot();
                return new RequestVoteOutcome(RequestVoteEvent.REJECTED_LOG_NOT_UP_TO_DATE,
                        new StateTransition(before, after));
            }

            RaftState.GrantVoteResult result = raftState.grantVote(request.getTerm(), request.getCandidateId());

            boolean granted = result.granted();
            // Повторный голос тому же кандидату классифицируется сервисом (GRANTED_REPEAT),
            // здесь он остаётся обычным GRANTED; таймер сбрасывает сервис через узел.
            boolean repeat = granted && !result.termIncreased()
                    && Objects.equals(before.votedFor(), request.getCandidateId());
            if (granted) {
                if (result.termIncreased()) {
                    becomeFollowerLocked(null, true);
                } else if (!repeat) {
                    electionTimer.reset();
                }
            }

            RequestVoteEvent event;
            if (!granted) {
                event = request.getTerm() < result.term()
                        ? RequestVoteEvent.REJECTED_STALE_TERM
                        : RequestVoteEvent.REJECTED_ALREADY_VOTED;
            } else if (result.termIncreased()) {
                event = RequestVoteEvent.GRANTED_TERM_ADVANCED;
            } else {
                event = RequestVoteEvent.GRANTED;
            }

            RaftStateSnapshot after = raftState.snapshot();
            return new RequestVoteOutcome(event, new StateTransition(before, after));
        }
    }

    /**
     * Сбрасывает election timer, только если состояние узла всё ещё соответствует
     * снапшоту {@code after} из outcome. Вызывается сервисом при {@code GRANTED_REPEAT}:
     * сам сервис таймера не трогает, а узел выполняет эффект под {@code nodeLock},
     * не сбрасывая таймер после возможного параллельного step-down/смены роли.
     */
    public void resetElectionTimerIfUnchanged(RaftStateSnapshot expected) {
        synchronized (nodeLock) {
            RaftStateSnapshot current = raftState.snapshot();
            if (current.currentTerm() == expected.currentTerm()
                    && Objects.equals(current.votedFor(), expected.votedFor())
                    && current.role() == expected.role()) {
                electionTimer.reset();
            }
        }
    }

    /**
     * Обрабатывает входящий {@code AppendEntries} (в этой реализации — heartbeat).
     * Терм проверяется повторно под {@code nodeLock}; лидер принимается только при
     * актуальном терме запроса.
     */
    public AppendEntriesOutcome handleAppendEntries(AppendEntriesRequest request) {
        synchronized (nodeLock) {
            RaftStateSnapshot before = raftState.snapshot();
            if (request.getTerm() < before.currentTerm()) {
                return new AppendEntriesOutcome(AppendEntriesEvent.REJECTED_STALE_TERM,
                        new StateTransition(before, before));
            }
            // При равном терме уже известный лидер не сменяется: второй лидер в том же
            // терме — признак рассинхрона/старого сообщения, heartbeat не принимаем.
            if (request.getTerm() == before.currentTerm()
                    && before.leaderId() != null
                    && !Objects.equals(before.leaderId(), request.getLeaderId())) {
                return new AppendEntriesOutcome(AppendEntriesEvent.REJECTED_FOREIGN_LEADER,
                        new StateTransition(before, before));
            }

            boolean termAdvanced = request.getTerm() > before.currentTerm();
            if (termAdvanced) {
                raftState.updateTermAndVote(request.getTerm(), null);
            }

            boolean roleChanged = before.role() != NodeRole.FOLLOWER;
            boolean leaderChanged = !Objects.equals(before.leaderId(), request.getLeaderId());
            // updateTermAndVote меняет только term/vote, поэтому role/leaderId из before актуальны.
            becomeFollowerLocked(request.getLeaderId(), true);

            AppendEntriesEvent event;
            if (termAdvanced) {
                event = AppendEntriesEvent.ACCEPTED_TERM_ADVANCED;
            } else if (roleChanged || leaderChanged) {
                event = AppendEntriesEvent.ACCEPTED_LEADER_DISCOVERED;
            } else {
                event = AppendEntriesEvent.ACCEPTED_IDEMPOTENT;
            }

            RaftStateSnapshot after = raftState.snapshot();
            return new AppendEntriesOutcome(event, new StateTransition(before, after));
        }
    }

    // ---------------------------------------------------------------- apply

    /**
     * Применяет закоммиченные записи к state machine под {@code nodeLock}.
     * Продвижение {@code commitIndex} по большинству появится на этапе 3;
     * пока тесты задают его через {@code RaftState.advanceCommitIndex}.
     */
    public void applyCommittedEntries() {
        updateMdc();
        try {
            synchronized (nodeLock) {
                applyCommittedLocked();
            }
        } finally {
            clearMdc();
        }
    }

    /** Результат apply непрозрачен для ядра; API-адаптер конкретизирует его на этапе 4. */
    public ApplyResult<?> appliedResult(long index) {
        synchronized (nodeLock) {
            return appliedResults.get(index);
        }
    }

    private void applyCommittedLocked() {
        long commitIndex = raftState.getCommitIndex();
        long lastApplied = raftState.getLastApplied();
        if (lastApplied >= commitIndex) {
            log.debug("apply skipped lastApplied={} commitIndex={}", lastApplied, commitIndex);
            return;
        }
        RaftLog logSnapshot = raftState.logSnapshot();
        while (lastApplied < commitIndex) {
            long next = lastApplied + 1;
            LogEntry entry = logSnapshot.entryAt(next);
            if (entry == null) {
                break;
            }
            RaftCommand command = entry.command();
            if (command instanceof UserCommand user) {
                ApplyResult<?> result = stateMachine.apply(next, user.payload());
                appliedResults.put(next, result);
                log.info("entry applied index={} lastApplied={}", next, next);
            } else {
                // NoOpCommand: Raft-маркер, state machine не вызывается.
                log.debug("no-op applied index={} lastApplied={}", next, next);
            }
            raftState.advanceLastApplied(next);
            lastApplied = next;
        }
    }

    // ---------------------------------------------------------------- elections

    private void onElectionTimeout() {
        updateMdc();
        try {
            log.debug("election timeout");
            startElection();
        } finally {
            clearMdc();
        }
    }

    private void startElection() {
        List<String> peers;
        int electionTerm;
        long electionLastLogIndex;
        int electionLastLogTerm;
        AtomicInteger votes = new AtomicInteger(1); // голос за себя
        int majority;

        synchronized (nodeLock) {
            RaftStateSnapshot snapshot = raftState.snapshot();
            if (snapshot.role() == NodeRole.LEADER) {
                electionTimer.reset();
                return;
            }

            electionTerm = raftState.startElection(raftProperties.getNodeId());
            RaftStateSnapshot afterStart = raftState.snapshot();

            // (1) повторная проверка перед мутацией роли/кампании
            if (afterStart.currentTerm() != electionTerm
                    || !Objects.equals(afterStart.votedFor(), raftProperties.getNodeId())) {
                becomeFollowerLocked(afterStart.leaderId(), true);
                return;
            }

            raftState.setRole(NodeRole.CANDIDATE);
            raftState.setLeaderId(null);
            updateMdc();
            log.info("starting election term={} (voted for self)", electionTerm);
            electionTimer.reset();

            electionLastLogIndex = afterStart.lastLogIndex();
            electionLastLogTerm = afterStart.lastLogTerm();
            peers = rpcClient.getPeers();
            majority = majority(peers.size() + 1);

            // Кластер из одного узла (или единственный доступный) не получит ответов,
            // поэтому кворум проверяется до рассылки.
            if (tryBecomeLeaderLocked(electionTerm, votes, majority)) {
                return;
            }
        }

        // Сетевые вызовы — вне nodeLock. Один лог на раунд выборов (как для heartbeats),
        // а не на каждый адресат; детали по пирам остаются в RaftRpcClient на DEBUG.
        log.info("RequestVote sent term={} peers={}", electionTerm, peers.size());
        for (String peer : peers) {
            RequestVoteRequest req = new RequestVoteRequest(electionTerm, raftProperties.getNodeId(),
                    electionLastLogIndex, electionLastLogTerm);
            rpcClient.requestVote(peer, req).thenAccept(resp -> {
                if (resp == null) {
                    return;
                }
                handleRequestVoteResponse(peer, resp, electionTerm, votes, majority);
            });
        }
    }

    private static int majority(int clusterSize) {
        return clusterSize / 2 + 1;
    }

    private void handleRequestVoteResponse(String peer, RequestVoteResponse resp, int electionTerm,
                                           AtomicInteger votes, int majority) {
        updateMdc();
        try {
            synchronized (nodeLock) {
                RaftStateSnapshot snapshot = raftState.snapshot();

                if (resp.getTerm() > snapshot.currentTerm()) {
                    log.info("stepping down (outgoing RequestVote response has higher term) newTerm={} from={}",
                            resp.getTerm(), peer);
                    raftState.updateTermAndVote(resp.getTerm(), null);
                    becomeFollowerLocked(null, true);
                    return;
                }

                // (4) контекст кампании: терм, роль и голос за себя
                if (snapshot.currentTerm() != electionTerm || resp.getTerm() != electionTerm) {
                    return;
                }
                if (snapshot.role() != NodeRole.CANDIDATE
                        || !Objects.equals(snapshot.votedFor(), raftProperties.getNodeId())) {
                    return;
                }

                if (resp.isVoteGranted()) {
                    int v = votes.incrementAndGet();
                    log.info("vote granted ({}/{}) from peer response from={}", v, majority, peer);
                    tryBecomeLeaderLocked(electionTerm, votes, majority);
                } else {
                    log.debug("vote rejected from peer response from={}", peer);
                }
            }
        } finally {
            clearMdc();
        }
    }

    /**
     * (2) Переход в лидера с обязательной повторной проверкой легитимности.
     *
     * @return {@code true}, если узел стал лидером
     */
    private boolean tryBecomeLeaderLocked(int expectedTerm, AtomicInteger votes, int majority) {
        if (votes.get() < majority) {
            return false;
        }
        RaftStateSnapshot snapshot = raftState.snapshot();
        if (snapshot.role() != NodeRole.CANDIDATE
                || snapshot.currentTerm() != expectedTerm
                || !Objects.equals(snapshot.votedFor(), raftProperties.getNodeId())) {
            return false;
        }
        raftState.setRole(NodeRole.LEADER);
        raftState.setLeaderId(raftProperties.getNodeId());
        updateMdc();
        log.info("became leader term={}", snapshot.currentTerm());
        startHeartbeatsLocked();
        return true;
    }

    // ---------------------------------------------------------------- heartbeats

    private void startHeartbeatsLocked() {
        stopHeartbeatsLocked();
        long interval = raftProperties.getHeartbeatIntervalMs();
        heartbeatTask = heartbeatScheduler.scheduleWithFixedDelay(
                this::sendHeartbeats, 0, interval, TimeUnit.MILLISECONDS);
    }

    private void stopHeartbeatsLocked() {
        ScheduledFuture<?> task = heartbeatTask;
        heartbeatTask = null;
        if (task != null) {
            task.cancel(false);
        }
    }

    private void sendHeartbeats() {
        AppendEntriesRequest req;
        List<String> peers;
        synchronized (nodeLock) {
            RaftStateSnapshot snapshot = raftState.snapshot();
            // (5) актуальность лидера/терма перед отправкой
            if (snapshot.role() != NodeRole.LEADER) {
                stopHeartbeatsLocked();
                return;
            }
            req = new AppendEntriesRequest(snapshot.currentTerm(), raftProperties.getNodeId());
            peers = rpcClient.getPeers();
        }

        updateMdc();
        try {
            int sendTerm = req.getTerm();
            int entriesCount = req.getEntries() == null ? 0 : req.getEntries().size();
            if (!peers.isEmpty()) {
                log.info("AppendEntries sent term={} entries={} peers={}", sendTerm, entriesCount, peers.size());
            }
            for (String peer : peers) {
                rpcClient.appendEntries(peer, req).thenAccept(resp -> {
                    if (resp == null) {
                        return;
                    }
                    handleAppendEntriesResponse(resp, sendTerm);
                });
            }
        } finally {
            clearMdc();
        }
    }

    private void handleAppendEntriesResponse(AppendEntriesResponse resp, int leaderTermAtSend) {
        updateMdc();
        try {
            synchronized (nodeLock) {
                RaftStateSnapshot snapshot = raftState.snapshot();
                // (5) ответ учитывается только для актуального лидерства/терма
                if (snapshot.role() != NodeRole.LEADER || snapshot.currentTerm() != leaderTermAtSend) {
                    return;
                }
                if (resp.getTerm() > snapshot.currentTerm()) {
                    log.info("stepping down (outgoing AppendEntries response has higher term) newTerm={}", resp.getTerm());
                    raftState.updateTermAndVote(resp.getTerm(), null);
                    becomeFollowerLocked(null, true);
                }
            }
        } finally {
            clearMdc();
        }
    }

    // ---------------------------------------------------------------- helpers

    /**
     * Переводит узел в {@code FOLLOWER} с указанным лидером и останавливает heartbeats.
     * Терм меняется только через атомарные операции {@link RaftState}.
     */
    private void becomeFollowerLocked(String leaderId, boolean resetTimer) {
        raftState.setRole(NodeRole.FOLLOWER);
        raftState.setLeaderId(leaderId);
        stopHeartbeatsLocked();
        if (resetTimer) {
            electionTimer.reset();
        }
    }

    private void updateMdc() {
        try {
            if (raftProperties != null) {
                MDC.put("nodeId", raftProperties.getNodeId());
            }
            RaftStateSnapshot snapshot = raftState.snapshot();
            MDC.put("term", String.valueOf(snapshot.currentTerm()));
            if (snapshot.role() != null) {
                MDC.put("role", String.valueOf(snapshot.role()));
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
        synchronized (nodeLock) {
            electionTimer.stop();
            stopHeartbeatsLocked();
            heartbeatScheduler.shutdownNow();
        }
    }
}
