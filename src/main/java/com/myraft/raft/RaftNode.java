package com.myraft.raft;

import com.myraft.config.RaftProperties;
import com.myraft.raft.fsm.ApplyResult;
import com.myraft.raft.fsm.Command;
import com.myraft.raft.fsm.StateMachine;
import com.myraft.raft.model.AppendEntriesEvent;
import com.myraft.raft.model.AppendEntriesOutcome;
import com.myraft.raft.model.LogEntry;
import com.myraft.raft.model.NoOpCommand;
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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
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

    /** Результаты apply по индексу; ожидающие клиенты снимаются через {@link #awaitApplied}. */
    private final Map<Long, ApplyResult<?>> appliedResults = new HashMap<>();

    /** Ожидающие apply клиенты: один future на индекс, снимается под {@code nodeLock}. */
    private final Map<Long, CompletableFuture<ApplyResult<?>>> pendingApplies = new HashMap<>();

    /**
     * Состояние репликации лидера: только под {@link #nodeLock}, сбрасывается при
     * потере лидерства и инициализируется заново при каждом переходе в LEADER.
     */
    private final Map<String, Long> nextIndex = new HashMap<>();
    private final Map<String, Long> matchIndex = new HashMap<>();

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
     * Обрабатывает входящий {@code AppendEntries}: heartbeat и репликацию лога.
     * Терм и лидер принимаются по §5.1, затем проверяется согласованность лога по §5.3.
     * Все мутации выполняются под {@code nodeLock}.
     */
    public AppendEntriesOutcome handleAppendEntries(AppendEntriesRequest request) {
        synchronized (nodeLock) {
            RaftStateSnapshot before = raftState.snapshot();
            if (request.getTerm() < before.currentTerm()) {
                return new AppendEntriesOutcome(AppendEntriesEvent.REJECTED_STALE_TERM,
                        new StateTransition(before, before));
            }
            // При равном терме уже известный лидер не сменяется: второй лидер в том же
            // терме — признак рассинхрона/старого сообщения, запрос не принимаем.
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

            RaftLog localLog = raftState.logSnapshot();
            long prevLogIndex = request.getPrevLogIndex();
            int prevLogTerm = request.getPrevLogTerm();
            if (prevLogIndex > localLog.lastIndex()
                    || (prevLogIndex > 0 && localLog.termAt(prevLogIndex) != prevLogTerm)) {
                int conflictTerm = prevLogIndex <= localLog.lastIndex()
                        ? localLog.termAt(prevLogIndex) : 0;
                long conflictIndex = conflictTerm > 0
                        ? firstIndexWithTerm(localLog, conflictTerm) : localLog.lastIndex() + 1;
                RaftStateSnapshot after = raftState.snapshot();
                return new AppendEntriesOutcome(AppendEntriesEvent.REJECTED_LOG_MISMATCH,
                        new StateTransition(before, after),
                        new AppendEntriesOutcome.Details(0, false, 0, conflictTerm, conflictIndex));
            }

            List<LogEntry> entries = request.getEntries() == null ? List.of() : request.getEntries();
            AppendEntriesMutation mutation = appendEntriesLocked(prevLogIndex, entries);
            advanceFollowerCommitLocked(request.getLeaderCommit(), mutation.lastNewIndex());

            AppendEntriesEvent event;
            if (termAdvanced) {
                event = AppendEntriesEvent.ACCEPTED_TERM_ADVANCED;
            } else if (roleChanged || leaderChanged) {
                event = AppendEntriesEvent.ACCEPTED_LEADER_DISCOVERED;
            } else if (mutation.appended() > 0) {
                event = mutation.truncated()
                        ? AppendEntriesEvent.ACCEPTED_LOG_TRUNCATED
                        : AppendEntriesEvent.ACCEPTED_LOG_APPENDED;
            } else {
                event = AppendEntriesEvent.ACCEPTED_IDEMPOTENT;
            }

            RaftStateSnapshot after = raftState.snapshot();
            return new AppendEntriesOutcome(event, new StateTransition(before, after),
                    new AppendEntriesOutcome.Details(mutation.appended(), mutation.truncated(),
                            mutation.lastNewIndex(), 0, 0));
        }
    }

    /** Копия лога после применения батча: число новых записей, факт truncate и последний индекс. */
    private record AppendEntriesMutation(int appended, boolean truncated, long lastNewIndex) {
    }

    /**
     * Дописывает записи после проверенного {@code prevLog*}. Записи с тем же индексом
     * и термом считаются уже реплицированными; первый же конфликт удаляет хвост лога.
     */
    private AppendEntriesMutation appendEntriesLocked(long prevLogIndex, List<LogEntry> entries) {
        RaftLog current = raftState.logSnapshot();
        // По умолчанию все записи уже реплицированы; firstNew сдвигается только
        // на первую отсутствующую/конфликтующую запись (идемпотентность повторов).
        int firstNew = entries.size();
        boolean truncated = false;

        for (int i = 0; i < entries.size(); i++) {
            LogEntry incoming = entries.get(i);
            long expectedIndex = prevLogIndex + 1L + i;
            if (incoming.index() != expectedIndex) {
                throw new IllegalArgumentException("AppendEntries batch must be contiguous: expected index "
                        + expectedIndex + " but got " + incoming.index());
            }
            LogEntry existing = current.entryAt(expectedIndex);
            if (existing == null) {
                firstNew = i;
                break;
            }
            if (existing.term() != incoming.term()) {
                current = current.truncateFrom(expectedIndex);
                raftState.truncateLogFrom(expectedIndex);
                truncated = true;
                firstNew = i;
                break;
            }
        }

        List<LogEntry> toAppend = entries.subList(firstNew, entries.size());
        raftState.appendLogEntries(toAppend);
        long lastNewIndex = entries.isEmpty()
                ? raftState.lastLogIndex() : entries.get(entries.size() - 1).index();
        return new AppendEntriesMutation(toAppend.size(), truncated, lastNewIndex);
    }

    /** Верхняя граница коммита от лидера: последняя новая запись или конец лога для heartbeat. */
    private void advanceFollowerCommitLocked(long leaderCommit, long lastNewIndex) {
        if (leaderCommit <= raftState.getCommitIndex()) {
            return;
        }
        long oldCommit = raftState.getCommitIndex();
        raftState.advanceCommitIndex(Math.min(leaderCommit, lastNewIndex));
        long newCommit = raftState.getCommitIndex();
        if (newCommit > oldCommit) {
            log.info("commitIndex advanced from={} to={} leaderCommit={}", oldCommit, newCommit, leaderCommit);
            applyCommittedSafelyLocked();
        }
    }

    private static long firstIndexWithTerm(RaftLog snapshot, int term) {
        for (LogEntry entry : snapshot.entries()) {
            if (entry.term() == term) {
                return entry.index();
            }
        }
        return snapshot.lastIndex() + 1;
    }

    // ---------------------------------------------------------------- apply

    /**
     * Применяет закоммиченные записи к state machine под {@code nodeLock}.
     * Ошибка apply не гасит RPC/колбэк: указатель остаётся на месте, повтор будет
     * сделан при следующем изменении {@code commitIndex}.
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

    /** Результат apply непрозрачен для ядра; API-адаптер конкретизирует его в JSON. */
    public ApplyResult<?> appliedResult(long index) {
        synchronized (nodeLock) {
            return appliedResults.get(index);
        }
    }

    /**
     * Ждёт, пока запись с индексом {@code index} будет применена к state machine.
     * <p>
     * Регистрация future и чтение уже готового результата выполняются под
     * {@code nodeLock}; само ожидание — вне лока, чтобы не блокировать Raft-переходы.
     * Таймаут снимает свою регистрацию, поэтому поздний apply не уведомляет старый запрос.
     */
    public ApplyResult<?> awaitApplied(long index, long timeoutMs) {
        if (index < 1) {
            throw new IllegalArgumentException("index must be positive");
        }
        CompletableFuture<ApplyResult<?>> future;
        synchronized (nodeLock) {
            ApplyResult<?> ready = appliedResults.get(index);
            if (ready != null) {
                return ready;
            }
            future = new CompletableFuture<>();
            pendingApplies.put(index, future);
        }
        try {
            return future.get(timeoutMs, TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            cancelAwait(index, future);
            throw new ApplyTimeoutException("apply timeout index=" + index, e);
        } catch (InterruptedException e) {
            cancelAwait(index, future);
            Thread.currentThread().interrupt();
            throw new IllegalStateException("apply interrupted index=" + index, e);
        } catch (ExecutionException e) {
            throw new IllegalStateException("apply failed index=" + index, e.getCause());
        }
    }

    /** Снимает регистрацию ожидающего запроса (для отмены по таймауту). */
    public void cancelAwait(long index) {
        synchronized (nodeLock) {
            pendingApplies.remove(index);
        }
    }

    private void cancelAwait(long index, CompletableFuture<?> future) {
        synchronized (nodeLock) {
            pendingApplies.remove(index, future);
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
                CompletableFuture<ApplyResult<?>> pending = pendingApplies.remove(next);
                if (pending != null) {
                    pending.complete(result);
                }
                log.info("entry applied index={} lastApplied={}", next, next);
            } else {
                // NoOpCommand: Raft-маркер, state machine не вызывается.
                log.debug("no-op applied index={} lastApplied={}", next, next);
            }
            raftState.advanceLastApplied(next);
            lastApplied = next;
        }
    }

    private void applyCommittedSafelyLocked() {
        try {
            applyCommittedLocked();
        } catch (RuntimeException e) {
            log.error("apply failed commitIndex={} lastApplied={}",
                    raftState.getCommitIndex(), raftState.getLastApplied(), e);
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
        initReplicationLocked();
        appendNoOpLocked(snapshot.currentTerm());
        maybeCommitLocked();
        startHeartbeatsLocked();
        return true;
    }

    private void initReplicationLocked() {
        nextIndex.clear();
        matchIndex.clear();
        // nextIndex указывает на первую запись, которой у пира, скорее всего, нет:
        // NoOp лидера будет дописан сразу после этой инициализации.
        long firstMissing = raftState.lastLogIndex() + 1;
        for (String peer : rpcClient.getPeers()) {
            nextIndex.put(peer, firstMissing);
            matchIndex.put(peer, 0L);
        }
    }

    private void appendNoOpLocked(int term) {
        long index = raftState.lastLogIndex() + 1;
        raftState.appendLogEntry(new LogEntry(index, term, new NoOpCommand()));
        log.info("no-op entry appended index={} term={}", index, term);
    }

    /**
     * Добавляет пользовательскую команду в лог лидера (ядро Raft без REST-адаптера).
     *
     * @return индекс новой записи или {@code -1}, если узел не лидер
     */
    public long appendUserCommand(Command command) {
        Objects.requireNonNull(command, "command");
        updateMdc();
        try {
            synchronized (nodeLock) {
                if (raftState.getRole() != NodeRole.LEADER) {
                    return -1;
                }
                int term = raftState.getCurrentTerm();
                long index = raftState.lastLogIndex() + 1;
                raftState.appendLogEntry(new LogEntry(index, term, new UserCommand(command)));
                log.info("log entry appended index={} term={} type=user", index, term);
                maybeCommitLocked();
                return index;
            }
        } finally {
            clearMdc();
        }
    }

    /**
     * Коммит только записей текущего терма: старые записи покрываются косвенно,
     * когда большинство реплицировало NoOp/новую запись этого терма (§5.4.2).
     */
    private void maybeCommitLocked() {
        if (raftState.getRole() != NodeRole.LEADER) {
            return;
        }
        RaftLog snapshot = raftState.logSnapshot();
        List<String> peers = rpcClient.getPeers();
        int majority = majority(peers.size() + 1);
        int currentTerm = raftState.getCurrentTerm();

        for (long n = snapshot.lastIndex(); n > raftState.getCommitIndex(); n--) {
            if (snapshot.termAt(n) != currentTerm) {
                continue;
            }
            int replicas = 1; // сам лидер уже держит запись в своём логе
            for (String peer : peers) {
                Long matched = matchIndex.get(peer);
                if (matched != null && matched >= n) {
                    replicas++;
                }
            }
            if (replicas < majority) {
                continue;
            }
            long oldCommit = raftState.getCommitIndex();
            raftState.advanceCommitIndex(n);
            if (raftState.getCommitIndex() > oldCommit) {
                log.info("commitIndex advanced from={} to={} term={}", oldCommit,
                        raftState.getCommitIndex(), currentTerm);
                applyCommittedSafelyLocked();
            }
            return;
        }
    }

    // ---------------------------------------------------------------- replication

    private void startHeartbeatsLocked() {
        stopHeartbeatsLocked();
        long interval = raftProperties.getHeartbeatIntervalMs();
        heartbeatTask = heartbeatScheduler.scheduleWithFixedDelay(
                this::sendAppendEntries, 0, interval, TimeUnit.MILLISECONDS);
    }

    private void stopHeartbeatsLocked() {
        ScheduledFuture<?> task = heartbeatTask;
        heartbeatTask = null;
        if (task != null) {
            task.cancel(false);
        }
    }

    /** Параметры одного адресата: у каждого пира свой {@code nextIndex}. */
    private record ReplicationTarget(String peer, int term, long prevLogIndex, int prevLogTerm,
                                     List<LogEntry> entries, long leaderCommit) {
    }

    /** Внеочередной раунд репликации; сеть всегда выполняется вне {@code nodeLock}. */
    public void replicateNow() {
        sendAppendEntries();
    }

    private void sendAppendEntries() {
        List<ReplicationTarget> targets = new ArrayList<>();
        int maxEntries;
        long lastLogIndex;
        synchronized (nodeLock) {
            RaftStateSnapshot snapshot = raftState.snapshot();
            // Актуальность лидера/терма перед отправкой.
            if (snapshot.role() != NodeRole.LEADER) {
                stopHeartbeatsLocked();
                return;
            }
            RaftLog currentLog = raftState.logSnapshot();
            long leaderCommit = raftState.getCommitIndex();
            int sendTerm = snapshot.currentTerm();
            for (String peer : rpcClient.getPeers()) {
                long next = Math.min(nextIndex.getOrDefault(peer, currentLog.lastIndex() + 1),
                        currentLog.lastIndex() + 1);
                long prevLogIndex = next - 1;
                int prevLogTerm = currentLog.termAt(prevLogIndex);
                List<LogEntry> entries = currentLog.entries().subList(
                        (int) prevLogIndex, (int) currentLog.lastIndex());
                targets.add(new ReplicationTarget(peer, sendTerm, prevLogIndex, prevLogTerm,
                        entries, leaderCommit));
            }
            maxEntries = targets.stream().mapToInt(t -> t.entries().size()).max().orElse(0);
            lastLogIndex = currentLog.lastIndex();
        }

        updateMdc();
        try {
            if (!targets.isEmpty()) {
                log.info("AppendEntries sent term={} peers={} entries={} leaderCommit={} lastLogIndex={}",
                        targets.get(0).term(), targets.size(), maxEntries,
                        targets.get(0).leaderCommit(), lastLogIndex);
            }
            for (ReplicationTarget target : targets) {
                AppendEntriesRequest req = new AppendEntriesRequest(target.term(), raftProperties.getNodeId(),
                        target.prevLogIndex(), target.prevLogTerm(), target.entries(), target.leaderCommit());
                rpcClient.appendEntries(target.peer(), req).thenAccept(resp -> {
                    if (resp != null) {
                        handleAppendEntriesResponse(target, resp);
                    }
                });
            }
        } finally {
            clearMdc();
        }
    }

    private void handleAppendEntriesResponse(ReplicationTarget target, AppendEntriesResponse resp) {
        updateMdc();
        try {
            synchronized (nodeLock) {
                RaftStateSnapshot snapshot = raftState.snapshot();
                // Step-down по более высокому терму имеет приоритет над проверкой контекста.
                if (resp.getTerm() > snapshot.currentTerm()) {
                    log.info("stepping down (outgoing AppendEntries response has higher term) newTerm={} from={}",
                            resp.getTerm(), target.peer());
                    raftState.updateTermAndVote(resp.getTerm(), null);
                    becomeFollowerLocked(null, true);
                    return;
                }
                if (snapshot.role() != NodeRole.LEADER || snapshot.currentTerm() != target.term()) {
                    return;
                }

                long oldNext = nextIndex.getOrDefault(target.peer(), target.prevLogIndex() + 1);
                if (resp.isSuccess()) {
                    // Resp-индекс пира может быть больше отправленного: он мог сохранить
                    // совпадающий хвост. В кворум учитываем только то, что есть у лидера.
                    long sentLastIndex = target.prevLogIndex() + target.entries().size();
                    long match = Math.min(resp.getLastIndex(), sentLastIndex);
                    long oldMatch = matchIndex.getOrDefault(target.peer(), 0L);
                    if (match > oldMatch) {
                        matchIndex.put(target.peer(), match);
                    }
                    long newNext = Math.max(oldNext, Math.max(match, sentLastIndex) + 1);
                    nextIndex.put(target.peer(), newNext);
                    log.debug("log replicated to {} matchIndex={} nextIndex={}", target.peer(), match, newNext);
                    maybeCommitLocked();
                } else {
                    long newNext = conflictBasedNextIndex(resp, oldNext);
                    nextIndex.put(target.peer(), newNext);
                    log.debug("nextIndex decremented peer={} from={} to={} prevLogIndex={}",
                            target.peer(), oldNext, newNext, target.prevLogIndex());
                }
            }
        } finally {
            clearMdc();
        }
    }

    /**
     * Откат {@code nextIndex} после отказа: подсказка по терму/индексу конфликта,
     * если пир её прислал, иначе просто на одну запись назад. Никогда не увеличивает индекс.
     */
    private long conflictBasedNextIndex(AppendEntriesResponse resp, long currentNext) {
        long candidate = currentNext - 1;
        if (resp.getConflictTerm() > 0) {
            long lastWithTerm = 0;
            for (LogEntry entry : raftState.logSnapshot().entries()) {
                if (entry.term() == resp.getConflictTerm()) {
                    lastWithTerm = entry.index();
                }
            }
            candidate = lastWithTerm > 0 ? lastWithTerm + 1 : resp.getConflictIndex();
        } else if (resp.getConflictIndex() > 0) {
            candidate = resp.getConflictIndex();
        }
        return Math.max(1, Math.min(candidate, currentNext - 1));
    }

    // ---------------------------------------------------------------- helpers

    /**
     * Переводит узел в {@code FOLLOWER} с указанным лидером и останавливает heartbeats.
     * Терм меняется только через атомарные операции {@link RaftState}.
     */
    private void becomeFollowerLocked(String leaderId, boolean resetTimer) {
        raftState.setRole(NodeRole.FOLLOWER);
        raftState.setLeaderId(leaderId);
        nextIndex.clear();
        matchIndex.clear();
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
