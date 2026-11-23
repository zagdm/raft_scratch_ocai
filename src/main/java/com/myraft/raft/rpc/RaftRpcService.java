package com.myraft.raft.rpc;

import com.myraft.config.RaftProperties;
import com.myraft.raft.RaftNode;
import com.myraft.raft.model.AppendEntriesEvent;
import com.myraft.raft.model.AppendEntriesOutcome;
import com.myraft.raft.model.NodeRole;
import com.myraft.raft.model.RaftLog;
import com.myraft.raft.model.RaftState;
import com.myraft.raft.model.RaftState.RaftStateSnapshot;
import com.myraft.raft.model.RequestVoteEvent;
import com.myraft.raft.model.RequestVoteOutcome;
import com.myraft.raft.model.StateTransition;
import com.myraft.raft.rpc.dto.AppendEntriesRequest;
import com.myraft.raft.rpc.dto.AppendEntriesResponse;
import com.myraft.raft.rpc.dto.RequestVoteRequest;
import com.myraft.raft.rpc.dto.RequestVoteResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.stereotype.Service;

import java.util.Objects;

/**
 * Тонкий адаптер RPC → {@link RaftNode}.
 * <p>
 * Сервис делает безопасный prefilter по снапшоту, вызывает узел, уточняет событие
 * {@code GRANTED_REPEAT}, логирует outcome и собирает ответ из {@code after}-снапшота.
 * Живое состояние после вызова узла не перечитывается; единственный эффект сервиса —
 * сброс election timer при повторном голосе, который узел выполняет под своим локом.
 */
@Service
public class RaftRpcService {
    private static final Logger log = LoggerFactory.getLogger(RaftRpcService.class);

    private final RaftNode raftNode;
    private final RaftState raftState;
    private final RaftProperties raftProperties;

    public RaftRpcService(RaftNode raftNode, RaftState raftState, RaftProperties raftProperties) {
        this.raftNode = raftNode;
        this.raftState = raftState;
        this.raftProperties = raftProperties;
    }

    public RequestVoteResponse handleRequestVote(RequestVoteRequest request) {
        RaftStateSnapshot pre = raftState.snapshot();
        updateMdc(pre);
        try {
            log.debug("RequestVote received <- {} term={}", request.getCandidateId(), request.getTerm());

            // Prefilter по снапшоту: случаи, монотонно неприменимые к узлу.
            // При term > currentTerm узел всё равно вызывается, чтобы шагнуть вниз по §5.1.
            RequestVoteOutcome outcome;
            if (request.getTerm() < pre.currentTerm()) {
                outcome = new RequestVoteOutcome(RequestVoteEvent.REJECTED_STALE_TERM,
                        new StateTransition(pre, pre));
            } else if (request.getTerm() == pre.currentTerm()
                    && pre.votedFor() != null
                    && !pre.votedFor().equals(request.getCandidateId())) {
                outcome = new RequestVoteOutcome(RequestVoteEvent.REJECTED_ALREADY_VOTED,
                        new StateTransition(pre, pre));
            } else if (request.getTerm() == pre.currentTerm()
                    && !RaftLog.isAtLeastAsUpToDate(request.getLastLogTerm(), request.getLastLogIndex(),
                            pre.lastLogTerm(), pre.lastLogIndex())) {
                outcome = new RequestVoteOutcome(RequestVoteEvent.REJECTED_LOG_NOT_UP_TO_DATE,
                        new StateTransition(pre, pre));
            } else {
                outcome = raftNode.handleRequestVote(request);
            }

            // Повторный голос тому же кандидату — решение сервиса по before/after узла.
            outcome = resolveRepeat(request, outcome);
            if (outcome.event() == RequestVoteEvent.GRANTED_REPEAT) {
                raftNode.resetElectionTimerIfUnchanged(outcome.state().after());
            }

            RaftStateSnapshot after = outcome.state().after();
            updateMdc(after);
            logRequestVoteDecision(request, outcome);

            RequestVoteResponse response = new RequestVoteResponse(after.currentTerm(), outcome.granted());
            log.debug("RequestVote response sent -> {} term={} granted={}",
                    request.getCandidateId(), response.getTerm(), response.isVoteGranted());
            return response;
        } finally {
            clearMdc();
        }
    }

    public AppendEntriesResponse handleAppendEntries(AppendEntriesRequest request) {
        RaftStateSnapshot pre = raftState.snapshot();
        updateMdc(pre);
        try {
            int entriesCount = request.getEntries() == null ? 0 : request.getEntries().size();
            log.info("AppendEntries received <- {} term={} entries={}",
                    request.getLeaderId(), request.getTerm(), entriesCount);

            // Prefilter по снапшоту: stale term и чужой лидер в том же терме.
            AppendEntriesOutcome outcome = request.getTerm() < pre.currentTerm()
                    ? new AppendEntriesOutcome(AppendEntriesEvent.REJECTED_STALE_TERM, new StateTransition(pre, pre))
                    : request.getTerm() == pre.currentTerm()
                            && pre.leaderId() != null
                            && !pre.leaderId().equals(request.getLeaderId())
                            ? new AppendEntriesOutcome(AppendEntriesEvent.REJECTED_FOREIGN_LEADER,
                                    new StateTransition(pre, pre))
                            : raftNode.handleAppendEntries(request);

            RaftStateSnapshot after = outcome.state().after();
            updateMdc(after);
            logAppendEntriesDecision(request, outcome);

            AppendEntriesOutcome.Details details = outcome.details();
            AppendEntriesResponse response = new AppendEntriesResponse(after.currentTerm(), outcome.accepted(),
                    after.lastLogIndex(), details.conflictTerm(), details.conflictIndex());
            log.debug("AppendEntries response sent -> {} term={} success={} lastIndex={}",
                    request.getLeaderId(), response.getTerm(), response.isSuccess(), response.getLastIndex());
            return response;
        } finally {
            clearMdc();
        }
    }

    /**
     * Узел возвращает {@code GRANTED} и для первого голоса, и для повторного;
     * различает их сервис по {@code before.votedFor} из outcome.
     */
    private static RequestVoteOutcome resolveRepeat(RequestVoteRequest request, RequestVoteOutcome outcome) {
        if (outcome.event() == RequestVoteEvent.GRANTED
                && Objects.equals(outcome.state().before().votedFor(), request.getCandidateId())) {
            return new RequestVoteOutcome(RequestVoteEvent.GRANTED_REPEAT, outcome.state());
        }
        return outcome;
    }

    private void logRequestVoteDecision(RequestVoteRequest request, RequestVoteOutcome outcome) {
        StateTransition state = outcome.state();
        String candidate = request.getCandidateId();
        int term = state.after().currentTerm();
        switch (outcome.event()) {
            case GRANTED -> log.info("vote granted (to candidate) {} term={}", candidate, term);
            case GRANTED_REPEAT -> log.info("vote granted (repeat, same candidate) {} term={}", candidate, term);
            case GRANTED_TERM_ADVANCED -> {
                log.info("stepping down (incoming RequestVote has higher term) newTerm={} from={}", term, candidate);
                log.info("vote granted (to candidate) {} term={} (was {})", candidate, term, state.before().currentTerm());
            }
            case REJECTED_STALE_TERM -> log.debug(
                    "vote rejected (stale term, incoming RequestVote) from={} theirTerm={} term={}",
                    candidate, request.getTerm(), term);
            case REJECTED_ALREADY_VOTED -> log.info(
                    "vote rejected (already voted / condition failed) to {} term={}", candidate, term);
            case REJECTED_LOG_NOT_UP_TO_DATE -> log.info(
                    "vote rejected (log not up-to-date) from={} candidateLastLogTerm={} candidateLastLogIndex={}"
                            + " localLastLogTerm={} localLastLogIndex={} term={}",
                    candidate, request.getLastLogTerm(), request.getLastLogIndex(),
                    state.before().lastLogTerm(), state.before().lastLogIndex(), term);
        }
    }

    private void logAppendEntriesDecision(AppendEntriesRequest request, AppendEntriesOutcome outcome) {
        StateTransition state = outcome.state();
        int term = state.after().currentTerm();
        String leader = request.getLeaderId();
        AppendEntriesOutcome.Details details = outcome.details();

        if (details.truncated()) {
            log.info("log conflict truncated from={} appended={} term={}",
                    details.lastNewIndex() - details.appended() + 1, details.appended(), term);
        }
        if (details.appended() > 0) {
            log.info("log entry appended count={} lastNewIndex={} from={} term={}",
                    details.appended(), details.lastNewIndex(), leader, term);
        }

        switch (outcome.event()) {
            case ACCEPTED_TERM_ADVANCED -> log.info(
                    "stepping down (incoming AppendEntries has higher term) newTerm={} from={}", term, leader);
            case ACCEPTED_LEADER_DISCOVERED -> {
                if (state.before().role() != NodeRole.FOLLOWER) {
                    log.info("discovered new leader {} term={}", leader, term);
                }
                if (!Objects.equals(state.before().leaderId(), leader)) {
                    log.info("leader changed to {} term={}", leader, term);
                }
            }
            case ACCEPTED_IDEMPOTENT -> log.debug("heartbeat accepted (no state change) from={} term={}", leader, term);
            case ACCEPTED_LOG_APPENDED, ACCEPTED_LOG_TRUNCATED -> log.debug(
                    "log entries accepted from={} term={} count={}", leader, term, details.appended());
            case REJECTED_LOG_MISMATCH -> log.info(
                    "log mismatch rejected from={} term={} prevLogIndex={} prevLogTerm={}"
                            + " localLastLogIndex={} localLastLogTerm={}",
                    leader, term, request.getPrevLogIndex(), request.getPrevLogTerm(),
                    state.after().lastLogIndex(), state.after().lastLogTerm());
            case REJECTED_STALE_TERM -> log.debug(
                    "heartbeat ignored (incoming AppendEntries stale term) from={} theirTerm={} term={}",
                    leader, request.getTerm(), term);
            case REJECTED_FOREIGN_LEADER -> log.debug(
                    "heartbeat ignored (foreign leader at same term) from={} currentLeader={} term={}",
                    leader, state.before().leaderId(), term);
        }
    }

    private void updateMdc(RaftStateSnapshot snapshot) {
        try {
            if (raftProperties != null) {
                MDC.put("nodeId", raftProperties.getNodeId());
            }
            if (snapshot != null) {
                MDC.put("term", String.valueOf(snapshot.currentTerm()));
                if (snapshot.role() != null) {
                    MDC.put("role", String.valueOf(snapshot.role()));
                }
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
}
