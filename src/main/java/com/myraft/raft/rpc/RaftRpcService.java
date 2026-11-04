package com.myraft.raft.rpc;

import com.myraft.config.RaftProperties;
import com.myraft.raft.model.NodeRole;
import com.myraft.raft.model.RaftState;
import com.myraft.raft.rpc.dto.AppendEntriesRequest;
import com.myraft.raft.rpc.dto.AppendEntriesResponse;
import com.myraft.raft.rpc.dto.RequestVoteRequest;
import com.myraft.raft.rpc.dto.RequestVoteResponse;
import com.myraft.raft.timer.ElectionTimer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.stereotype.Service;

import java.util.Objects;

@Service
public class RaftRpcService {
    private static final Logger log = LoggerFactory.getLogger(RaftRpcService.class);
    private final RaftState raftState;
    private final RaftProperties raftProperties;
    private final ElectionTimer electionTimer;

    public RaftRpcService(RaftState raftState, RaftProperties raftProperties, ElectionTimer electionTimer) {
        this.raftState = raftState;
        this.raftProperties = raftProperties;
        this.electionTimer = electionTimer;
    }

    public RequestVoteResponse handleRequestVote(RequestVoteRequest request) {
        updateMdc();
        try {
            log.debug("RequestVote received <- {} term={}", request.getCandidateId(), request.getTerm());
            int currentTerm = raftState.getCurrentTerm();
            if (request.getTerm() < currentTerm) {
                log.debug("vote rejected (stale term, incoming RequestVote) from={} theirTerm={} term={}",
                        request.getCandidateId(), request.getTerm(), currentTerm);
                log.debug("RequestVote response sent -> {} term={} granted=false", request.getCandidateId(), currentTerm);
                return new RequestVoteResponse(currentTerm, false);
            }
            if (request.getTerm() > currentTerm) {
                log.info("stepping down (incoming RequestVote has higher term) newTerm={} from={}",
                        request.getTerm(), request.getCandidateId());
                raftState.updateTermAndVote(request.getTerm(), null);
                raftState.setRole(NodeRole.FOLLOWER);
                raftState.setLeaderId(null);
                updateMdc();
                electionTimer.reset();
            }
            currentTerm = raftState.getCurrentTerm();
            if (request.getTerm() == currentTerm) {
                String votedFor = raftState.getVotedFor();
                if (votedFor == null || votedFor.equals(request.getCandidateId())) {
                    raftState.updateTermAndVote(currentTerm, request.getCandidateId());
                    electionTimer.reset();
                    log.debug("vote granted (to candidate) {} term={}", request.getCandidateId(), currentTerm);
                    log.debug("RequestVote response sent -> {} term={} granted=true", request.getCandidateId(), currentTerm);
                    return new RequestVoteResponse(currentTerm, true);
                } else {
                    log.debug("vote rejected (already voted / condition failed) to {} (votedFor={})", request.getCandidateId(), votedFor);
                    log.debug("RequestVote response sent -> {} term={} granted=false", request.getCandidateId(), currentTerm);
                    return new RequestVoteResponse(currentTerm, false);
                }
            }
            log.debug("RequestVote response sent -> {} term={} granted=false", request.getCandidateId(), raftState.getCurrentTerm());
            return new RequestVoteResponse(raftState.getCurrentTerm(), false);
        } finally {
            clearMdc();
        }
    }

    public AppendEntriesResponse handleAppendEntries(AppendEntriesRequest request) {
        updateMdc();
        try {
            int currentTerm = raftState.getCurrentTerm();
            int entriesCount = request.getEntries() == null ? 0 : request.getEntries().size();
            log.info("AppendEntries received <- {} term={} entries={}", request.getLeaderId(), request.getTerm(), entriesCount);
            if (request.getTerm() < currentTerm) {
                log.debug("heartbeat ignored (incoming AppendEntries stale term) from={} theirTerm={} term={}",
                        request.getLeaderId(), request.getTerm(), currentTerm);
                log.debug("AppendEntries response sent -> {} term={} success=false", request.getLeaderId(), currentTerm);
                return new AppendEntriesResponse(currentTerm, false);
            }
            if (request.getTerm() > currentTerm) {
                log.info("stepping down (incoming AppendEntries has higher term) newTerm={} from={}",
                        request.getTerm(), request.getLeaderId());
                raftState.updateTermAndVote(request.getTerm(), null);
            }
            if (raftState.getRole() != NodeRole.FOLLOWER) {
                log.info("discovered new leader {} term={}", request.getLeaderId(), request.getTerm());
                raftState.setRole(NodeRole.FOLLOWER);
            }
            String previousLeaderId = raftState.getLeaderId();
            raftState.setLeaderId(request.getLeaderId());
            electionTimer.reset();
            updateMdc();
            if (!Objects.equals(previousLeaderId, request.getLeaderId())) {
                log.info("leader changed to {} term={}",
                        request.getLeaderId(), request.getTerm());
            }
            log.debug("AppendEntries response sent -> {} term={} success=true", request.getLeaderId(), raftState.getCurrentTerm());
            return new AppendEntriesResponse(raftState.getCurrentTerm(), true);
        } finally {
            clearMdc();
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
}
