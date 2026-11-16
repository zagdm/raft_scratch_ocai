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
            RaftState.GrantVoteResult result = raftState.grantVote(request.getTerm(), request.getCandidateId());
            if (result.termIncreased()) {
                log.info("stepping down (incoming RequestVote has higher term) newTerm={} from={}",
                        request.getTerm(), request.getCandidateId());
                raftState.setRole(NodeRole.FOLLOWER);
                raftState.setLeaderId(null);
                updateMdc();
                electionTimer.reset();
            }
            if (result.granted()) {
                electionTimer.reset();
                log.info("vote granted (to candidate) {} term={}", request.getCandidateId(), result.term());
                log.debug("RequestVote response sent -> {} term={} granted=true", request.getCandidateId(), result.term());
                return new RequestVoteResponse(result.term(), true);
            }
            if (request.getTerm() < result.term()) {
                log.debug("vote rejected (stale term, incoming RequestVote) from={} theirTerm={} term={}",
                        request.getCandidateId(), request.getTerm(), result.term());
            } else {
                log.info("vote rejected (already voted / condition failed) to {} term={}", request.getCandidateId(), result.term());
            }
            log.debug("RequestVote response sent -> {} term={} granted=false", request.getCandidateId(), result.term());
            return new RequestVoteResponse(result.term(), false);
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
