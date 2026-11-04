package com.myraft.api;

import com.myraft.api.dto.StatusResponse;
import com.myraft.config.RaftProperties;
import com.myraft.raft.model.NodeRole;
import com.myraft.raft.model.RaftState;
import com.myraft.raft.rpc.RaftRpcClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.concurrent.TimeUnit;

@RestController
public class StatusController {
    private static final Logger log = LoggerFactory.getLogger(StatusController.class);
    private final RaftState raftState;
    private final RaftProperties raftProperties;
    private final RaftRpcClient rpcClient;

    public StatusController(RaftState raftState, RaftProperties raftProperties, RaftRpcClient rpcClient) {
        this.raftState = raftState;
        this.raftProperties = raftProperties;
        this.rpcClient = rpcClient;
    }

    @GetMapping("/status")
    public StatusResponse status() {
        updateMdc();
        try {
            NodeRole role = raftState.getRole();
            long uptimeMs = System.currentTimeMillis() - raftState.getStartTime();
            long seconds = TimeUnit.MILLISECONDS.toSeconds(uptimeMs);
            String uptime = seconds + "s";
            log.debug("status request");
            return new StatusResponse(
                    raftProperties.getNodeId(),
                    role.name(),
                    raftState.getCurrentTerm(),
                    raftState.getLeaderId(),
                    rpcClient.getPeers(),
                    role == NodeRole.LEADER,
                    uptime
            );
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
                if (raftState.getRole() != null) {
                    MDC.put("role", String.valueOf(raftState.getRole()));
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
