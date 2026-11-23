package com.myraft.api;

import com.myraft.api.dto.ApiErrorResponse;
import com.myraft.api.dto.AppendRequest;
import com.myraft.api.dto.AppendResponse;
import com.myraft.api.dto.KvReadResponse;
import com.myraft.api.dto.LogEntryResponse;
import com.myraft.raft.ApplyTimeoutException;
import com.myraft.config.RaftProperties;
import com.myraft.raft.model.RaftState;
import com.myraft.raft.model.RaftState.RaftStateSnapshot;
import org.slf4j.MDC;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/**
 * Внутренний клиентский API: запись/чтение лога и чтение KV.
 * MDC выставляется из одного согласованного снапшота на каждый HTTP-запрос.
 */
@RestController
@RequestMapping("/raft")
public class RaftClientController {
    private final RaftClientService clientService;
    private final RaftState raftState;
    private final RaftProperties raftProperties;

    public RaftClientController(RaftClientService clientService, RaftState raftState,
                                RaftProperties raftProperties) {
        this.clientService = clientService;
        this.raftState = raftState;
        this.raftProperties = raftProperties;
    }

    @PostMapping("/log")
    public ResponseEntity<AppendResponse> append(@RequestBody AppendRequest request) {
        RaftStateSnapshot snapshot = raftState.snapshot();
        updateMdc(snapshot);
        try {
            return ResponseEntity.ok(clientService.append(request.getKey(), request.getValue()));
        } finally {
            clearMdc();
        }
    }

    @GetMapping("/log")
    public ResponseEntity<List<LogEntryResponse>> readLog() {
        RaftStateSnapshot snapshot = raftState.snapshot();
        updateMdc(snapshot);
        try {
            return ResponseEntity.ok(clientService.readLog());
        } finally {
            clearMdc();
        }
    }

    @GetMapping("/kv")
    public ResponseEntity<KvReadResponse> readKv(@RequestParam String key) {
        RaftStateSnapshot snapshot = raftState.snapshot();
        updateMdc(snapshot);
        try {
            return ResponseEntity.ok(clientService.readKv(key));
        } finally {
            clearMdc();
        }
    }

    @ExceptionHandler(NotLeaderException.class)
    public ResponseEntity<ApiErrorResponse> notLeader(NotLeaderException e) {
        return errorResponse(HttpStatus.SERVICE_UNAVAILABLE, e.getMessage(), e.getLeaderId());
    }

    @ExceptionHandler(ApplyTimeoutException.class)
    public ResponseEntity<ApiErrorResponse> applyTimeout(ApplyTimeoutException e) {
        return errorResponse(HttpStatus.GATEWAY_TIMEOUT, e.getMessage(), null);
    }

    @ExceptionHandler(IllegalArgumentException.class)
    public ResponseEntity<ApiErrorResponse> badRequest(IllegalArgumentException e) {
        return errorResponse(HttpStatus.BAD_REQUEST, e.getMessage(), null);
    }

    private ResponseEntity<ApiErrorResponse> errorResponse(HttpStatus status, String message, String leaderId) {
        RaftStateSnapshot snapshot = raftState.snapshot();
        updateMdc(snapshot);
        try {
            return ResponseEntity.status(status).body(new ApiErrorResponse(message, leaderId));
        } finally {
            clearMdc();
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
