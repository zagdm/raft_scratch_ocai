package com.myraft.raft.rpc;

import com.myraft.raft.rpc.dto.AppendEntriesRequest;
import com.myraft.raft.rpc.dto.AppendEntriesResponse;
import com.myraft.raft.rpc.dto.RequestVoteRequest;
import com.myraft.raft.rpc.dto.RequestVoteResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/raft")
public class RaftRpcController {
    private static final Logger log = LoggerFactory.getLogger(RaftRpcController.class);
    private final RaftRpcService rpcService;

    public RaftRpcController(RaftRpcService rpcService) {
        this.rpcService = rpcService;
    }

    @PostMapping("/requestVote")
    public RequestVoteResponse requestVote(@RequestBody RequestVoteRequest request) {
        return rpcService.handleRequestVote(request);
    }

    @PostMapping("/appendEntries")
    public AppendEntriesResponse appendEntries(@RequestBody AppendEntriesRequest request) {
        return rpcService.handleAppendEntries(request);
    }
}
