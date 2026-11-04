package com.myraft.raft.rpc;

import com.myraft.config.ClusterProperties;
import com.myraft.raft.rpc.dto.AppendEntriesRequest;
import com.myraft.raft.rpc.dto.AppendEntriesResponse;
import com.myraft.raft.rpc.dto.RequestVoteRequest;
import com.myraft.raft.rpc.dto.RequestVoteResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestTemplate;

import java.util.concurrent.CompletableFuture;

@Component
public class RaftRpcClient {
    private static final Logger log = LoggerFactory.getLogger(RaftRpcClient.class);
    private final RestTemplate restTemplate;
    private final ClusterProperties clusterProperties;

    public RaftRpcClient(RestTemplate restTemplate, ClusterProperties clusterProperties) {
        this.restTemplate = restTemplate;
        this.clusterProperties = clusterProperties;
    }

    public CompletableFuture<RequestVoteResponse> requestVote(String peer, RequestVoteRequest request) {
        String url = peer + "/raft/requestVote";
        return CompletableFuture.supplyAsync(() -> {
            try {
                log.info("RequestVote sent -> {} term={}", peer, request.getTerm());
                RequestVoteResponse response = restTemplate.postForObject(url, request, RequestVoteResponse.class);
                log.debug("RequestVote response from {} term={} granted={}", peer, response == null ? -1 : response.getTerm(), response == null ? null : response.isVoteGranted());
                return response;
            } catch (RestClientException e) {
                log.trace("vote request failed {}: {}", peer, e.getMessage());
                return null;
            }
        });
    }

    public CompletableFuture<AppendEntriesResponse> appendEntries(String peer, AppendEntriesRequest request) {
        String url = peer + "/raft/appendEntries";
        int entriesCount = request.getEntries() == null ? 0 : request.getEntries().size();
        return CompletableFuture.supplyAsync(() -> {
            try {
                log.info("AppendEntries sent -> {} term={} entries={}", peer, request.getTerm(), entriesCount);
                AppendEntriesResponse response = restTemplate.postForObject(url, request, AppendEntriesResponse.class);
                log.debug("AppendEntries response from {} term={} success={}", peer, response == null ? -1 : response.getTerm(), response == null ? null : response.isSuccess());
                return response;
            } catch (RestClientException e) {
                log.trace("heartbeat failed {}: {}", peer, e.getMessage());
                return null;
            }
        });
    }

    public java.util.List<String> getPeers() {
        return clusterProperties.getPeers();
    }
}
