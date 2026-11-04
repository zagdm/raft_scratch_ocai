package com.myraft.raft;

import com.myraft.config.ClusterProperties;
import com.myraft.config.RaftProperties;
import com.myraft.raft.model.NodeRole;
import com.myraft.raft.model.RaftState;
import com.myraft.raft.rpc.RaftRpcClient;
import com.myraft.raft.storage.InMemoryRaftStorage;
import com.myraft.raft.timer.ElectionTimer;
import org.junit.jupiter.api.Test;
import org.springframework.web.client.RestTemplate;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RaftNodeElectionTest {

    @Test
    void singleNodeBecomesLeaderWithoutPeers() throws Exception {
        RaftProperties props = new RaftProperties();
        props.setNodeId("node-1");
        props.setElectionTimeoutMinMs(50);
        props.setElectionTimeoutMaxMs(100);
        props.setHeartbeatIntervalMs(20);

        RaftState state = new RaftState(new InMemoryRaftStorage());
        ElectionTimer timer = new ElectionTimer(props);
        RaftRpcClient rpcClient = new RaftRpcClient(new RestTemplate(), new ClusterProperties());
        RaftNode node = new RaftNode(state, props, rpcClient, timer);

        node.init();
        try {
            long deadline = System.currentTimeMillis() + 2000;
            while (state.getRole() != NodeRole.LEADER && System.currentTimeMillis() < deadline) {
                Thread.sleep(20);
            }
            assertEquals(NodeRole.LEADER, state.getRole(), "single node must elect itself");
            assertEquals("node-1", state.getLeaderId());
            assertTrue(state.getCurrentTerm() >= 1);
        } finally {
            node.shutdown();
        }
    }
}
