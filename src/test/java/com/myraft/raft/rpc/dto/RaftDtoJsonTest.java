package com.myraft.raft.rpc.dto;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.TextNode;
import com.myraft.raft.fsm.kv.KvCommandsModule;
import com.myraft.raft.fsm.kv.PutCommand;
import com.myraft.raft.model.LogEntry;
import com.myraft.raft.model.UserCommand;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * DTO должны сохранять новые поля при round-trip: это подготовка к репликации лога.
 */
class RaftDtoJsonTest {

    private final ObjectMapper mapper = new ObjectMapper().registerModule(new KvCommandsModule());

    @Test
    void requestVoteCarriesLogMetadata() throws Exception {
        RequestVoteRequest request = new RequestVoteRequest(7, "cand", 4, 3);

        String json = mapper.writeValueAsString(request);
        assertTrue(json.contains("\"lastLogIndex\":4"));
        assertTrue(json.contains("\"lastLogTerm\":3"));

        RequestVoteRequest restored = mapper.readValue(json, RequestVoteRequest.class);
        assertEquals(7, restored.getTerm());
        assertEquals("cand", restored.getCandidateId());
        assertEquals(4, restored.getLastLogIndex());
        assertEquals(3, restored.getLastLogTerm());
    }

    @Test
    void appendEntriesCarriesEntriesAndCommitMetadata() throws Exception {
        AppendEntriesRequest request = new AppendEntriesRequest(7, "leader", 1, 3,
                List.of(new LogEntry(2, 7,
                        new UserCommand(new PutCommand("key", TextNode.valueOf("value"))))), 2);

        AppendEntriesRequest restored = mapper.readValue(
                mapper.writeValueAsString(request), AppendEntriesRequest.class);

        assertEquals(7, restored.getTerm());
        assertEquals("leader", restored.getLeaderId());
        assertEquals(1, restored.getPrevLogIndex());
        assertEquals(3, restored.getPrevLogTerm());
        assertEquals(2, restored.getLeaderCommit());
        assertEquals(1, restored.getEntries().size());
        assertEquals(2, restored.getEntries().get(0).index());
        UserCommand command = assertInstanceOf(UserCommand.class, restored.getEntries().get(0).command());
        PutCommand payload = assertInstanceOf(PutCommand.class, command.payload());
        assertEquals("key", payload.key());
        assertEquals(TextNode.valueOf("value"), payload.value());
    }

    @Test
    void appendEntriesResponseCarriesLastIndexAndConflictFields() throws Exception {
        AppendEntriesResponse response = new AppendEntriesResponse(7, false, 5, 3, 2);

        AppendEntriesResponse restored = mapper.readValue(
                mapper.writeValueAsString(response), AppendEntriesResponse.class);

        assertEquals(7, restored.getTerm());
        assertFalse(restored.isSuccess());
        assertEquals(5, restored.getLastIndex());
        assertEquals(3, restored.getConflictTerm());
        assertEquals(2, restored.getConflictIndex());
    }
}
