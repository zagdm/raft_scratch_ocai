package com.myraft.api.dto;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class StatusResponseJsonTest {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Test
    void leaderFlagIsSerializedAsIsLeader() throws Exception {
        StatusResponse response = new StatusResponse(
                "node-1", "LEADER", 5, "node-1",
                List.of("http://localhost:8081"), true, "10s");

        JsonNode json = objectMapper.readTree(objectMapper.writeValueAsString(response));

        assertTrue(json.has("isLeader"), "JSON must contain isLeader");
        assertTrue(json.get("isLeader").asBoolean());
        assertFalse(json.has("leader"), "JSON must not leak the Jackson default 'leader' name");
    }
}
