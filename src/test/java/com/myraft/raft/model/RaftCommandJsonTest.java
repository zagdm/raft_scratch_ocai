package com.myraft.raft.model;

import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.TextNode;
import com.myraft.raft.fsm.kv.KvCommandsModule;
import com.myraft.raft.fsm.kv.PutCommand;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * RaftCommand-envelope и открытый реестр команд: неизвестные типы — ошибка,
 * старый формат этапа 1 не читается (fail-fast по formatVersion на уровне storage).
 */
class RaftCommandJsonTest {

    private final ObjectMapper mapper = new ObjectMapper().registerModule(new KvCommandsModule());

    @Test
    void userCommandRoundTripKeepsEnvelopeAndPayloadTypes() throws Exception {
        RaftCommand command = new UserCommand(new PutCommand("k", TextNode.valueOf("v")));

        String json = mapper.writeValueAsString(command);
        assertTrue(json.contains("\"type\":\"USER\""), json);
        assertTrue(json.contains("\"payload\""), json);
        assertTrue(json.contains("\"type\":\"PUT\""), json);

        RaftCommand restored = mapper.readValue(json, RaftCommand.class);
        UserCommand user = assertInstanceOf(UserCommand.class, restored);
        PutCommand payload = assertInstanceOf(PutCommand.class, user.payload());
        assertEquals("k", payload.key());
        assertEquals(TextNode.valueOf("v"), payload.value());
    }

    @Test
    void noOpRoundTrip() throws Exception {
        String json = mapper.writeValueAsString(new NoOpCommand());
        assertTrue(json.contains("\"type\":\"NOOP\""), json);

        assertInstanceOf(NoOpCommand.class, mapper.readValue(json, RaftCommand.class));
    }

    @Test
    void unknownEnvelopeTypeFails() {
        assertThrows(JsonMappingException.class,
                () -> mapper.readValue("{\"type\":\"MAGIC\"}", RaftCommand.class));
    }

    @Test
    void unknownPayloadTypeFails() {
        String json = "{\"type\":\"USER\",\"payload\":{\"type\":\"DELETE\",\"key\":\"k\"}}";

        assertThrows(JsonMappingException.class, () -> mapper.readValue(json, RaftCommand.class));
    }

    @Test
    void stageOneCommandFormatIsNotAccepted() {
        // Раньше command был PutCommand/NoOpCommand напрямую, без USER-envelope.
        String oldJson = "{\"type\":\"PUT\",\"key\":\"k\",\"value\":\"v\"}";

        assertThrows(JsonMappingException.class, () -> mapper.readValue(oldJson, RaftCommand.class));
    }
}
