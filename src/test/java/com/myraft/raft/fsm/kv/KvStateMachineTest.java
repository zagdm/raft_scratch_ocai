package com.myraft.raft.fsm.kv;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.NullNode;
import com.myraft.raft.fsm.ApplyResult;
import com.myraft.raft.fsm.Command;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class KvStateMachineTest {

    private static final JsonNodeFactory JSON = JsonNodeFactory.instance;

    @Test
    void putReturnsPreviousAndNewValueWithIndex() {
        KvStateMachine sm = new KvStateMachine();

        ApplyResult<JsonNode> first = sm.apply(1, new PutCommand("k", JSON.textNode("v1")));
        assertEquals(JSON.textNode("v1"), first.value());
        assertEquals(JSON.textNode("v1"), sm.get("k"));
        assertNull(first.previousValue());
        assertEquals(1, first.index());

        ApplyResult<JsonNode> second = sm.apply(2, new PutCommand("k", JSON.numberNode(42)));
        assertEquals(JSON.textNode("v1"), second.previousValue());
        assertEquals(JSON.numberNode(42), second.value());
        assertEquals(JSON.numberNode(42), sm.get("k"));
        assertEquals(2, second.index());
    }

    @Test
    void supportsAllJsonValueTypes() {
        KvStateMachine sm = new KvStateMachine();
        List<JsonNode> values = List.of(
                JSON.textNode("string"),
                JSON.numberNode(7),
                JSON.arrayNode().add(1).add("two"),
                JSON.objectNode().put("nested", true),
                JSON.nullNode());

        for (int i = 0; i < values.size(); i++) {
            long index = i + 1L;
            ApplyResult<JsonNode> result = sm.apply(index, new PutCommand("key-" + i, values.get(i)));
            assertEquals(values.get(i), result.value(), "value type at index " + index);
            assertEquals(values.get(i), sm.get("key-" + i));
        }
    }

    @Test
    void javaNullValueIsStoredAsJsonNull() {
        PutCommand command = new PutCommand("k", null);

        assertEquals(NullNode.instance, command.value());
    }

    @Test
    void sameCommandSequenceProducesSameResults() {
        List<Command> sequence = List.of(
                new PutCommand("a", JSON.textNode("1")),
                new PutCommand("b", JSON.numberNode(2)),
                new PutCommand("a", JSON.textNode("3")));
        KvStateMachine first = new KvStateMachine();
        KvStateMachine second = new KvStateMachine();
        List<ApplyResult<JsonNode>> firstResults = applyAll(first, sequence);
        List<ApplyResult<JsonNode>> secondResults = applyAll(second, sequence);

        assertEquals(firstResults, secondResults);
        assertEquals(JSON.textNode("3"), first.get("a"));
        assertEquals(JSON.textNode("3"), second.get("a"));
    }

    @Test
    void rejectsUnknownCommandType() {
        KvStateMachine sm = new KvStateMachine();
        Command unknown = new Command() { };

        assertThrows(IllegalArgumentException.class, () -> sm.apply(1, unknown));
    }

    private static List<ApplyResult<JsonNode>> applyAll(KvStateMachine sm, List<Command> commands) {
        List<ApplyResult<JsonNode>> results = new ArrayList<>();
        for (int i = 0; i < commands.size(); i++) {
            results.add(sm.apply(i + 1L, commands.get(i)));
        }
        return results;
    }
}
