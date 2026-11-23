package com.myraft.raft.fsm.kv;

import com.fasterxml.jackson.databind.JsonNode;
import com.myraft.raft.fsm.ApplyResult;
import com.myraft.raft.fsm.Command;
import com.myraft.raft.fsm.StateMachine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * KV-конкретизация {@link StateMachine}. Мутации выполняются под {@code nodeLock},
 * ConcurrentHashMap используется для безопасного чтения будущими read-API без лока.
 */
public class KvStateMachine implements StateMachine<JsonNode> {
    private static final Logger log = LoggerFactory.getLogger(KvStateMachine.class);

    private final Map<String, JsonNode> kv = new ConcurrentHashMap<>();

    @Override
    public ApplyResult<JsonNode> apply(long index, Command command) {
        if (!(command instanceof PutCommand put)) {
            throw new IllegalArgumentException("unsupported KV command type: "
                    + (command == null ? "null" : command.getClass().getName()));
        }
        JsonNode previous = kv.put(put.key(), put.value());
        log.debug("kv applied index={} key={}", index, put.key());
        return new ApplyResult<>(previous, put.value(), index);
    }

    /** Чтение текущего значения; мутации по-прежнему идут только через apply. */
    public JsonNode get(String key) {
        return kv.get(key);
    }

    /** Отличает отсутствующий ключ от ключа со значением JSON {@code null}. */
    public boolean contains(String key) {
        return kv.containsKey(key);
    }
}
