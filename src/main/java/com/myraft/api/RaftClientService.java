package com.myraft.api;

import com.fasterxml.jackson.databind.JsonNode;
import com.myraft.api.dto.AppendResponse;
import com.myraft.api.dto.KvReadResponse;
import com.myraft.api.dto.LogEntryResponse;
import com.myraft.raft.ApplyTimeoutException;
import com.myraft.raft.RaftNode;
import com.myraft.raft.fsm.ApplyResult;
import com.myraft.raft.fsm.kv.KvStateMachine;
import com.myraft.raft.fsm.kv.PutCommand;
import com.myraft.raft.model.LogEntry;
import com.myraft.raft.model.NodeRole;
import com.myraft.raft.model.RaftLog;
import com.myraft.raft.model.RaftState;
import com.myraft.raft.model.RaftState.RaftStateSnapshot;
import com.myraft.raft.model.UserCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;

/**
 * API-адаптер поверх ядра Raft: конкретизирует непрозрачный {@link ApplyResult}
 * до JSON-ответа и ожидает коммита/apply вне {@code nodeLock}.
 */
@Service
public class RaftClientService {
    private static final Logger log = LoggerFactory.getLogger(RaftClientService.class);

    private final RaftNode raftNode;
    private final RaftState raftState;
    private final KvStateMachine stateMachine;
    private final long applyTimeoutMs;

    public RaftClientService(RaftNode raftNode, RaftState raftState, KvStateMachine stateMachine,
                             @Value("${raft.client.apply-timeout-ms:5000}") long applyTimeoutMs) {
        this.raftNode = raftNode;
        this.raftState = raftState;
        this.stateMachine = stateMachine;
        this.applyTimeoutMs = applyTimeoutMs;
    }

    /** Принимает запись только на лидере и отвечает после коммита и apply. */
    public AppendResponse append(String key, JsonNode value) {
        if (key == null || key.isBlank()) {
            throw new IllegalArgumentException("key must not be blank");
        }
        RaftStateSnapshot before = raftState.snapshot();
        if (before.role() != NodeRole.LEADER) {
            log.info("client redirected leaderId={}", before.leaderId());
            throw new NotLeaderException(before.leaderId());
        }

        PutCommand command = new PutCommand(key, value);
        long index = raftNode.appendUserCommand(command);
        if (index < 0) {
            // Лидерство могло измениться между проверкой снимка и append под nodeLock.
            RaftStateSnapshot after = raftState.snapshot();
            log.info("client redirected leaderId={}", after.leaderId());
            throw new NotLeaderException(after.leaderId());
        }
        log.info("client append accepted key={} index={} term={} valueSize={}",
                key, index, before.currentTerm(), valueSize(command.value()));

        ApplyResult<?> applied = raftNode.awaitApplied(index, applyTimeoutMs);
        @SuppressWarnings("unchecked")
        ApplyResult<JsonNode> result = (ApplyResult<JsonNode>) applied;
        log.info("client append responded index={} previousValue={} value={}",
                index, result.previousValue(), result.value());
        return new AppendResponse(result.previousValue(), result.value(), index);
    }

    /** Пользовательские записи лога в порядке индекса, без Raft-маркеров NoOp. */
    public List<LogEntryResponse> readLog() {
        RaftLog snapshot = raftState.logSnapshot();
        List<LogEntryResponse> entries = new ArrayList<>();
        for (LogEntry entry : snapshot.entries()) {
            if (entry.command() instanceof UserCommand user
                    && user.payload() instanceof PutCommand put) {
                entries.add(new LogEntryResponse(entry.index(), entry.term(), put.key(), put.value()));
            }
        }
        log.info("log read count={} lastIndex={}", entries.size(), snapshot.lastIndex());
        return entries;
    }

    /** Чтение текущего значения KV; found различает отсутствие ключа и JSON null. */
    public KvReadResponse readKv(String key) {
        if (key == null || key.isBlank()) {
            throw new IllegalArgumentException("key must not be blank");
        }
        boolean found = stateMachine.contains(key);
        JsonNode value = found ? stateMachine.get(key) : null;
        log.info("kv read key={} found={}", key, found);
        return new KvReadResponse(key, value, found);
    }

    private static int valueSize(JsonNode value) {
        return value == null ? 0 : value.toString().length();
    }
}
