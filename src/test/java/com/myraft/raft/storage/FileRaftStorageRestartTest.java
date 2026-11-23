package com.myraft.raft.storage;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.TextNode;
import com.myraft.config.RaftProperties;
import com.myraft.raft.fsm.kv.KvCommandsModule;
import com.myraft.raft.fsm.kv.PutCommand;
import com.myraft.raft.model.LogEntry;
import com.myraft.raft.model.NoOpCommand;
import com.myraft.raft.model.NodeRole;
import com.myraft.raft.model.RaftLog;
import com.myraft.raft.model.RaftState;
import com.myraft.raft.model.UserCommand;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Перезапуск узла с файловым хранилищем: term/vote/лог восстанавливаются,
 * а volatile-поля (commitIndex, lastApplied, role, leaderId) начинаются с нуля.
 */
class FileRaftStorageRestartTest {

    @Test
    void restoresPersistentStateButResetsVolatileFields(@TempDir Path dir) {
        Path file = dir.resolve("node-1.json");
        RaftState before = new RaftState(storage(file));
        List<LogEntry> entries = List.of(
                new LogEntry(1, 1, new NoOpCommand()),
                new LogEntry(2, 2, new UserCommand(new PutCommand("key", TextNode.valueOf("v")))));

        before.updateTermAndVote(3, "node-2");
        before.appendLogEntries(entries);
        before.advanceCommitIndex(2);
        before.advanceLastApplied(2);
        before.setRole(NodeRole.LEADER);
        before.setLeaderId("node-1");

        assertEquals(3, before.getCurrentTerm());
        assertEquals("node-2", before.getVotedFor());
        assertEquals(new RaftLog(entries), before.logSnapshot());
        assertEquals(2, before.getCommitIndex());
        assertEquals(2, before.getLastApplied());

        // Новый RaftState поверх того же файла — имитация перезапуска процесса.
        RaftState after = new RaftState(storage(file));
        assertEquals(3, after.getCurrentTerm());
        assertEquals("node-2", after.getVotedFor());
        assertEquals(new RaftLog(entries), after.logSnapshot());
        assertEquals(PersistedState.CURRENT_FORMAT_VERSION, storage(file).load().formatVersion());
        assertEquals(2, PersistedState.CURRENT_FORMAT_VERSION, "expected format version 2");

        assertEquals(0, after.getCommitIndex());
        assertEquals(0, after.getLastApplied());
        assertEquals(NodeRole.FOLLOWER, after.getRole());
        assertNull(after.getLeaderId());
    }

    private static FileRaftStorage storage(Path file) {
        RaftProperties props = new RaftProperties();
        props.setStoragePath(file.toString());
        return new FileRaftStorage(props, new ObjectMapper().registerModule(new KvCommandsModule()));
    }
}
