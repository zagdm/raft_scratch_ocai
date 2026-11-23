package com.myraft.raft.storage;

import com.fasterxml.jackson.databind.node.TextNode;
import com.myraft.config.RaftProperties;
import com.myraft.raft.model.LogEntry;
import com.myraft.raft.model.PutCommand;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class FileRaftStorageTest {

    @Test
    void persistsAndReloadsState(@TempDir Path dir) {
        RaftProperties props = new RaftProperties();
        props.setStoragePath(dir.resolve("raft-state.json").toString());

        FileRaftStorage storage = new FileRaftStorage(props);
        assertEquals(PersistedState.EMPTY, storage.load());

        storage.persist(new PersistedState(4, "node-2", List.of()));

        // Новый экземпляр имитирует перезапуск узла.
        FileRaftStorage reopened = new FileRaftStorage(props);
        assertEquals(new PersistedState(4, "node-2", List.of()), reopened.load());
    }

    @Test
    void overwritesPreviousState(@TempDir Path dir) {
        RaftProperties props = new RaftProperties();
        props.setStoragePath(dir.resolve("raft-state.json").toString());

        FileRaftStorage storage = new FileRaftStorage(props);
        storage.persist(new PersistedState(4, "node-2", List.of()));
        storage.persist(new PersistedState(5, null, List.of()));

        assertEquals(new PersistedState(5, null, List.of()), storage.load());
    }

    @Test
    void persistsLogTogetherWithTermAndVote(@TempDir Path dir) throws Exception {
        RaftProperties props = new RaftProperties();
        Path file = dir.resolve("raft-state.json");
        props.setStoragePath(file.toString());

        PersistedState state = new PersistedState(6, "node-1", List.of(
                new LogEntry(1, 5, new PutCommand("key", TextNode.valueOf("old"))),
                new LogEntry(2, 6, new PutCommand("key", TextNode.valueOf("new")))));

        FileRaftStorage storage = new FileRaftStorage(props);
        storage.persist(state);

        assertEquals(state, new FileRaftStorage(props).load());
    }

    @Test
    void legacyStateWithoutLogLoadsWithEmptyLog(@TempDir Path dir) throws Exception {
        RaftProperties props = new RaftProperties();
        Path file = dir.resolve("raft-state.json");
        props.setStoragePath(file.toString());
        Files.writeString(file, "{\"term\":3,\"votedFor\":\"node-9\"}");

        assertEquals(new PersistedState(3, "node-9", List.of()), new FileRaftStorage(props).load());
    }
}
