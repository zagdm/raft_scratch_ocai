package com.myraft.raft.storage;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.TextNode;
import com.myraft.config.RaftProperties;
import com.myraft.raft.fsm.kv.KvCommandsModule;
import com.myraft.raft.fsm.kv.PutCommand;
import com.myraft.raft.model.LogEntry;
import com.myraft.raft.model.NoOpCommand;
import com.myraft.raft.model.UserCommand;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FileRaftStorageTest {

    private static ObjectMapper mapper() {
        return new ObjectMapper().registerModule(new KvCommandsModule());
    }

    private static FileRaftStorage storage(Path file) {
        RaftProperties props = new RaftProperties();
        props.setStoragePath(file.toString());
        return new FileRaftStorage(props, mapper());
    }

    @Test
    void persistsAndReloadsState(@TempDir Path dir) {
        FileRaftStorage storage = storage(dir.resolve("raft-state.json"));
        assertEquals(PersistedState.EMPTY, storage.load());

        storage.persist(PersistedState.of(4, "node-2", List.of()));

        // Новый экземпляр имитирует перезапуск узла.
        assertEquals(PersistedState.of(4, "node-2", List.of()),
                storage(dir.resolve("raft-state.json")).load());
    }

    @Test
    void overwritesPreviousState(@TempDir Path dir) {
        FileRaftStorage storage = storage(dir.resolve("raft-state.json"));
        storage.persist(PersistedState.of(4, "node-2", List.of()));
        storage.persist(PersistedState.of(5, null, List.of()));

        assertEquals(PersistedState.of(5, null, List.of()), storage.load());
    }

    @Test
    void persistsLogTogetherWithTermAndVote(@TempDir Path dir) {
        Path file = dir.resolve("raft-state.json");
        PersistedState state = PersistedState.of(6, "node-1", List.of(
                new LogEntry(1, 5, new UserCommand(new PutCommand("key", TextNode.valueOf("old")))),
                new LogEntry(2, 6, new NoOpCommand())));

        storage(file).persist(state);

        assertEquals(state, storage(file).load());
        assertEquals(PersistedState.CURRENT_FORMAT_VERSION, storage(file).load().formatVersion());
    }

    @Test
    void rejectsStateWithoutFormatVersion(@TempDir Path dir) throws Exception {
        Path file = dir.resolve("raft-state.json");
        Files.writeString(file, "{\"term\":3,\"votedFor\":\"node-9\"}");

        IllegalStateException e = assertThrows(IllegalStateException.class, () -> storage(file).load());
        assertTrue(e.getMessage().contains("unsupported raft state format"), e.getMessage());
    }

    @Test
    void rejectsOldAndUnknownFormatVersions(@TempDir Path dir) throws Exception {
        Path file = dir.resolve("raft-state.json");
        for (int version : List.of(1, 99)) {
            Files.writeString(file, "{\"formatVersion\":" + version + ",\"term\":3,\"votedFor\":null}");
            IllegalStateException e = assertThrows(IllegalStateException.class, () -> storage(file).load());
            assertTrue(e.getMessage().contains("unsupported raft state format"), e.getMessage());
        }
    }
}
