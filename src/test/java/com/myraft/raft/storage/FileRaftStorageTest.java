package com.myraft.raft.storage;

import com.myraft.config.RaftProperties;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;

class FileRaftStorageTest {

    @Test
    void persistsAndReloadsState(@TempDir Path dir) {
        RaftProperties props = new RaftProperties();
        props.setStoragePath(dir.resolve("raft-state.json").toString());

        FileRaftStorage storage = new FileRaftStorage(props);
        assertEquals(PersistedState.EMPTY, storage.load());

        storage.persist(4, "node-2");

        // Новый экземпляр имитирует перезапуск узла.
        FileRaftStorage reopened = new FileRaftStorage(props);
        assertEquals(new PersistedState(4, "node-2"), reopened.load());
    }

    @Test
    void overwritesPreviousState(@TempDir Path dir) {
        RaftProperties props = new RaftProperties();
        props.setStoragePath(dir.resolve("raft-state.json").toString());

        FileRaftStorage storage = new FileRaftStorage(props);
        storage.persist(4, "node-2");
        storage.persist(5, null);

        assertEquals(new PersistedState(5, null), storage.load());
    }
}
