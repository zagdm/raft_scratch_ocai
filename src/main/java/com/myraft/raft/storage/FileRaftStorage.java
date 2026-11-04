package com.myraft.raft.storage;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.myraft.config.RaftProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;

/**
 * Файловое хранилище состояния Raft.
 * <p>
 * Запись идёт во временный файл в том же каталоге, затем {@code fsync} и атомарный
 * {@code rename}. Это защищает от частично записанного файла при падении процесса.
 */
@Component
@org.springframework.boot.autoconfigure.condition.ConditionalOnProperty(name = "raft.storage.type", havingValue = "file", matchIfMissing = true)
public class FileRaftStorage implements RaftStorage {
    private static final Logger log = LoggerFactory.getLogger(FileRaftStorage.class);

    private final Path file;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public FileRaftStorage(RaftProperties raftProperties) {
        this.file = Path.of(raftProperties.getStoragePath());
    }

    @Override
    public PersistedState load() {
        if (!Files.exists(file)) {
            log.debug("storage empty path={} term=0 votedFor=null", file.toAbsolutePath());
            return PersistedState.EMPTY;
        }
        try {
            PersistedState state = objectMapper.readValue(Files.readAllBytes(file), PersistedState.class);
            if (state == null) {
                return PersistedState.EMPTY;
            }
            log.debug("storage loaded path={} term={} votedFor={}",
                    file.toAbsolutePath(), state.term(), state.votedFor());
            return state;
        } catch (IOException e) {
            // Молча начинать с term=0 нельзя: это может привести к двойному голосованию.
            throw new IllegalStateException("Failed to read Raft state from " + file.toAbsolutePath(), e);
        }
    }

    @Override
    public synchronized void persist(int term, String votedFor) {
        PersistedState state = new PersistedState(term, votedFor);
        Path absolute = file.toAbsolutePath();
        Path tmp = absolute.resolveSibling(absolute.getFileName() + ".tmp");
        try {
            Path dir = absolute.getParent();
            if (dir != null) {
                Files.createDirectories(dir);
            }
            byte[] payload = objectMapper.writeValueAsBytes(state);
            try (FileChannel channel = FileChannel.open(tmp,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.WRITE,
                    StandardOpenOption.TRUNCATE_EXISTING)) {
                channel.write(ByteBuffer.wrap(payload));
                channel.force(true);
            }
            try {
                Files.move(tmp, absolute, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
            } catch (AtomicMoveNotSupportedException e) {
                Files.move(tmp, absolute, StandardCopyOption.REPLACE_EXISTING);
            }
            log.trace("storage persisted term={} votedFor={}", term, votedFor);
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to persist Raft state to " + absolute, e);
        }
    }
}
