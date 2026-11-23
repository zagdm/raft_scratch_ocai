package com.myraft.raft.model;

import com.fasterxml.jackson.databind.node.TextNode;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RaftLogTest {

    @Test
    void emptyLogHasZeroIndexAndTerm() {
        RaftLog log = RaftLog.EMPTY;

        assertEquals(0, log.lastIndex());
        assertEquals(0, log.lastTerm());
        assertEquals(0, log.termAt(0));
        assertEquals(0, log.termAt(1));
        assertNull(log.entryAt(1));
        assertTrue(log.isEmpty());
    }

    @Test
    void appendIsImmutableAndKeepsOneBasedIndices() {
        LogEntry first = new LogEntry(1, 2, new PutCommand("a", TextNode.valueOf("1")));
        LogEntry second = new LogEntry(2, 3, new NoOpCommand());

        RaftLog log = RaftLog.EMPTY.append(first).append(second);

        assertEquals(2, log.lastIndex());
        assertEquals(3, log.lastTerm());
        assertEquals(2, log.termAt(1));
        assertEquals(3, log.termAt(2));
        assertEquals(second, log.entryAt(2));
        assertEquals(0, RaftLog.EMPTY.lastIndex(), "append must not mutate the original log");
        assertThrows(IllegalArgumentException.class, () -> log.append(new LogEntry(4, 3, new NoOpCommand())));
    }

    @Test
    void truncateFromRemovesTail() {
        RaftLog log = RaftLog.EMPTY
                .append(new LogEntry(1, 1, new NoOpCommand()))
                .append(new LogEntry(2, 2, new NoOpCommand()))
                .append(new LogEntry(3, 3, new NoOpCommand()));

        RaftLog truncated = log.truncateFrom(2);

        assertEquals(1, truncated.lastIndex());
        assertEquals(1, truncated.lastTerm());
        assertEquals(3, log.lastIndex(), "truncate must not mutate the original log");
        assertEquals(RaftLog.EMPTY, log.truncateFrom(1));
        assertSame(log, log.truncateFrom(9));
    }

    @Test
    void upToDateComparisonFollowsSection5_4_1() {
        assertTrue(RaftLog.isAtLeastAsUpToDate(4, 1, 3, 9), "later term wins regardless of length");
        assertFalse(RaftLog.isAtLeastAsUpToDate(3, 1, 4, 9));
        assertTrue(RaftLog.isAtLeastAsUpToDate(3, 10, 3, 9), "same term: longer log wins");
        assertFalse(RaftLog.isAtLeastAsUpToDate(3, 8, 3, 9));
        assertTrue(RaftLog.isAtLeastAsUpToDate(0, 0, 0, 0));
    }

    @Test
    void rejectsNonContiguousIndicesOnLoad() {
        List<LogEntry> broken = List.of(new LogEntry(2, 1, new NoOpCommand()));
        assertThrows(IllegalArgumentException.class, () -> new RaftLog(broken));
    }
}
