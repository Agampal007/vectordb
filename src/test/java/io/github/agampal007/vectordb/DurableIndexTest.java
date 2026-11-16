package io.github.agampal007.vectordb;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.agampal007.vectordb.wal.WalCorruptedException;
import io.github.agampal007.vectordb.wal.WriteAheadLog.SyncPolicy;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.function.Supplier;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class DurableIndexTest {

    private static final int DIM = 8;
    private static final Supplier<HnswIndex> EMPTY = () -> new HnswIndex(DIM, Distance.L2, 8, 40, 20, 5L);
    private static final float[][] VECTORS = RandomVectors.generate(600, DIM, 77L);

    @TempDir Path dir;

    private DurableIndex open() throws IOException {
        return DurableIndex.open(dir, EMPTY, SyncPolicy.MANUAL);
    }

    private static void addRange(VectorIndex index, int from, int to) {
        for (int i = from; i < to; i++) {
            index.add(i, VECTORS[i]);
        }
    }

    /** Same nodes, levels, links and tombstones: the two indexes are the same graph. */
    private static void assertSameGraph(HnswIndex expected, HnswIndex actual) {
        assertEquals(expected.nodeCount(), actual.nodeCount());
        assertEquals(expected.size(), actual.size());
        for (int node = 0; node < expected.nodeCount(); node++) {
            assertEquals(expected.levelOf(node), actual.levelOf(node), "level of node " + node);
            for (int layer = 0; layer <= expected.levelOf(node); layer++) {
                assertArrayEquals(
                        expected.neighboursOf(node, layer),
                        actual.neighboursOf(node, layer),
                        "links of node " + node + " on layer " + layer);
            }
        }
        for (float[] query : RandomVectors.generate(20, DIM, 78L)) {
            assertEquals(expected.search(query, 10), actual.search(query, 10));
        }
    }

    @Test
    void writesSurviveAReopen() throws IOException {
        try (DurableIndex index = open()) {
            addRange(index, 0, 200);
            assertTrue(index.remove(17));
            assertFalse(index.remove(17));
        }
        try (DurableIndex index = open()) {
            assertEquals(new DurableIndex.Recovery(false, 0, 201, 0, 0), index.recovery());
            assertEquals(199, index.size());
            assertFalse(index.contains(17));
            assertArrayEquals(VECTORS[42], index.get(42));
            assertEquals(42L, index.search(VECTORS[42], 1).get(0).id());
        }
    }

    @Test
    void recoveredIndexIsTheGraphItWouldHaveBeenWithoutStopping() throws IOException {
        HnswIndex uninterrupted = EMPTY.get();
        addRange(uninterrupted, 0, 600);
        for (int id = 0; id < 600; id += 7) {
            uninterrupted.remove(id);
        }

        // The same operations, interrupted by a reopen from the log alone, a snapshot, a reopen
        // from snapshot plus log, and another reopen.
        try (DurableIndex index = open()) {
            addRange(index, 0, 150);
        }
        try (DurableIndex index = open()) {
            addRange(index, 150, 300);
            index.snapshot();
            addRange(index, 300, 450);
        }
        try (DurableIndex index = open()) {
            assertTrue(index.recovery().snapshotLoaded());
            assertEquals(150, index.recovery().replayedEntries());
            addRange(index, 450, 600);
            for (int id = 0; id < 600; id += 7) {
                index.remove(id);
            }
        }
        try (DurableIndex index = open()) {
            assertSameGraph(uninterrupted, index.index());
        }
    }

    @Test
    void snapshotKeepsTombstonesAndSearchSettings() throws IOException {
        try (DurableIndex index = open()) {
            addRange(index, 0, 100);
            index.remove(3);
            index.setEfSearch(77);
            index.snapshot();
        }
        try (DurableIndex index = open()) {
            assertEquals(new DurableIndex.Recovery(true, 101, 0, 0, 0), index.recovery());
            assertEquals(99, index.size());
            assertFalse(index.contains(3));
            assertEquals(77, index.index().efSearch());
            index.add(3, VECTORS[3]);
            assertTrue(index.contains(3));
        }
    }

    @Test
    void crashBetweenWritingTheSnapshotAndEmptyingTheLogAppliesNothingTwice() throws IOException {
        try (DurableIndex index = open()) {
            addRange(index, 0, 100);
            index.writeSnapshot(); // the process "dies" here, before the log is emptied
        }
        try (DurableIndex index = open()) {
            assertEquals(new DurableIndex.Recovery(true, 100, 0, 100, 0), index.recovery());
            assertEquals(100, index.size());
            addRange(index, 100, 120);
        }
        try (DurableIndex index = open()) {
            assertEquals(20, index.recovery().replayedEntries());
            assertEquals(120, index.size());
        }
    }

    @Test
    void unfinishedSnapshotFromACrashIsDiscarded() throws IOException {
        try (DurableIndex index = open()) {
            addRange(index, 0, 50);
        }
        Path temporary = dir.resolve(DurableIndex.SNAPSHOT_TEMPORARY);
        Files.write(temporary, new byte[] {1, 2, 3});
        try (DurableIndex index = open()) {
            assertEquals(50, index.size());
            assertFalse(Files.exists(temporary));
        }
    }

    @Test
    void tornLogTailIsDroppedAndEarlierWritesKept() throws IOException {
        try (DurableIndex index = open()) {
            addRange(index, 0, 50);
        }
        // Half a record: a length, a checksum and part of a payload.
        Files.write(dir.resolve(DurableIndex.LOG_FILE), new byte[] {0, 0, 0, 53, 9, 9, 9, 9, 0, 0, 0}, StandardOpenOption.APPEND);
        try (DurableIndex index = open()) {
            assertEquals(11, index.recovery().truncatedBytes());
            assertEquals(50, index.size());
            addRange(index, 50, 60);
        }
        try (DurableIndex index = open()) {
            assertEquals(60, index.size());
            assertEquals(0, index.recovery().truncatedBytes());
        }
    }

    @Test
    void corruptSnapshotRefusesToOpen() throws IOException {
        try (DurableIndex index = open()) {
            addRange(index, 0, 100);
            index.snapshot();
        }
        Path snapshot = dir.resolve(DurableIndex.SNAPSHOT_FILE);
        byte[] bytes = Files.readAllBytes(snapshot);
        bytes[bytes.length / 2] ^= 0x10;
        Files.write(snapshot, bytes);
        assertThrows(CorruptSnapshotException.class, this::open);

        Files.write(snapshot, java.util.Arrays.copyOf(bytes, bytes.length / 3));
        assertThrows(CorruptSnapshotException.class, this::open);
    }

    @Test
    void corruptLogRefusesToOpen() throws IOException {
        try (DurableIndex index = open()) {
            addRange(index, 0, 100);
        }
        Path log = dir.resolve(DurableIndex.LOG_FILE);
        byte[] bytes = Files.readAllBytes(log);
        bytes[bytes.length / 2] ^= 0x10;
        Files.write(log, bytes);
        assertThrows(WalCorruptedException.class, this::open);
    }

    @Test
    void rejectedWritesAreNeverLogged() throws IOException {
        try (DurableIndex index = open()) {
            addRange(index, 0, 10);
            long before = Files.size(dir.resolve(DurableIndex.LOG_FILE));
            assertThrows(IllegalArgumentException.class, () -> index.add(3, VECTORS[3]));
            assertThrows(IllegalArgumentException.class, () -> index.add(99, new float[DIM + 1]));
            assertFalse(index.remove(12345));
            assertEquals(before, Files.size(dir.resolve(DurableIndex.LOG_FILE)));
        }
        try (DurableIndex index = open()) {
            assertEquals(10, index.recovery().replayedEntries());
        }
    }
}
