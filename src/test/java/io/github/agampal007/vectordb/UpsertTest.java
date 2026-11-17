package io.github.agampal007.vectordb;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.agampal007.vectordb.wal.WriteAheadLog.SyncPolicy;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.function.Supplier;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class UpsertTest {

    private static final int DIM = 8;
    private static final Supplier<HnswIndex> EMPTY = () -> new HnswIndex(DIM, Distance.L2, 8, 40, 20, 5L);
    private static final float[][] VECTORS = RandomVectors.generate(400, DIM, 91L);

    @TempDir Path dir;

    private DurableIndex open() throws IOException {
        return DurableIndex.open(dir, EMPTY, SyncPolicy.MANUAL);
    }

    @Test
    void upsertAddsANewIdAndReplacesAnExistingOne() throws IOException {
        try (DurableIndex index = open()) {
            assertFalse(index.upsert(7, VECTORS[0]));
            assertFalse(index.upsert(8, VECTORS[1]));
            assertTrue(index.upsert(7, VECTORS[2]));

            assertEquals(2, index.size());
            assertArrayEquals(VECTORS[2], index.get(7));
            assertEquals(new SearchResult(7, 0f), index.search(VECTORS[2], 1).get(0));
            // The old vector is gone: its nearest neighbour is now at a distance.
            assertTrue(index.search(VECTORS[0], 1).get(0).distance() > 0f);
            assertThrows(IllegalArgumentException.class, () -> index.upsert(9, new float[DIM + 1]));
        }
    }

    @Test
    void upsertsReplayIntoTheSameGraph() throws IOException {
        HnswIndex oneGo = EMPTY.get();
        HnswIndex live;
        try (DurableIndex index = open()) {
            for (int i = 0; i < 300; i++) {
                long id = i % 120; // every id is written two or three times
                assertEquals(i >= 120, index.upsert(id, VECTORS[i]));
                oneGo.remove(id);
                oneGo.add(id, VECTORS[i]);
                if (i == 150) {
                    index.snapshot(); // replay then starts from a snapshot that already has tombstones
                }
            }
            live = index.index();
            TestGraphs.assertSameGraph(oneGo, live);
        }
        try (DurableIndex index = open()) {
            assertEquals(120, index.size());
            assertEquals(149, index.recovery().replayedEntries());
            assertArrayEquals(VECTORS[299], index.get(299 % 120));
            TestGraphs.assertSameGraph(live, index.index());
        }
    }

    /**
     * An upsert is one log record. If the process dies while writing it, the record is torn and
     * dropped, and the id keeps its old vector. Removing and then adding would be two records, and
     * a crash between them would leave the id with no vector at all.
     */
    @Test
    void anUpsertCutShortByACrashLeavesTheOldVector() throws IOException {
        Path log = dir.resolve(DurableIndex.LOG_FILE);
        long sizeBefore;
        try (DurableIndex index = open()) {
            index.upsert(1, VECTORS[0]);
            index.upsert(2, VECTORS[1]);
            sizeBefore = index.logSizeBytes();
            assertTrue(index.upsert(1, VECTORS[2]));
        }
        try (FileChannel channel = FileChannel.open(log, StandardOpenOption.WRITE)) {
            assertTrue(channel.size() > sizeBefore + 5);
            channel.truncate(channel.size() - 5);
        }
        try (DurableIndex index = open()) {
            assertEquals(2, index.size());
            assertArrayEquals(VECTORS[0], index.get(1));
            assertTrue(index.recovery().truncatedBytes() > 0);
        }
    }
}
