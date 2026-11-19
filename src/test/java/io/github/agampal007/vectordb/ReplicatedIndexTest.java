package io.github.agampal007.vectordb;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.agampal007.vectordb.DurableIndex.Position;
import io.github.agampal007.vectordb.wal.WriteAheadLog;
import io.github.agampal007.vectordb.wal.WriteAheadLog.Entry;
import io.github.agampal007.vectordb.wal.WriteAheadLog.Op;
import io.github.agampal007.vectordb.wal.WriteAheadLog.SyncPolicy;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.zip.CRC32;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** The pieces of replication that live in {@link DurableIndex}, without any networking. */
class ReplicatedIndexTest {

    private static final int DIM = 8;
    private static final Supplier<HnswIndex> EMPTY = () -> new HnswIndex(DIM, Distance.L2, 8, 40, 20, 5L);
    private static final float[][] VECTORS = RandomVectors.generate(700, DIM, 33L);

    @TempDir Path dir;

    private DurableIndex open(String name) throws IOException {
        return DurableIndex.open(dir.resolve(name), EMPTY, SyncPolicy.MANUAL);
    }

    /** Copies records from the leader to the follower in small batches until there are none left. */
    private static int replicate(DurableIndex leader, DurableIndex follower) throws IOException {
        int batches = 0;
        while (true) {
            byte[] records = leader.readLogAfter(follower.lastPosition(), 2_000);
            assertNotNull(records, "the leader refused the follower's position " + follower.lastPosition());
            if (records.length == 0) {
                return batches;
            }
            follower.applyReplicated(WriteAheadLog.decodeRecords(records));
            batches++;
        }
    }

    /** Adds, replaces and removes, in a fixed pattern, then forces the log to the device. */
    private static void writeSome(DurableIndex index, int from, int to) throws IOException {
        for (int i = from; i < to; i++) {
            index.upsert(i, VECTORS[i]);
            if (i % 5 == 4) {
                assertTrue(index.remove(i - 2));
            }
            if (i % 7 == 6) {
                index.upsert(i - 1, VECTORS[(i * 3) % VECTORS.length]);
            }
        }
        index.sync();
    }

    @Test
    void aFollowerThatAppliesTheLeadersRecordsHasTheLeadersGraph() throws IOException {
        try (DurableIndex leader = open("leader");
                DurableIndex follower = open("follower")) {
            leader.setTerm(1);
            writeSome(leader, 0, 250);
            leader.setTerm(3);
            writeSome(leader, 250, 400);
            assertTrue(replicate(leader, follower) > 1, "expected more than one batch");

            assertEquals(leader.lastPosition(), follower.lastPosition());
            assertEquals(new Position(leader.lastPosition().sequence(), 3), follower.lastPosition());
            TestGraphs.assertSameGraph(leader.index(), follower.index());
        }
        // The follower's own log replays into the same graph.
        try (DurableIndex leader = open("leader");
                DurableIndex follower = open("follower")) {
            TestGraphs.assertSameGraph(leader.index(), follower.index());
            assertArrayEquals(
                    Files.readAllBytes(dir.resolve("leader").resolve(DurableIndex.LOG_FILE)),
                    Files.readAllBytes(dir.resolve("follower").resolve(DurableIndex.LOG_FILE)));
        }
    }

    @Test
    void theLeaderRefusesAFollowerWhoseLogDoesNotContinueIntoItsOwn() throws IOException {
        try (DurableIndex leader = open("leader")) {
            leader.setTerm(1);
            writeSome(leader, 0, 10); // sequence numbers 1 to 13, term 1
            leader.setTerm(2);
            writeSome(leader, 10, 20); // 14 to 26, term 2
            long last = leader.lastPosition().sequence();

            assertNotNull(leader.readLogAfter(new Position(0, 0), 1 << 20), "a new follower");
            assertNotNull(leader.readLogAfter(new Position(12, 1), 1 << 20), "agrees at 12");
            assertEquals(0, leader.readLogAfter(new Position(last, 2), 1 << 20).length, "up to date");
            assertNull(leader.readLogAfter(new Position(14, 1), 1 << 20), "a different term at 14");
            assertNull(leader.readLogAfter(new Position(last + 3, 2), 1 << 20), "ahead of the leader");

            leader.snapshot();
            assertNull(leader.readLogAfter(new Position(12, 1), 1 << 20), "now only in the snapshot");
            assertEquals(0, leader.readLogAfter(new Position(last, 2), 1 << 20).length, "still up to date");
        }
    }

    @Test
    void onlyOperationsForcedToTheDeviceAreSent() throws IOException {
        try (DurableIndex leader = open("leader")) {
            leader.upsert(1, VECTORS[1]);
            assertEquals(0, leader.readLogAfter(new Position(0, 0), 1 << 20).length);
            leader.sync();
            assertEquals(1, WriteAheadLog.decodeRecords(leader.readLogAfter(new Position(0, 0), 1 << 20)).size());
        }
    }

    @Test
    void aFollowerStartsAgainFromATransferSnapshotAndCarriesOn() throws IOException {
        Path followerDirectory = dir.resolve("follower");
        try (DurableIndex leader = open("leader")) {
            leader.setTerm(2);
            writeSome(leader, 0, 200);
            leader.snapshot(); // the log no longer holds the start
            writeSome(leader, 200, 300);
            try (DurableIndex follower = open("follower")) {
                follower.upsert(9_999, VECTORS[0]); // something the leader never had
                assertNull(leader.readLogAfter(follower.lastPosition(), 1 << 20));
            }

            DurableIndex.Transfer transfer = leader.writeTransferSnapshot();
            assertEquals(leader.lastPosition(), transfer.position());
            Path download = followerDirectory.resolve(DurableIndex.TRANSFER_PREFIX + "download");
            Files.copy(transfer.file(), download);
            Files.delete(transfer.file());

            assertEquals(transfer.position(), DurableIndex.installSnapshot(followerDirectory, download));
            assertFalse(Files.exists(download));
            try (DurableIndex follower = open("follower")) {
                assertEquals(transfer.position(), follower.lastPosition());
                assertFalse(follower.contains(9_999));
                TestGraphs.assertSameGraph(leader.index(), follower.index());

                writeSome(leader, 300, 400);
                replicate(leader, follower);
                TestGraphs.assertSameGraph(leader.index(), follower.index());
            }
        }
    }

    @Test
    void aDamagedDownloadChangesNothing() throws IOException {
        try (DurableIndex leader = open("leader")) {
            writeSome(leader, 0, 50);
            try (DurableIndex follower = open("follower")) {
                writeSome(follower, 0, 20);
            }
            DurableIndex.Transfer transfer = leader.writeTransferSnapshot();
            byte[] bytes = Files.readAllBytes(transfer.file());
            bytes[bytes.length / 2] ^= 0x01;
            Path download = dir.resolve("follower").resolve(DurableIndex.TRANSFER_PREFIX + "download");
            Files.write(download, bytes);
            byte[] logBefore = Files.readAllBytes(dir.resolve("follower").resolve(DurableIndex.LOG_FILE));

            assertThrows(CorruptSnapshotException.class, () -> DurableIndex.installSnapshot(dir.resolve("follower"), download));
            assertArrayEquals(logBefore, Files.readAllBytes(dir.resolve("follower").resolve(DurableIndex.LOG_FILE)));
            try (DurableIndex follower = open("follower")) {
                assertFalse(Files.exists(download), "left-over transfers are deleted on open");
                assertTrue(follower.contains(19));
            }
        }
    }

    @Test
    void installingIntoAnOpenDirectoryIsRefused() throws IOException {
        try (DurableIndex leader = open("leader");
                DurableIndex follower = open("follower")) {
            DurableIndex.Transfer transfer = leader.writeTransferSnapshot();
            assertThrows(IndexLockedException.class, () -> DurableIndex.installSnapshot(dir.resolve("follower"), transfer.file()));
            assertNotNull(follower.lastPosition());
        }
    }

    @Test
    void recordsThatWouldBreakReplayAreRefused() throws IOException {
        try (DurableIndex follower = open("follower")) {
            Entry gap = new Entry(5, 1, Op.ADD, 1, VECTORS[1]);
            assertThrows(IllegalArgumentException.class, () -> follower.applyReplicated(List.of(gap)));
            Entry absent = new Entry(1, 1, Op.REMOVE, 42, null);
            assertThrows(IllegalArgumentException.class, () -> follower.applyReplicated(List.of(absent)));
            Entry wrongDimension = new Entry(1, 1, Op.ADD, 1, new float[DIM + 1]);
            assertThrows(IllegalArgumentException.class, () -> follower.applyReplicated(List.of(wrongDimension)));
            follower.applyReplicated(List.of(new Entry(1, 4, Op.ADD, 1, VECTORS[1])));
            Entry olderTerm = new Entry(2, 3, Op.ADD, 2, VECTORS[2]);
            assertThrows(IllegalArgumentException.class, () -> follower.applyReplicated(List.of(olderTerm)));
            assertEquals(new Position(1, 4), follower.lastPosition());
            assertEquals(1, follower.size());
        }
    }

    @Test
    void theTermOfLocalWritesCannotGoBelowTheLog() throws IOException {
        try (DurableIndex index = open("index")) {
            index.setTerm(5);
            index.upsert(1, VECTORS[1]);
            assertEquals(new Position(1, 5), index.lastPosition());
            assertThrows(IllegalArgumentException.class, () -> index.setTerm(4));
            index.setTerm(6);
            index.remove(1);
            assertEquals(new Position(2, 6), index.lastPosition());
        }
        try (DurableIndex index = open("index")) {
            assertEquals(6, index.term(), "a reopened index carries on in its last term");
        }
    }

    @Test
    void aWaitingRequestWakesWhenOperationsReachTheDevice() throws Exception {
        try (DurableIndex leader = open("leader")) {
            long start = System.nanoTime();
            assertFalse(leader.awaitSyncedAfter(0, 50, TimeUnit.MILLISECONDS));
            assertTrue(System.nanoTime() - start >= 40_000_000L);

            CompletableFuture<Boolean> waiting = CompletableFuture.supplyAsync(() -> {
                try {
                    return leader.awaitSyncedAfter(0, 10, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    throw new IllegalStateException(e);
                }
            });
            leader.upsert(1, VECTORS[1]);
            Thread.sleep(50);
            assertFalse(waiting.isDone(), "written but not yet forced to the device");
            leader.sync();
            assertTrue(waiting.get(5, TimeUnit.SECONDS));
        }
    }

    /** An index directory from before terms existed opens, and is rewritten so it can be appended to. */
    @Test
    void aDirectoryWrittenByVersion1IsBroughtUpToDate() throws IOException {
        Path old = dir.resolve("old");
        Files.createDirectories(old);
        Files.write(old.resolve(DurableIndex.LOG_FILE), version1Log(3));
        try (DurableIndex index = open("old")) {
            assertEquals(3, index.size());
            assertEquals(new Position(3, 0), index.lastPosition());
            assertTrue(Files.exists(old.resolve(DurableIndex.SNAPSHOT_FILE)), "the old log was folded into a snapshot");
            assertFalse(index.hasLoggedOperations());
            index.setTerm(1);
            index.upsert(10, VECTORS[10]);
        }
        try (DurableIndex index = open("old")) {
            assertEquals(4, index.size());
            assertEquals(new Position(4, 1), index.lastPosition());
            assertArrayEquals(VECTORS[1], index.get(1));
        }
    }

    /** A version 1 log holding ADD records for ids 0 to count - 1 with {@link #VECTORS}. */
    private static byte[] version1Log(int count) {
        int payloadBytes = 8 + 1 + 8 + 4 + 4 * DIM;
        ByteBuffer out = ByteBuffer.allocate(8 + count * (8 + payloadBytes));
        out.putInt(0x56444257).putInt(1);
        for (int id = 0; id < count; id++) {
            ByteBuffer payload = ByteBuffer.allocate(payloadBytes);
            payload.putLong(id + 1).put((byte) 1).putLong(id).putInt(DIM);
            for (float value : VECTORS[id]) {
                payload.putFloat(value);
            }
            CRC32 crc = new CRC32();
            crc.update(payload.array());
            out.putInt(payloadBytes).putInt((int) crc.getValue()).put(payload.array());
        }
        return out.array();
    }
}
