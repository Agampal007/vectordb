package io.github.agampal007.vectordb.wal;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.github.agampal007.vectordb.wal.WriteAheadLog.Entry;
import io.github.agampal007.vectordb.wal.WriteAheadLog.Op;
import io.github.agampal007.vectordb.wal.WriteAheadLog.SyncPolicy;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class WriteAheadLogTest {

    @TempDir Path dir;

    private static float[] vector(int i) {
        return new float[] {i, i + 0.5f, -i};
    }

    private List<Entry> open(Path path) throws IOException {
        List<Entry> entries = new ArrayList<>();
        WriteAheadLog.open(path, SyncPolicy.MANUAL, 0, entries::add).close();
        return entries;
    }

    /** Writes {@code count} ADD records and returns the file size after each one. */
    private long[] write(Path path, int count) throws IOException {
        long[] sizes = new long[count];
        try (WriteAheadLog log = WriteAheadLog.open(path, SyncPolicy.MANUAL, 0, e -> {})) {
            for (int i = 0; i < count; i++) {
                log.append(Op.ADD, 100 + i, vector(i));
                sizes[i] = log.sizeBytes();
            }
        }
        return sizes;
    }

    @Test
    void replaysEntriesInOrderWithTheirSequenceNumbers() throws IOException {
        Path path = dir.resolve("wal");
        try (WriteAheadLog log = WriteAheadLog.open(path, SyncPolicy.EVERY_WRITE, 0, e -> {})) {
            assertEquals(1, log.append(Op.ADD, 7, vector(1)));
            assertEquals(2, log.append(Op.REMOVE, 7, null));
            assertEquals(3, log.append(Op.ADD, -9, vector(2)));
        }
        List<Entry> entries = open(path);
        assertEquals(List.of(1L, 2L, 3L), entries.stream().map(Entry::sequence).toList());
        assertEquals(List.of(Op.ADD, Op.REMOVE, Op.ADD), entries.stream().map(Entry::op).toList());
        assertEquals(List.of(7L, 7L, -9L), entries.stream().map(Entry::id).toList());
        assertArrayEquals(vector(1), entries.get(0).vector());
        assertNull(entries.get(1).vector());
        assertArrayEquals(vector(2), entries.get(2).vector());
    }

    @Test
    void sequenceContinuesAfterReopenAndAfterReset() throws IOException {
        Path path = dir.resolve("wal");
        write(path, 3);
        try (WriteAheadLog log = WriteAheadLog.open(path, SyncPolicy.MANUAL, 0, e -> {})) {
            assertEquals(3, log.replayedEntries());
            assertEquals(4, log.append(Op.REMOVE, 100, null));
            log.reset();
            assertEquals(5, log.append(Op.REMOVE, 101, null));
        }
        assertEquals(List.of(5L), open(path).stream().map(Entry::sequence).toList());
        // An empty log cannot know the last sequence; the snapshot's is passed in instead.
        try (WriteAheadLog log = WriteAheadLog.open(dir.resolve("fresh"), SyncPolicy.MANUAL, 41, e -> {})) {
            assertEquals(42, log.append(Op.REMOVE, 1, null));
        }
    }

    @Test
    void tornLastRecordIsDroppedWhereverItWasCut() throws IOException {
        Path original = dir.resolve("wal");
        long[] sizes = write(original, 5);
        byte[] bytes = Files.readAllBytes(original);
        long lastStart = sizes[3];
        for (long cut = lastStart + 1; cut < sizes[4]; cut++) {
            Path torn = dir.resolve("torn-" + cut);
            Files.write(torn, Arrays.copyOf(bytes, (int) cut));
            List<Entry> entries = new ArrayList<>();
            try (WriteAheadLog log = WriteAheadLog.open(torn, SyncPolicy.MANUAL, 0, entries::add)) {
                assertEquals(4, entries.size(), "cut at " + cut);
                assertEquals(cut - lastStart, log.truncatedBytes(), "cut at " + cut);
                assertEquals(lastStart, log.sizeBytes(), "cut at " + cut);
                assertEquals(5, log.append(Op.ADD, 999, vector(9)));
            }
            assertEquals(5, open(torn).size(), "cut at " + cut);
        }
    }

    @Test
    void cutExactlyBetweenRecordsLosesNothing() throws IOException {
        Path original = dir.resolve("wal");
        long[] sizes = write(original, 5);
        byte[] bytes = Files.readAllBytes(original);
        for (int kept = 0; kept < 5; kept++) {
            Path cut = dir.resolve("cut-" + kept);
            Files.write(cut, Arrays.copyOf(bytes, (int) (kept == 0 ? 8 : sizes[kept - 1])));
            try (WriteAheadLog log = WriteAheadLog.open(cut, SyncPolicy.MANUAL, 0, e -> {})) {
                assertEquals(kept, log.replayedEntries());
                assertEquals(0, log.truncatedBytes());
            }
        }
    }

    @Test
    void zeroFilledTailIsDropped() throws IOException {
        Path path = dir.resolve("wal");
        long[] sizes = write(path, 3);
        byte[] bytes = Arrays.copyOf(Files.readAllBytes(path), (int) sizes[2] + 4096);
        Files.write(path, bytes);
        try (WriteAheadLog log = WriteAheadLog.open(path, SyncPolicy.MANUAL, 0, e -> {})) {
            assertEquals(3, log.replayedEntries());
            assertEquals(4096, log.truncatedBytes());
        }
    }

    @Test
    void corruptionBeforeTheTailRefusesToOpenAndLeavesTheFileAlone() throws IOException {
        Path path = dir.resolve("wal");
        long[] sizes = write(path, 5);
        byte[] bytes = Files.readAllBytes(path);
        bytes[(int) sizes[1] + 12] ^= 0x01; // inside the third record's payload
        Files.write(path, bytes);

        assertThrows(WalCorruptedException.class, () -> open(path));
        assertArrayEquals(bytes, Files.readAllBytes(path));
    }

    @Test
    void corruptedLastRecordIsTreatedAsATornWrite() throws IOException {
        Path path = dir.resolve("wal");
        long[] sizes = write(path, 5);
        byte[] bytes = Files.readAllBytes(path);
        bytes[(int) sizes[3] + 12] ^= 0x01;
        Files.write(path, bytes);

        assertEquals(4, open(path).size());
    }

    @Test
    void garbageLengthIsCorruptionNotATornWrite() throws IOException {
        Path path = dir.resolve("wal");
        long[] sizes = write(path, 2);
        byte[] bytes = Arrays.copyOf(Files.readAllBytes(path), (int) sizes[1] + 16);
        Arrays.fill(bytes, (int) sizes[1], bytes.length, (byte) 0x7F);
        Files.write(path, bytes);

        assertThrows(WalCorruptedException.class, () -> open(path));
    }

    @Test
    void rejectsAFileThatIsNotALog() throws IOException {
        Path path = dir.resolve("not-a-wal");
        Files.write(path, new byte[64]);
        assertThrows(WalCorruptedException.class, () -> open(path));
    }

    @Test
    void fileCutShortWhileBeingCreatedOpensEmpty() throws IOException {
        Path path = dir.resolve("wal");
        Files.write(path, new byte[] {0x56, 0x44, 0x42});
        try (WriteAheadLog log = WriteAheadLog.open(path, SyncPolicy.MANUAL, 0, e -> {})) {
            assertEquals(0, log.replayedEntries());
            assertEquals(1, log.append(Op.ADD, 1, vector(1)));
        }
        assertEquals(1, open(path).size());
    }

    @Test
    void rejectsMisuse() throws IOException {
        WriteAheadLog log = WriteAheadLog.open(dir.resolve("wal"), SyncPolicy.MANUAL, 0, e -> {});
        assertThrows(IllegalArgumentException.class, () -> log.append(Op.ADD, 1, null));
        assertThrows(IllegalArgumentException.class, () -> log.append(Op.REMOVE, 1, vector(1)));
        assertThrows(IllegalArgumentException.class, () -> log.append(Op.ADD, 1, new float[0]));
        log.close();
        assertThrows(IllegalStateException.class, () -> log.append(Op.ADD, 1, vector(1)));
    }
}
