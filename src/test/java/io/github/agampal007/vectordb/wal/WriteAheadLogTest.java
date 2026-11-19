package io.github.agampal007.vectordb.wal;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.agampal007.vectordb.wal.WriteAheadLog.Entry;
import io.github.agampal007.vectordb.wal.WriteAheadLog.Op;
import io.github.agampal007.vectordb.wal.WriteAheadLog.SyncPolicy;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.zip.CRC32;
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
            Files.write(cut, Arrays.copyOf(bytes, (int) (kept == 0 ? WriteAheadLog.FILE_HEADER_BYTES : sizes[kept - 1])));
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
        byte[] text = new byte[64];
        Arrays.fill(text, (byte) 'x');
        Files.write(path, text);
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

    @Test
    void termsAreKeptAndNeverGoDown() throws IOException {
        Path path = dir.resolve("wal");
        try (WriteAheadLog log = WriteAheadLog.open(path, SyncPolicy.MANUAL, 0, e -> {})) {
            assertEquals(1, log.append(1, Op.ADD, 7, vector(1)));
            assertEquals(2, log.append(1, Op.REMOVE, 7, null));
            assertEquals(3, log.append(4, Op.ADD, 8, vector(2)));
            assertThrows(IllegalArgumentException.class, () -> log.append(3, Op.REMOVE, 8, null));
            assertEquals(3, log.lastSequence());
            assertEquals(4, log.lastTerm());
        }
        assertEquals(List.of(1L, 1L, 4L), open(path).stream().map(Entry::term).toList());
        try (WriteAheadLog log = WriteAheadLog.open(path, SyncPolicy.MANUAL, 0, e -> {})) {
            assertEquals(0, log.termAt(0)); // the base of a new log
            assertEquals(1, log.termAt(2));
            assertEquals(4, log.termAt(3));
            assertEquals(-1, log.termAt(4));
            assertEquals(4, log.append(Op.REMOVE, 8, null)); // stays in the last record's term
            assertEquals(4, log.lastTerm());
        }
    }

    @Test
    void copiedRecordsKeepTheirNumbersAndTermsAndMustComeInOrder() throws IOException {
        Path original = dir.resolve("leader");
        List<Entry> source;
        try (WriteAheadLog log = WriteAheadLog.open(original, SyncPolicy.MANUAL, 0, e -> {})) {
            log.append(2, Op.ADD, 1, vector(1));
            log.append(2, Op.ADD, 2, vector(2));
            log.append(5, Op.REMOVE, 1, null);
            log.sync();
            source = WriteAheadLog.decodeRecords(log.readAfter(0, Integer.MAX_VALUE));
        }
        Path copy = dir.resolve("follower");
        try (WriteAheadLog log = WriteAheadLog.open(copy, SyncPolicy.MANUAL, 0, e -> {})) {
            assertThrows(IllegalArgumentException.class, () -> log.append(source.get(1))); // a gap
            for (Entry entry : source) {
                log.append(entry);
            }
            assertThrows(IllegalArgumentException.class, () -> log.append(source.get(2))); // twice
        }
        assertArrayEquals(Files.readAllBytes(original), Files.readAllBytes(copy));
    }

    @Test
    void readAfterSendsOnlyWholeRecordsThatAreOnTheDeviceUpToTheLimit() throws IOException {
        try (WriteAheadLog log = WriteAheadLog.open(dir.resolve("wal"), SyncPolicy.MANUAL, 0, e -> {})) {
            for (int i = 0; i < 5; i++) {
                log.append(1, Op.ADD, i, vector(i));
            }
            assertEquals(0, log.readAfter(0, 1 << 20).length, "nothing has been forced to the device yet");
            log.sync();
            assertEquals(List.of(1L, 2L, 3L, 4L, 5L), sequencesOf(log.readAfter(0, 1 << 20)));
            assertEquals(List.of(3L, 4L, 5L), sequencesOf(log.readAfter(2, 1 << 20)));
            assertEquals(List.of(), sequencesOf(log.readAfter(5, 1 << 20)));
            int one = log.readAfter(0, 1).length;
            assertEquals(List.of(1L), sequencesOf(log.readAfter(0, 1)), "one record even over the limit");
            assertEquals(List.of(1L, 2L), sequencesOf(log.readAfter(0, 3 * one - 1)));
            log.append(1, Op.REMOVE, 0, null);
            assertEquals(List.of(5L), sequencesOf(log.readAfter(4, 1 << 20)), "record 6 is not synced");
        }
    }

    @Test
    void resetStartsTheLogAtTheLastSequenceNumberAndKeepsItsTerm() throws IOException {
        Path path = dir.resolve("wal");
        try (WriteAheadLog log = WriteAheadLog.open(path, SyncPolicy.MANUAL, 0, e -> {})) {
            log.append(3, Op.ADD, 1, vector(1));
            log.append(3, Op.ADD, 2, vector(2));
            log.reset();
            assertEquals(2, log.base());
            assertEquals(3, log.termAt(2));
            assertEquals(-1, log.termAt(1), "gone with the reset");
            assertThrows(IllegalArgumentException.class, () -> log.readAfter(1, 1 << 20));
            log.append(3, Op.ADD, 3, vector(3));
        }
        try (WriteAheadLog log = WriteAheadLog.open(path, SyncPolicy.MANUAL, 0, e -> {})) {
            assertEquals(2, log.base());
            assertEquals(3, log.termAt(2));
            assertEquals(3, log.lastSequence());
            assertEquals(List.of(3L), sequencesOf(log.readAfter(2, 1 << 20)));
        }
    }

    @Test
    void aHeaderTornWhileBeingWrittenStartsAgainFromTheSnapshot() throws IOException {
        Path path = dir.resolve("wal");
        write(path, 3);
        byte[] header = Arrays.copyOf(Files.readAllBytes(path), WriteAheadLog.FILE_HEADER_BYTES);
        header[12] ^= 0x10; // inside the base sequence number
        // A crash part-way through writing a header: some of it, or all of it but not as written,
        // and nothing after it.
        for (int length : new int[] {10, WriteAheadLog.FILE_HEADER_BYTES}) {
            Path torn = dir.resolve("torn-" + length);
            Files.write(torn, Arrays.copyOf(header, length));
            try (WriteAheadLog log = WriteAheadLog.open(torn, SyncPolicy.MANUAL, 7, 2, e -> {})) {
                assertEquals(7, log.base(), "length " + length);
                assertEquals(2, log.termAt(7));
                assertEquals(8, log.append(2, Op.ADD, 1, vector(1)));
            }
        }
        // The same damage with records after it cannot be a torn write.
        byte[] damaged = Files.readAllBytes(path);
        damaged[12] ^= 0x10;
        Files.write(path, damaged);
        assertThrows(WalCorruptedException.class, () -> open(path));
    }

    @Test
    void aVersion1LogIsReadWithTermZeroAndRewrittenByAReset() throws IOException {
        Path path = dir.resolve("old");
        Files.write(path, version1Log(5, 6, 7)); // as left after a version 1 snapshot at 4
        List<Entry> entries = new ArrayList<>();
        try (WriteAheadLog log = WriteAheadLog.open(path, SyncPolicy.MANUAL, 4, entries::add)) {
            assertTrue(log.isOldFormat());
            assertEquals(4, log.base());
            assertEquals(7, log.lastSequence());
            assertThrows(IllegalStateException.class, () -> log.append(0, Op.REMOVE, 105, null));
            log.reset();
            assertFalse(log.isOldFormat());
            assertEquals(8, log.append(0, Op.REMOVE, 105, null));
        }
        assertEquals(List.of(5L, 6L, 7L), entries.stream().map(Entry::sequence).toList());
        assertEquals(List.of(0L, 0L, 0L), entries.stream().map(Entry::term).toList());
        assertEquals(106L, entries.get(1).id());
        assertArrayEquals(vector(6), entries.get(1).vector());
        assertEquals(List.of(8L), open(path).stream().map(Entry::sequence).toList());
    }

    @Test
    void decodeRecordsRejectsAnyDamagedOrIncompleteData() throws IOException {
        byte[] records;
        try (WriteAheadLog log = WriteAheadLog.open(dir.resolve("wal"), SyncPolicy.EVERY_WRITE, 0, e -> {})) {
            log.append(1, Op.ADD, 1, vector(1));
            log.append(1, Op.REMOVE, 1, null);
            records = log.readAfter(0, 1 << 20);
        }
        assertEquals(2, WriteAheadLog.decodeRecords(records).size());
        for (int i = 0; i < records.length; i++) {
            byte[] damaged = records.clone();
            damaged[i] ^= 0x01;
            assertThrows(IOException.class, () -> WriteAheadLog.decodeRecords(damaged), "byte " + i);
        }
        assertThrows(IOException.class, () -> WriteAheadLog.decodeRecords(Arrays.copyOf(records, records.length - 1)));
    }

    private static List<Long> sequencesOf(byte[] records) throws IOException {
        return WriteAheadLog.decodeRecords(records).stream().map(Entry::sequence).toList();
    }

    /** A log as version 1 wrote it: an 8-byte header, then ADD records without a term. */
    static byte[] version1Log(long... sequences) {
        int payloadBytes = 8 + 1 + 8 + 4 + 3 * 4;
        ByteBuffer out = ByteBuffer.allocate(8 + sequences.length * (8 + payloadBytes));
        out.putInt(0x56444257).putInt(1);
        for (long sequence : sequences) {
            ByteBuffer payload = ByteBuffer.allocate(payloadBytes);
            payload.putLong(sequence).put((byte) 1).putLong(sequence + 100).putInt(3);
            for (float value : vector((int) sequence)) {
                payload.putFloat(value);
            }
            CRC32 crc = new CRC32();
            crc.update(payload.array());
            out.putInt(payloadBytes).putInt((int) crc.getValue()).put(payload.array());
        }
        return out.array();
    }
}
