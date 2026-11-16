package io.github.agampal007.vectordb;

import io.github.agampal007.vectordb.wal.WriteAheadLog;
import io.github.agampal007.vectordb.wal.WriteAheadLog.Entry;
import io.github.agampal007.vectordb.wal.WriteAheadLog.Op;
import io.github.agampal007.vectordb.wal.WriteAheadLog.SyncPolicy;
import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.function.Supplier;

/**
 * An {@link HnswIndex} whose contents survive a crash.
 *
 * <p>Every {@link #add} and {@link #remove} is appended to a write-ahead log before it is applied
 * in memory. With {@link SyncPolicy#EVERY_WRITE}, a call that returns has been forced to the
 * storage device. Opening a directory loads the latest snapshot, if any, and replays the logged
 * operations that came after it.
 *
 * <p>Replay goes through the same code as the original calls, and the index's level generator is
 * part of the snapshot, so a recovered index has exactly the graph it would have had without the
 * crash.
 *
 * <p>{@link #snapshot()} writes the whole index and then empties the log. If the process dies
 * between those two steps, the log still holds operations the snapshot already contains; each
 * record's sequence number lets recovery skip them instead of applying them twice.
 *
 * <p>Not thread-safe.
 */
public final class DurableIndex implements VectorIndex, Closeable {

    /** What opening the directory found. */
    public record Recovery(
            boolean snapshotLoaded,
            long snapshotSequence,
            long replayedEntries,
            long skippedEntries,
            long truncatedBytes) {}

    static final String SNAPSHOT_FILE = "snapshot.bin";
    static final String SNAPSHOT_TEMPORARY = "snapshot.tmp";
    static final String LOG_FILE = "wal.log";

    private final Path directory;
    private final HnswIndex index;
    private final WriteAheadLog log;
    private final Recovery recovery;

    private DurableIndex(Path directory, HnswIndex index, WriteAheadLog log, Recovery recovery) {
        this.directory = directory;
        this.index = index;
        this.log = log;
        this.recovery = recovery;
    }

    /**
     * Opens the index stored in {@code directory}, creating it if the directory is empty.
     *
     * @param emptyIndex creates the index when there is no snapshot to load. It must return the
     *     same parameters and seed every time, because the log is replayed into it.
     * @throws CorruptSnapshotException if the snapshot fails its checksum
     * @throws io.github.agampal007.vectordb.wal.WalCorruptedException if the log is damaged before
     *     its tail
     */
    public static DurableIndex open(Path directory, Supplier<HnswIndex> emptyIndex, SyncPolicy policy)
            throws IOException {
        Files.createDirectories(directory);
        // A snapshot that was still being written when the process died was never renamed into place.
        Files.deleteIfExists(directory.resolve(SNAPSHOT_TEMPORARY));

        Path snapshotPath = directory.resolve(SNAPSHOT_FILE);
        boolean snapshotLoaded = Files.exists(snapshotPath);
        HnswIndex index;
        long snapshotSequence = 0;
        if (snapshotLoaded) {
            Snapshot snapshot = Snapshot.read(snapshotPath);
            index = snapshot.index;
            snapshotSequence = snapshot.sequence;
        } else {
            index = emptyIndex.get();
        }

        long[] replayed = {0};
        long[] skipped = {0};
        long alreadyInSnapshot = snapshotSequence;
        WriteAheadLog log;
        try {
            log = WriteAheadLog.open(directory.resolve(LOG_FILE), policy, snapshotSequence, entry -> {
                if (entry.sequence() <= alreadyInSnapshot) {
                    skipped[0]++;
                } else {
                    apply(index, entry);
                    replayed[0]++;
                }
            });
        } catch (IllegalArgumentException | IllegalStateException e) {
            throw new IOException(directory + ": the log does not match the index it is replayed into", e);
        }
        Recovery recovery =
                new Recovery(snapshotLoaded, snapshotSequence, replayed[0], skipped[0], log.truncatedBytes());
        return new DurableIndex(directory, index, log, recovery);
    }

    private static void apply(HnswIndex index, Entry entry) {
        if (entry.op() == Op.ADD) {
            index.add(entry.id(), entry.vector());
        } else if (!index.remove(entry.id())) {
            throw new IllegalStateException("log removes id " + entry.id() + ", which is not in the index");
        }
    }

    /**
     * Logs the vector, then adds it. Invalid input is rejected before anything is logged, so the
     * log never holds an operation that would fail on replay.
     *
     * @throws UncheckedIOException if the log cannot be written; the vector is then not added
     */
    @Override
    public void add(long id, float[] vector) {
        if (vector.length != index.dimension()) {
            throw new IllegalArgumentException(
                    "expected dimension " + index.dimension() + ", got " + vector.length);
        }
        if (index.contains(id)) {
            throw new IllegalArgumentException("duplicate id: " + id);
        }
        append(Op.ADD, id, vector);
        index.add(id, vector);
    }

    @Override
    public boolean remove(long id) {
        if (!index.contains(id)) {
            return false;
        }
        append(Op.REMOVE, id, null);
        return index.remove(id);
    }

    private void append(Op op, long id, float[] vector) {
        try {
            log.append(op, id, vector);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Writes the whole index to disk and empties the log, so the next open loads the snapshot
     * instead of replaying every operation.
     */
    public void snapshot() throws IOException {
        writeSnapshot();
        log.reset();
    }

    /** The first half of {@link #snapshot()}, separate so tests can stop between the two steps. */
    void writeSnapshot() throws IOException {
        Snapshot.write(
                directory.resolve(SNAPSHOT_FILE), directory.resolve(SNAPSHOT_TEMPORARY), index, log.lastSequence());
    }

    /** Forces logged operations to the storage device; needed only with {@link SyncPolicy#MANUAL}. */
    public void sync() throws IOException {
        log.sync();
    }

    public Recovery recovery() {
        return recovery;
    }

    public void setEfSearch(int efSearch) {
        index.setEfSearch(efSearch);
    }

    /** The in-memory index. For tests. */
    HnswIndex index() {
        return index;
    }

    @Override
    public int dimension() {
        return index.dimension();
    }

    @Override
    public Distance distance() {
        return index.distance();
    }

    @Override
    public int size() {
        return index.size();
    }

    @Override
    public boolean contains(long id) {
        return index.contains(id);
    }

    @Override
    public float[] get(long id) {
        return index.get(id);
    }

    @Override
    public List<SearchResult> search(float[] query, int k) {
        return index.search(query, k);
    }

    @Override
    public void close() throws IOException {
        log.close();
    }
}
