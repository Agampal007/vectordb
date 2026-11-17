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
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Supplier;

/**
 * An {@link HnswIndex} whose contents survive a crash, safe to use from many threads.
 *
 * <p>Every {@link #add}, {@link #upsert} and {@link #remove} is appended to a write-ahead log
 * before it is applied in memory. With {@link SyncPolicy#EVERY_WRITE}, a call that returns has been
 * forced to the storage device. Opening a directory loads the latest snapshot, if any, and replays
 * the logged operations that came after it.
 *
 * <p>Replay goes through the same code as the original calls, and the index's level generator is
 * part of the snapshot, so a recovered index has exactly the graph it would have had without the
 * crash.
 *
 * <p>{@link #snapshot()} writes the whole index and then empties the log. If the process dies
 * between those two steps, the log still holds operations the snapshot already contains; each
 * record's sequence number lets recovery skip them instead of applying them twice.
 *
 * <h2>Locking</h2>
 *
 * <p>Two locks, always taken in the same order:
 *
 * <ul>
 *   <li>The <em>writer lock</em> admits one writer at a time and is held across the log append and
 *       the change in memory. Operations therefore reach the log in the order they are applied,
 *       which replay depends on.
 *   <li>The <em>graph lock</em> is a read-write lock around the in-memory index. Searches share
 *       it; a writer takes it exclusively only to make its change, after its log append has
 *       finished and after the insert has been worked out.
 * </ul>
 *
 * <p>An insert spends nearly all its time finding the new vector's neighbours, which reads the
 * graph without changing it. Only the writer lock is held for that, so searches carry on
 * alongside; the holder of the writer lock is the only thread that can change the graph, so what
 * it reads cannot move under it. The graph lock is then taken exclusively just to link the new
 * node in, a handful of array copies.
 *
 * <p>So searches wait only for that linking step, and never for disk: not for a log append, not
 * for an fsync, and not for a snapshot, which only reads the index while holding the writer lock.
 *
 * <p>The directory is also locked against a second opener, in this process or another; see
 * {@link DirectoryLock}.
 */
public final class DurableIndex implements VectorIndex, Closeable {

    /** What opening the directory found. */
    public record Recovery(
            boolean snapshotLoaded,
            long snapshotSequence,
            long replayedEntries,
            long skippedEntries,
            long truncatedBytes,
            boolean unfinishedSnapshotDiscarded) {}

    static final String SNAPSHOT_FILE = "snapshot.bin";
    static final String SNAPSHOT_TEMPORARY = "snapshot.tmp";
    static final String LOG_FILE = "wal.log";

    private final Path directory;
    private final HnswIndex index;
    private final WriteAheadLog log;
    private final Recovery recovery;
    private final DirectoryLock directoryLock;
    private final ReentrantLock writerLock = new ReentrantLock();
    private final ReentrantReadWriteLock graphLock = new ReentrantReadWriteLock();
    private volatile boolean closed;

    private DurableIndex(
            Path directory, HnswIndex index, WriteAheadLog log, Recovery recovery, DirectoryLock directoryLock) {
        this.directory = directory;
        this.index = index;
        this.log = log;
        this.recovery = recovery;
        this.directoryLock = directoryLock;
    }

    /**
     * Opens the index stored in {@code directory}, creating it if the directory is empty.
     *
     * @param emptyIndex creates the index when there is no snapshot to load. It must return the
     *     same parameters and seed every time, because the log is replayed into it.
     * @throws IndexLockedException if the directory is already open, here or in another process
     * @throws CorruptSnapshotException if the snapshot fails its checksum
     * @throws io.github.agampal007.vectordb.wal.WalCorruptedException if the log is damaged before
     *     its tail
     */
    public static DurableIndex open(Path directory, Supplier<HnswIndex> emptyIndex, SyncPolicy policy)
            throws IOException {
        // The lock comes first: everything after it, starting with deleting a half-written
        // snapshot, would damage a directory that another process is using.
        DirectoryLock directoryLock = DirectoryLock.acquire(directory);
        try {
            return open(directory, emptyIndex, policy, directoryLock);
        } catch (IOException | RuntimeException e) {
            directoryLock.close();
            throw e;
        }
    }

    private static DurableIndex open(
            Path directory, Supplier<HnswIndex> emptyIndex, SyncPolicy policy, DirectoryLock directoryLock)
            throws IOException {
        // A snapshot that was still being written when the process died was never renamed into place.
        boolean unfinishedSnapshot = Files.deleteIfExists(directory.resolve(SNAPSHOT_TEMPORARY));

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
        Recovery recovery = new Recovery(
                snapshotLoaded, snapshotSequence, replayed[0], skipped[0], log.truncatedBytes(), unfinishedSnapshot);
        return new DurableIndex(directory, index, log, recovery, directoryLock);
    }

    /** Applies one logged operation. An ADD for an id that is present replaces it, as {@link #upsert} does. */
    private static void apply(HnswIndex index, Entry entry) {
        if (entry.op() == Op.ADD) {
            index.remove(entry.id());
            index.add(entry.id(), entry.vector());
        } else if (!index.remove(entry.id())) {
            throw new IllegalStateException("log removes id " + entry.id() + ", which is not in the index");
        }
    }

    /**
     * Logs the vector, then adds it. Invalid input is rejected before anything is logged, so the
     * log never holds an operation that would fail on replay.
     *
     * @throws IllegalArgumentException if the id is already present or the dimension is wrong
     * @throws UncheckedIOException if the log cannot be written; the vector is then not added
     */
    @Override
    public void add(long id, float[] vector) {
        writerLock.lock();
        try {
            ensureOpen();
            checkDimension(vector);
            // Reading without the graph lock is safe here: only the holder of the writer lock
            // changes the index.
            if (index.contains(id)) {
                throw new IllegalArgumentException("duplicate id: " + id);
            }
            append(Op.ADD, id, vector);
            HnswIndex.PendingAdd pending = index.prepareAdd(id, vector); // searches keep running
            graphLock.writeLock().lock();
            try {
                index.commit(pending);
            } finally {
                graphLock.writeLock().unlock();
            }
        } finally {
            writerLock.unlock();
        }
    }

    /**
     * Stores the vector under {@code id}, replacing the one already there if any. It is one log
     * record, so after a crash the id has either its old vector or its new one, never neither.
     *
     * @return whether a vector was replaced
     * @throws UncheckedIOException if the log cannot be written; nothing is then changed
     */
    public boolean upsert(long id, float[] vector) {
        writerLock.lock();
        try {
            ensureOpen();
            checkDimension(vector);
            append(Op.ADD, id, vector);
            // Preparing before the old vector is removed gives the same result as after: removal
            // only marks the node, and building the graph does not look at the marks.
            HnswIndex.PendingAdd pending = index.prepareAdd(id, vector);
            graphLock.writeLock().lock();
            try {
                boolean replaced = index.remove(id);
                index.commit(pending);
                return replaced;
            } finally {
                graphLock.writeLock().unlock();
            }
        } finally {
            writerLock.unlock();
        }
    }

    @Override
    public boolean remove(long id) {
        writerLock.lock();
        try {
            ensureOpen();
            if (!index.contains(id)) {
                return false;
            }
            append(Op.REMOVE, id, null);
            graphLock.writeLock().lock();
            try {
                return index.remove(id);
            } finally {
                graphLock.writeLock().unlock();
            }
        } finally {
            writerLock.unlock();
        }
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
     * instead of replaying every operation. Writers wait until it finishes; searches do not.
     */
    public void snapshot() throws IOException {
        writerLock.lock();
        try {
            ensureOpen();
            writeSnapshot();
            log.reset();
        } finally {
            writerLock.unlock();
        }
    }

    /**
     * Takes a snapshot if the log has grown past {@code maxLogBytes}, which bounds how much there
     * is to replay after a crash.
     *
     * @return whether a snapshot was taken
     */
    public boolean snapshotIfLogExceeds(long maxLogBytes) throws IOException {
        writerLock.lock();
        try {
            ensureOpen();
            if (log.sizeBytes() <= maxLogBytes) {
                return false;
            }
            snapshot();
            return true;
        } finally {
            writerLock.unlock();
        }
    }

    /** The first half of {@link #snapshot()}, separate so tests can stop between the two steps. */
    void writeSnapshot() throws IOException {
        writerLock.lock();
        try {
            Snapshot.write(
                    directory.resolve(SNAPSHOT_FILE),
                    directory.resolve(SNAPSHOT_TEMPORARY),
                    index,
                    log.lastSequence());
        } finally {
            writerLock.unlock();
        }
    }

    /** Forces logged operations to the storage device; needed only with {@link SyncPolicy#MANUAL}. */
    public void sync() throws IOException {
        writerLock.lock();
        try {
            ensureOpen();
            log.sync();
        } finally {
            writerLock.unlock();
        }
    }

    /** Size of the log file: what a restart would have to replay. */
    public long logSizeBytes() throws IOException {
        writerLock.lock();
        try {
            ensureOpen();
            return log.sizeBytes();
        } finally {
            writerLock.unlock();
        }
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
        graphLock.readLock().lock();
        try {
            ensureOpen();
            return index.size();
        } finally {
            graphLock.readLock().unlock();
        }
    }

    @Override
    public boolean contains(long id) {
        graphLock.readLock().lock();
        try {
            ensureOpen();
            return index.contains(id);
        } finally {
            graphLock.readLock().unlock();
        }
    }

    @Override
    public float[] get(long id) {
        graphLock.readLock().lock();
        try {
            ensureOpen();
            return index.get(id);
        } finally {
            graphLock.readLock().unlock();
        }
    }

    @Override
    public List<SearchResult> search(float[] query, int k) {
        return search(query, k, index.efSearch());
    }

    /** As {@link #search(float[], int)}, with a beam width for this query only. */
    public List<SearchResult> search(float[] query, int k, int efSearch) {
        graphLock.readLock().lock();
        try {
            ensureOpen();
            return index.search(query, k, efSearch);
        } finally {
            graphLock.readLock().unlock();
        }
    }

    /** Closes the log and gives up the directory. Waits for running operations; later ones fail. */
    @Override
    public void close() throws IOException {
        writerLock.lock();
        try {
            graphLock.writeLock().lock();
            try {
                if (closed) {
                    return;
                }
                closed = true;
            } finally {
                graphLock.writeLock().unlock();
            }
            try {
                log.close();
            } finally {
                directoryLock.close();
            }
        } finally {
            writerLock.unlock();
        }
    }

    private void ensureOpen() {
        if (closed) {
            throw new IllegalStateException(directory + " is closed");
        }
    }

    private void checkDimension(float[] vector) {
        if (vector.length != index.dimension()) {
            throw new IllegalArgumentException(
                    "expected dimension " + index.dimension() + ", got " + vector.length);
        }
    }
}
