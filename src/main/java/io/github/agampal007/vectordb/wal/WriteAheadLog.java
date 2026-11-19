package io.github.agampal007.vectordb.wal;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;
import java.util.zip.CRC32;

/**
 * An append-only log of index operations, written before each operation is applied so that a
 * crash cannot lose an acknowledged write.
 *
 * <p>File layout, all big-endian:
 *
 * <pre>
 * file    := header record*
 * header  := magic:int32 version:int32 base:int64 baseTerm:int64 crc32:int32   (CRC of the 24 bytes before it)
 * record  := length:int32 crc32:int32 payload[length]
 * payload := sequence:int64 term:int64 op:int8 id:int64 [dimension:int32 float32*dimension]   (vector for ADD only)
 * </pre>
 *
 * <p>Records hold increasing sequence numbers above {@code base}, which is 0 for a new log and the
 * snapshot's sequence number after {@link #reset()}. Each record also carries the term in which it
 * was first written: the leadership term of the node that accepted the operation. Terms never go
 * down along a log. Two logs that hold the same sequence number with the same term hold the same
 * operations up to that point, which is how a follower and its leader check that they agree.
 *
 * <p>Opening the log replays every intact record and then decides what to do with whatever follows:
 *
 * <ul>
 *   <li>A record that is cut short, or whose checksum fails and which is the last thing in the
 *       file, is a torn write from a crash. It was never acknowledged, so it is truncated away.
 *   <li>A record whose checksum fails with more data after it is corruption. Truncating there
 *       would drop acknowledged writes, so opening fails with {@link WalCorruptedException}.
 *   <li>A header that is cut short or fails its checksum, with nothing after it, is a crash while
 *       the file was being created or reset. The header is written again, at the position given
 *       by the caller, which is the snapshot's.
 * </ul>
 *
 * <p>Version 1 logs, written before terms existed, are still read, with every term 0. They are
 * not appended to: {@link #reset()} rewrites the file in the current format.
 *
 * <p>Not thread-safe.
 */
public final class WriteAheadLog implements Closeable {

    /** When appended records are forced to the storage device. */
    public enum SyncPolicy {
        /** fsync before {@link #append} returns: an acknowledged write survives power loss. */
        EVERY_WRITE,
        /** fsync only on {@link #sync()}, {@link #reset()} and {@link #close()}; the caller batches. */
        MANUAL
    }

    public enum Op {
        ADD,
        REMOVE
    }

    /** One logged operation. {@code vector} is null for {@link Op#REMOVE}. */
    public record Entry(long sequence, long term, Op op, long id, float[] vector) {}

    private static final int MAGIC = 0x56444257; // "VDBW"
    private static final int VERSION = 2;
    /** Header of the current version: magic, version, base, base term and a CRC. */
    static final int FILE_HEADER_BYTES = 4 + 4 + 8 + 8 + 4;
    private static final int V1_HEADER_BYTES = 8;
    private static final int RECORD_HEADER_BYTES = 8;
    private static final int FIXED_PAYLOAD_BYTES = 8 + 8 + 1 + 8;
    private static final int V1_FIXED_PAYLOAD_BYTES = 8 + 1 + 8;
    private static final int MAX_DIMENSION = 65_536;
    private static final int MAX_PAYLOAD_BYTES = FIXED_PAYLOAD_BYTES + 4 + 4 * MAX_DIMENSION;

    private final Path path;
    private final FileChannel channel;
    private final SyncPolicy policy;
    private int version = VERSION;
    private long base;
    private long baseTerm;
    private long lastSequence;
    private long lastTerm;
    private long syncedSequence;
    private long truncatedBytes;
    private long replayedEntries;
    private boolean usable = true;

    /** Sequence number, term and file offset of each record, in file order. */
    private long[] sequences = new long[64];
    private long[] terms = new long[64];
    private long[] offsets = new long[64];
    private int records;

    private WriteAheadLog(Path path, FileChannel channel, SyncPolicy policy) {
        this.path = path;
        this.channel = channel;
        this.policy = policy;
    }

    /** As {@link #open(Path, SyncPolicy, long, long, Consumer)} with term 0. */
    public static WriteAheadLog open(Path path, SyncPolicy policy, long minSequence, Consumer<Entry> replay)
            throws IOException {
        return open(path, policy, minSequence, 0, replay);
    }

    /**
     * Opens or creates the log, passing every intact record to {@code replay} in order.
     *
     * @param minSequence a sequence number already known to be durable elsewhere (a snapshot's);
     *     new records are numbered above both it and everything in the log, and a new or reset
     *     log starts there
     * @param minTerm the term of the operation at {@code minSequence}
     * @throws WalCorruptedException if the log is damaged before its tail
     */
    public static WriteAheadLog open(
            Path path, SyncPolicy policy, long minSequence, long minTerm, Consumer<Entry> replay) throws IOException {
        FileChannel channel =
                FileChannel.open(path, StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
        try {
            WriteAheadLog log = new WriteAheadLog(path, channel, policy);
            log.recover(minSequence, minTerm, replay);
            if (minSequence > log.lastSequence) {
                log.lastSequence = minSequence;
                log.lastTerm = minTerm;
            }
            log.syncedSequence = log.lastSequence; // recover() forced the file
            return log;
        } catch (IOException | RuntimeException e) {
            channel.close();
            throw e;
        }
    }

    private void recover(long minSequence, long minTerm, Consumer<Entry> replay) throws IOException {
        long size = channel.size();
        long position = readHeader(size, minSequence, minTerm);
        int fixedPayload = version == 1 ? V1_FIXED_PAYLOAD_BYTES : FIXED_PAYLOAD_BYTES;
        while (size - position >= RECORD_HEADER_BYTES) {
            ByteBuffer header = readFully(RECORD_HEADER_BYTES, position);
            int length = header.getInt();
            int expectedCrc = header.getInt();
            if (length < fixedPayload || length > MAX_PAYLOAD_BYTES) {
                if (isZeroFrom(position, size)) {
                    break; // the file was extended but the data never reached it
                }
                throw new WalCorruptedException(path, position, "impossible record length " + length);
            }
            long end = position + RECORD_HEADER_BYTES + length;
            if (end > size) {
                break; // torn write: the payload was cut short
            }
            ByteBuffer payload = readFully(length, position + RECORD_HEADER_BYTES);
            if (crcOf(payload.array(), 0, length) != expectedCrc) {
                if (end == size) {
                    break; // the last record: cannot be told apart from a torn write
                }
                throw new WalCorruptedException(path, position, "checksum mismatch");
            }
            Entry entry = decode(payload, version);
            if (entry == null) {
                throw new WalCorruptedException(path, position, "malformed record");
            }
            if (version == 1 && records == 0) {
                base = entry.sequence() - 1;
            }
            if (entry.sequence() <= (records > 0 ? sequences[records - 1] : base)) {
                throw new WalCorruptedException(path, position, "sequence number went backwards");
            }
            if (entry.term() < (records > 0 ? terms[records - 1] : baseTerm)) {
                throw new WalCorruptedException(path, position, "term went backwards");
            }
            index(entry.sequence(), entry.term(), position);
            replayedEntries++;
            replay.accept(entry);
            position = end;
        }
        if (position < size) {
            truncatedBytes += size - position;
            channel.truncate(position);
        }
        channel.force(true);
        channel.position(position);
        if (version == 1 && records == 0) {
            base = minSequence; // an emptied version 1 log does not record where it starts
        }
        lastSequence = records > 0 ? sequences[records - 1] : base;
        lastTerm = records > 0 ? terms[records - 1] : baseTerm;
    }

    /** Reads the header, or writes a new one if there is none, and returns where the records start. */
    private long readHeader(long size, long minSequence, long minTerm) throws IOException {
        if (size < V1_HEADER_BYTES) {
            return startOver(size, minSequence, minTerm); // a new file, or a crash while creating one
        }
        ByteBuffer start = readFully(V1_HEADER_BYTES, 0);
        int magic = start.getInt();
        int fileVersion = start.getInt();
        if (magic != MAGIC) {
            if (isZeroFrom(0, size)) {
                return startOver(size, minSequence, minTerm); // extended, but the header never arrived
            }
            throw new WalCorruptedException(path, 0, "not a write-ahead log");
        }
        if (fileVersion == 1) {
            version = 1;
            base = 0;
            baseTerm = 0;
            return V1_HEADER_BYTES;
        }
        if (fileVersion != VERSION) {
            throw new WalCorruptedException(path, 4, "unsupported version " + fileVersion);
        }
        if (size >= FILE_HEADER_BYTES) {
            ByteBuffer header = readFully(FILE_HEADER_BYTES, 0);
            if (crcOf(header.array(), 0, FILE_HEADER_BYTES - 4) == header.getInt(FILE_HEADER_BYTES - 4)) {
                version = VERSION;
                base = header.getLong(8);
                baseTerm = header.getLong(16);
                return FILE_HEADER_BYTES;
            }
        }
        // A damaged header. Records are only ever appended after a complete header, so if nothing
        // follows it, this is a crash while the header was being written.
        if (size <= FILE_HEADER_BYTES || isZeroFrom(FILE_HEADER_BYTES, size)) {
            return startOver(size, minSequence, minTerm);
        }
        throw new WalCorruptedException(path, 0, "header checksum mismatch");
    }

    private long startOver(long size, long minSequence, long minTerm) throws IOException {
        truncatedBytes += size;
        writeHeader(minSequence, minTerm);
        return FILE_HEADER_BYTES;
    }

    /** Empties the file and writes a header saying that the records start after {@code base}. */
    private void writeHeader(long base, long baseTerm) throws IOException {
        channel.truncate(0);
        ByteBuffer header = ByteBuffer.allocate(FILE_HEADER_BYTES)
                .putInt(MAGIC)
                .putInt(VERSION)
                .putLong(base)
                .putLong(baseTerm);
        header.putInt(crcOf(header.array(), 0, FILE_HEADER_BYTES - 4)).flip();
        writeFully(header, 0);
        channel.force(true);
        channel.position(FILE_HEADER_BYTES);
        this.version = VERSION;
        this.base = base;
        this.baseTerm = baseTerm;
        this.records = 0;
    }

    /**
     * Appends one operation, numbered one above the last, and returns its sequence number. Under
     * {@link SyncPolicy#EVERY_WRITE} the record is on the storage device when this returns.
     *
     * <p>If the write or the fsync fails, the state of the file is unknown, so the log refuses
     * further appends; reopen it to recover.
     *
     * @param term the term to record; never lower than the last record's
     */
    public long append(long term, Op op, long id, float[] vector) throws IOException {
        long sequence = lastSequence + 1;
        write(sequence, term, op, id, vector);
        return sequence;
    }

    /** Appends in the same term as the last record. */
    public long append(Op op, long id, float[] vector) throws IOException {
        return append(lastTerm, op, id, vector);
    }

    /**
     * Appends an operation copied from another log, keeping its sequence number and term. It must
     * be the next one: its sequence number is one above this log's last.
     */
    public void append(Entry entry) throws IOException {
        if (entry.sequence() != lastSequence + 1) {
            throw new IllegalArgumentException(
                    "expected sequence number " + (lastSequence + 1) + ", got " + entry.sequence());
        }
        write(entry.sequence(), entry.term(), entry.op(), entry.id(), entry.vector());
    }

    private void write(long sequence, long term, Op op, long id, float[] vector) throws IOException {
        if (!usable) {
            throw new IllegalStateException("write-ahead log is closed or failed: " + path);
        }
        if (version != VERSION) {
            throw new IllegalStateException(path + " is in an older format; reset it before appending");
        }
        if (term < lastTerm) {
            throw new IllegalArgumentException("term " + term + " is below the log's last term " + lastTerm);
        }
        if ((op == Op.ADD) != (vector != null)) {
            throw new IllegalArgumentException("ADD needs a vector and REMOVE must not have one");
        }
        if (vector != null && (vector.length == 0 || vector.length > MAX_DIMENSION)) {
            throw new IllegalArgumentException("vector dimension out of range: " + vector.length);
        }
        int length = FIXED_PAYLOAD_BYTES + (vector == null ? 0 : 4 + 4 * vector.length);
        ByteBuffer record = ByteBuffer.allocate(RECORD_HEADER_BYTES + length);
        record.position(RECORD_HEADER_BYTES);
        record.putLong(sequence).putLong(term).put((byte) (op.ordinal() + 1)).putLong(id);
        if (vector != null) {
            record.putInt(vector.length);
            for (float value : vector) {
                record.putFloat(value);
            }
        }
        record.putInt(0, length).putInt(4, crcOf(record.array(), RECORD_HEADER_BYTES, length));
        record.position(0);

        long start = channel.position();
        try {
            while (record.hasRemaining()) {
                channel.write(record);
            }
            if (policy == SyncPolicy.EVERY_WRITE) {
                channel.force(false);
            }
        } catch (IOException e) {
            usable = false;
            try {
                channel.truncate(start);
            } catch (IOException ignored) {
                // Recovery on the next open deals with whatever is left.
            }
            throw e;
        }
        index(sequence, term, start);
        lastSequence = sequence;
        lastTerm = term;
        if (policy == SyncPolicy.EVERY_WRITE) {
            syncedSequence = sequence;
        }
    }

    /** Forces everything appended so far to the storage device. */
    public void sync() throws IOException {
        channel.force(false);
        syncedSequence = lastSequence;
    }

    /**
     * Discards every record, so that the log starts after {@link #lastSequence()}. Call only after
     * a snapshot covering that sequence number is durable. Sequence numbers keep counting from
     * where they were. Also brings a version 1 log up to the current format.
     */
    public void reset() throws IOException {
        if (!usable) {
            throw new IllegalStateException("write-ahead log is closed or failed: " + path);
        }
        writeHeader(lastSequence, lastTerm);
        syncedSequence = lastSequence;
    }

    /**
     * Copies whole records after {@code sequence}, byte for byte as stored, for a follower to
     * append with {@link #decodeRecords}. Only records already forced to the storage device are
     * included, so a follower never holds an operation that this log could still lose. Stops
     * before {@code maxBytes}, unless the first record alone is larger.
     *
     * @return the records, or an empty array if there are none yet
     * @throws IllegalArgumentException if {@code sequence} is below {@link #base()}: the records
     *     after it are no longer in this log
     */
    public byte[] readAfter(long sequence, int maxBytes) throws IOException {
        if (sequence < base) {
            throw new IllegalArgumentException("sequence " + sequence + " is below the log's base " + base);
        }
        if (version != VERSION) {
            throw new IllegalStateException(path + " is in an older format");
        }
        int first = firstAbove(sequence);
        int end = first;
        long bytes = 0;
        while (end < records && sequences[end] <= syncedSequence) {
            long size = endOf(end) - offsets[end];
            if (end > first && bytes + size > maxBytes) {
                break;
            }
            bytes += size;
            end++;
        }
        if (end == first) {
            return new byte[0];
        }
        return readFully(Math.toIntExact(bytes), offsets[first]).array();
    }

    /**
     * Parses records copied by {@link #readAfter}, checking each one's checksum.
     *
     * @throws IOException if the bytes are not whole, intact records
     */
    public static List<Entry> decodeRecords(byte[] bytes) throws IOException {
        List<Entry> entries = new ArrayList<>();
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        while (buffer.hasRemaining()) {
            if (buffer.remaining() < RECORD_HEADER_BYTES) {
                throw new IOException("replicated records are cut short");
            }
            int length = buffer.getInt();
            int expectedCrc = buffer.getInt();
            if (length < FIXED_PAYLOAD_BYTES || length > MAX_PAYLOAD_BYTES || length > buffer.remaining()) {
                throw new IOException("replicated record has an impossible length " + length);
            }
            if (crcOf(bytes, buffer.position(), length) != expectedCrc) {
                throw new IOException("replicated record failed its checksum");
            }
            Entry entry = decode(ByteBuffer.wrap(bytes, buffer.position(), length).slice(), VERSION);
            if (entry == null) {
                throw new IOException("replicated record is malformed");
            }
            entries.add(entry);
            buffer.position(buffer.position() + length);
        }
        return entries;
    }

    /**
     * The term of the operation at {@code sequence}, if this log knows it: for a record it holds,
     * for its base, or for its last sequence number. Otherwise -1.
     */
    public long termAt(long sequence) {
        if (sequence == lastSequence) {
            return lastTerm;
        }
        if (sequence == base) {
            return baseTerm;
        }
        int i = Arrays.binarySearch(sequences, 0, records, sequence);
        return i >= 0 ? terms[i] : -1;
    }

    /** Sequence number of the newest record, or the {@code minSequence} given at open. */
    public long lastSequence() {
        return lastSequence;
    }

    /** Term of the operation at {@link #lastSequence()}. */
    public long lastTerm() {
        return lastTerm;
    }

    /** The sequence number just before this log's first record: everything up to it is in a snapshot. */
    public long base() {
        return base;
    }

    /** The newest sequence number known to be on the storage device. */
    public long syncedSequence() {
        return syncedSequence;
    }

    /** Records currently in the file. */
    public int recordCount() {
        return records;
    }

    /** Whether the file is in a format that predates terms and must be reset before appending. */
    public boolean isOldFormat() {
        return version != VERSION;
    }

    /** Bytes of torn tail, or of a torn header, dropped when the log was opened. */
    public long truncatedBytes() {
        return truncatedBytes;
    }

    /** Records read back when the log was opened. */
    public long replayedEntries() {
        return replayedEntries;
    }

    public long sizeBytes() throws IOException {
        return channel.size();
    }

    @Override
    public void close() throws IOException {
        if (channel.isOpen()) {
            usable = false;
            try {
                channel.force(false);
            } finally {
                channel.close();
            }
        }
    }

    private void index(long sequence, long term, long offset) {
        if (records == sequences.length) {
            int capacity = records * 2;
            sequences = Arrays.copyOf(sequences, capacity);
            terms = Arrays.copyOf(terms, capacity);
            offsets = Arrays.copyOf(offsets, capacity);
        }
        sequences[records] = sequence;
        terms[records] = term;
        offsets[records] = offset;
        records++;
    }

    /** Index of the first record with a sequence number above {@code sequence}. */
    private int firstAbove(long sequence) {
        int i = Arrays.binarySearch(sequences, 0, records, sequence);
        return i >= 0 ? i + 1 : -i - 1;
    }

    private long endOf(int record) throws IOException {
        return record + 1 < records ? offsets[record + 1] : channel.position();
    }

    /** Decodes one payload, or returns null if it is malformed. */
    private static Entry decode(ByteBuffer payload, int version) {
        long sequence = payload.getLong();
        long term = version == 1 ? 0 : payload.getLong();
        int opCode = payload.get();
        long id = payload.getLong();
        if (opCode == Op.REMOVE.ordinal() + 1 && !payload.hasRemaining()) {
            return new Entry(sequence, term, Op.REMOVE, id, null);
        }
        if (opCode == Op.ADD.ordinal() + 1 && payload.remaining() >= 4) {
            int dimension = payload.getInt();
            if (dimension > 0 && payload.remaining() == 4 * dimension) {
                float[] vector = new float[dimension];
                payload.asFloatBuffer().get(vector);
                return new Entry(sequence, term, Op.ADD, id, vector);
            }
        }
        return null;
    }

    private static int crcOf(byte[] bytes, int offset, int length) {
        CRC32 crc = new CRC32();
        crc.update(bytes, offset, length);
        return (int) crc.getValue();
    }

    private ByteBuffer readFully(int length, long position) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(length);
        while (buffer.hasRemaining()) {
            if (channel.read(buffer, position + buffer.position()) < 0) {
                throw new IOException(path + ": unexpected end of file at byte " + (position + buffer.position()));
            }
        }
        return buffer.flip();
    }

    private void writeFully(ByteBuffer buffer, long position) throws IOException {
        while (buffer.hasRemaining()) {
            position += channel.write(buffer, position);
        }
    }

    private boolean isZeroFrom(long position, long size) throws IOException {
        while (position < size) {
            ByteBuffer chunk = readFully((int) Math.min(8192, size - position), position);
            while (chunk.hasRemaining()) {
                if (chunk.get() != 0) {
                    return false;
                }
            }
            position += chunk.limit();
        }
        return true;
    }
}
