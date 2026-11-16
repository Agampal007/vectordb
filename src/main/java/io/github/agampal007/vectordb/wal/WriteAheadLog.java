package io.github.agampal007.vectordb.wal;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.function.Consumer;
import java.util.zip.CRC32;

/**
 * An append-only log of index operations, written before each operation is applied so that a
 * crash cannot lose an acknowledged write.
 *
 * <p>File layout, all big-endian:
 *
 * <pre>
 * file   := magic:int32 version:int32 record*
 * record := length:int32 crc32:int32 payload[length]
 * payload:= sequence:int64 op:int8 id:int64 [dimension:int32 float32*dimension]   (vector for ADD only)
 * </pre>
 *
 * <p>Opening the log replays every intact record and then decides what to do with whatever follows:
 *
 * <ul>
 *   <li>A record that is cut short, or whose checksum fails and which is the last thing in the
 *       file, is a torn write from a crash. It was never acknowledged, so it is truncated away.
 *   <li>A record whose checksum fails with more data after it is corruption. Truncating there
 *       would drop acknowledged writes, so opening fails with {@link WalCorruptedException}.
 * </ul>
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
    public record Entry(long sequence, Op op, long id, float[] vector) {}

    private static final int MAGIC = 0x56444257; // "VDBW"
    private static final int VERSION = 1;
    private static final int FILE_HEADER_BYTES = 8;
    private static final int RECORD_HEADER_BYTES = 8;
    private static final int FIXED_PAYLOAD_BYTES = 8 + 1 + 8;
    private static final int MAX_DIMENSION = 65_536;
    private static final int MAX_PAYLOAD_BYTES = FIXED_PAYLOAD_BYTES + 4 + 4 * MAX_DIMENSION;

    private final Path path;
    private final FileChannel channel;
    private final SyncPolicy policy;
    private long lastSequence;
    private long truncatedBytes;
    private long replayedEntries;
    private boolean usable = true;

    private WriteAheadLog(Path path, FileChannel channel, SyncPolicy policy) {
        this.path = path;
        this.channel = channel;
        this.policy = policy;
    }

    /**
     * Opens or creates the log, passing every intact record to {@code replay} in order.
     *
     * @param minSequence a sequence number already known to be durable elsewhere (a snapshot's);
     *     new records are numbered above both it and everything in the log
     * @throws WalCorruptedException if the log is damaged before its tail
     */
    public static WriteAheadLog open(Path path, SyncPolicy policy, long minSequence, Consumer<Entry> replay)
            throws IOException {
        FileChannel channel =
                FileChannel.open(path, StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
        try {
            WriteAheadLog log = new WriteAheadLog(path, channel, policy);
            log.recover(replay);
            log.lastSequence = Math.max(log.lastSequence, minSequence);
            return log;
        } catch (IOException | RuntimeException e) {
            channel.close();
            throw e;
        }
    }

    private void recover(Consumer<Entry> replay) throws IOException {
        long size = channel.size();
        if (size < FILE_HEADER_BYTES) {
            // A new file, or a crash while creating one: no record can have been written yet.
            truncatedBytes = size;
            channel.truncate(0);
            ByteBuffer header = ByteBuffer.allocate(FILE_HEADER_BYTES).putInt(MAGIC).putInt(VERSION).flip();
            writeFully(header, 0);
            channel.force(true);
            channel.position(FILE_HEADER_BYTES);
            return;
        }
        ByteBuffer fileHeader = readFully(FILE_HEADER_BYTES, 0);
        if (fileHeader.getInt() != MAGIC) {
            throw new WalCorruptedException(path, 0, "not a write-ahead log");
        }
        int version = fileHeader.getInt();
        if (version != VERSION) {
            throw new WalCorruptedException(path, 4, "unsupported version " + version);
        }

        long position = FILE_HEADER_BYTES;
        while (size - position >= RECORD_HEADER_BYTES) {
            ByteBuffer header = readFully(RECORD_HEADER_BYTES, position);
            int length = header.getInt();
            int expectedCrc = header.getInt();
            if (length < FIXED_PAYLOAD_BYTES || length > MAX_PAYLOAD_BYTES) {
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
            Entry entry = decode(payload, position);
            if (entry.sequence() <= lastSequence) {
                throw new WalCorruptedException(path, position, "sequence number went backwards");
            }
            lastSequence = entry.sequence();
            replayedEntries++;
            replay.accept(entry);
            position = end;
        }
        if (position < size) {
            truncatedBytes = size - position;
            channel.truncate(position);
            channel.force(true);
        }
        channel.position(position);
    }

    /**
     * Appends one operation and returns its sequence number. Under {@link SyncPolicy#EVERY_WRITE}
     * the record is on the storage device when this returns.
     *
     * <p>If the write or the fsync fails, the state of the file is unknown, so the log refuses
     * further appends; reopen it to recover.
     */
    public long append(Op op, long id, float[] vector) throws IOException {
        if (!usable) {
            throw new IllegalStateException("write-ahead log is closed or failed: " + path);
        }
        if ((op == Op.ADD) != (vector != null)) {
            throw new IllegalArgumentException("ADD needs a vector and REMOVE must not have one");
        }
        if (vector != null && (vector.length == 0 || vector.length > MAX_DIMENSION)) {
            throw new IllegalArgumentException("vector dimension out of range: " + vector.length);
        }
        long sequence = lastSequence + 1;
        int length = FIXED_PAYLOAD_BYTES + (vector == null ? 0 : 4 + 4 * vector.length);
        ByteBuffer record = ByteBuffer.allocate(RECORD_HEADER_BYTES + length);
        record.position(RECORD_HEADER_BYTES);
        record.putLong(sequence).put((byte) (op.ordinal() + 1)).putLong(id);
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
        lastSequence = sequence;
        return sequence;
    }

    /** Forces everything appended so far to the storage device. */
    public void sync() throws IOException {
        channel.force(false);
    }

    /**
     * Discards every record. Call only after a snapshot covering {@link #lastSequence()} is
     * durable. Sequence numbers keep counting from where they were.
     */
    public void reset() throws IOException {
        channel.truncate(FILE_HEADER_BYTES);
        channel.position(FILE_HEADER_BYTES);
        channel.force(true);
    }

    /** Sequence number of the newest record, or the {@code minSequence} given at open. */
    public long lastSequence() {
        return lastSequence;
    }

    /** Bytes of torn tail dropped when the log was opened. */
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

    private Entry decode(ByteBuffer payload, long position) throws WalCorruptedException {
        long sequence = payload.getLong();
        int opCode = payload.get();
        long id = payload.getLong();
        if (opCode == Op.REMOVE.ordinal() + 1 && !payload.hasRemaining()) {
            return new Entry(sequence, Op.REMOVE, id, null);
        }
        if (opCode == Op.ADD.ordinal() + 1 && payload.remaining() >= 4) {
            int dimension = payload.getInt();
            if (dimension > 0 && payload.remaining() == 4 * dimension) {
                float[] vector = new float[dimension];
                payload.asFloatBuffer().get(vector);
                return new Entry(sequence, Op.ADD, id, vector);
            }
        }
        throw new WalCorruptedException(path, position, "malformed record");
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
