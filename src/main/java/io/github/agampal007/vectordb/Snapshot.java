package io.github.agampal007.vectordb;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.zip.CRC32;
import java.util.zip.CheckedInputStream;
import java.util.zip.CheckedOutputStream;

/**
 * A point-in-time copy of an index on disk, so recovery does not have to rebuild the graph from
 * the whole log, and so a follower that has fallen too far behind can start again from it.
 *
 * <p>Layout, big-endian: {@code magic:int32 version:int32 crc32:int32 body}, where the body is the
 * sequence number of the last operation the snapshot includes, that operation's term, and then
 * the index. The CRC covers the body. Version 1 snapshots have no term and are read as term 0.
 *
 * <p>A snapshot is written to a temporary file, forced to the device and then renamed over the
 * old one. The rename is atomic, so a reader sees either the old complete snapshot or the new
 * complete one, never a partial file.
 */
final class Snapshot {

    private static final int MAGIC = 0x56444253; // "VDBS"
    private static final int VERSION = 2;
    private static final int HEADER_BYTES = 12;
    private static final int CRC_OFFSET = 8;

    final HnswIndex index;
    /** Sequence number of the last logged operation contained in {@link #index}. */
    final long sequence;
    /** Term of that operation. */
    final long term;

    private Snapshot(HnswIndex index, long sequence, long term) {
        this.index = index;
        this.sequence = sequence;
        this.term = term;
    }

    static void write(Path target, Path temporary, HnswIndex index, long sequence, long term) throws IOException {
        try (FileChannel channel = FileChannel.open(
                temporary,
                StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING,
                StandardOpenOption.WRITE)) {
            // The CRC is not known until the body is written, so it starts as zero and is filled in.
            channel.write(ByteBuffer.allocate(HEADER_BYTES).putInt(MAGIC).putInt(VERSION).putInt(0).flip());
            CheckedOutputStream checked = new CheckedOutputStream(Channels.newOutputStream(channel), new CRC32());
            DataOutputStream out = new DataOutputStream(new BufferedOutputStream(checked, 1 << 16));
            out.writeLong(sequence);
            out.writeLong(term);
            index.writeTo(out);
            out.flush();
            ByteBuffer crc = ByteBuffer.allocate(4).putInt((int) checked.getChecksum().getValue()).flip();
            channel.write(crc, CRC_OFFSET);
            channel.force(true);
        }
        Files.move(temporary, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        syncDirectory(target.getParent());
    }

    static Snapshot read(Path path) throws IOException {
        long size = Files.size(path);
        try (InputStream file = Files.newInputStream(path)) {
            int version = readHeader(path, new DataInputStream(file));
            int expectedCrc = new DataInputStream(file).readInt();

            CheckedInputStream checked = new CheckedInputStream(file, new CRC32());
            DataInputStream in = new DataInputStream(new BufferedInputStream(checked, 1 << 16));
            long sequence = in.readLong();
            long term = version == 1 ? 0 : in.readLong();
            HnswIndex index = HnswIndex.readFrom(in, size);
            if (in.read() != -1) {
                throw new CorruptSnapshotException(path, "unexpected data after the index", null);
            }
            if ((int) checked.getChecksum().getValue() != expectedCrc) {
                throw new CorruptSnapshotException(path, "checksum mismatch", null);
            }
            return new Snapshot(index, sequence, term);
        } catch (CorruptSnapshotException e) {
            throw e;
        } catch (EOFException e) {
            throw new CorruptSnapshotException(path, "file is cut short", e);
        } catch (IOException e) {
            throw new CorruptSnapshotException(path, e.getMessage(), e);
        }
    }

    /**
     * Checks a snapshot's checksum without building the index, and returns the position it is at.
     * Used on a downloaded snapshot before it replaces anything.
     */
    static DurableIndex.Position verify(Path path) throws IOException {
        try (InputStream file = Files.newInputStream(path)) {
            int version = readHeader(path, new DataInputStream(file));
            int expectedCrc = new DataInputStream(file).readInt();
            CheckedInputStream checked = new CheckedInputStream(new BufferedInputStream(file, 1 << 16), new CRC32());
            DataInputStream in = new DataInputStream(checked);
            long sequence = in.readLong();
            long term = version == 1 ? 0 : in.readLong();
            byte[] buffer = new byte[1 << 16];
            while (checked.read(buffer) != -1) {
                // Only the checksum is wanted.
            }
            if ((int) checked.getChecksum().getValue() != expectedCrc) {
                throw new CorruptSnapshotException(path, "checksum mismatch", null);
            }
            return new DurableIndex.Position(sequence, term);
        } catch (CorruptSnapshotException e) {
            throw e;
        } catch (EOFException e) {
            throw new CorruptSnapshotException(path, "file is cut short", e);
        }
    }

    private static int readHeader(Path path, DataInputStream in) throws IOException {
        if (in.readInt() != MAGIC) {
            throw new CorruptSnapshotException(path, "not a snapshot file", null);
        }
        int version = in.readInt();
        if (version != 1 && version != VERSION) {
            throw new CorruptSnapshotException(path, "unsupported version " + version, null);
        }
        return version;
    }

    /** Makes a rename durable. Windows cannot open a directory to fsync it; skipped there. */
    static void syncDirectory(Path directory) {
        try (FileChannel channel = FileChannel.open(directory, StandardOpenOption.READ)) {
            channel.force(true);
        } catch (IOException e) {
            // Not supported on this platform.
        }
    }
}
