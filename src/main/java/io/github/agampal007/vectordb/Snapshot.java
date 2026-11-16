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
 * the whole log.
 *
 * <p>Layout, big-endian: {@code magic:int32 version:int32 crc32:int32 body}, where the body is the
 * sequence number of the last operation the snapshot includes followed by the index, and the CRC
 * covers the body.
 *
 * <p>A snapshot is written to a temporary file, forced to the device and then renamed over the
 * old one. The rename is atomic, so a reader sees either the old complete snapshot or the new
 * complete one, never a partial file.
 */
final class Snapshot {

    private static final int MAGIC = 0x56444253; // "VDBS"
    private static final int VERSION = 1;
    private static final int HEADER_BYTES = 12;
    private static final int CRC_OFFSET = 8;

    final HnswIndex index;
    /** Sequence number of the last logged operation contained in {@link #index}. */
    final long sequence;

    private Snapshot(HnswIndex index, long sequence) {
        this.index = index;
        this.sequence = sequence;
    }

    static void write(Path target, Path temporary, HnswIndex index, long sequence) throws IOException {
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
            DataInputStream header = new DataInputStream(file);
            if (header.readInt() != MAGIC) {
                throw new CorruptSnapshotException(path, "not a snapshot file", null);
            }
            int version = header.readInt();
            if (version != VERSION) {
                throw new CorruptSnapshotException(path, "unsupported version " + version, null);
            }
            int expectedCrc = header.readInt();

            CheckedInputStream checked = new CheckedInputStream(file, new CRC32());
            DataInputStream in = new DataInputStream(new BufferedInputStream(checked, 1 << 16));
            long sequence = in.readLong();
            HnswIndex index = HnswIndex.readFrom(in, size);
            if (in.read() != -1) {
                throw new CorruptSnapshotException(path, "unexpected data after the index", null);
            }
            if ((int) checked.getChecksum().getValue() != expectedCrc) {
                throw new CorruptSnapshotException(path, "checksum mismatch", null);
            }
            return new Snapshot(index, sequence);
        } catch (CorruptSnapshotException e) {
            throw e;
        } catch (EOFException e) {
            throw new CorruptSnapshotException(path, "file is cut short", e);
        } catch (IOException e) {
            throw new CorruptSnapshotException(path, e.getMessage(), e);
        }
    }

    /** Makes the rename itself durable. Windows cannot open a directory to fsync it; skipped there. */
    private static void syncDirectory(Path directory) {
        try (FileChannel channel = FileChannel.open(directory, StandardOpenOption.READ)) {
            channel.force(true);
        } catch (IOException e) {
            // Not supported on this platform.
        }
    }
}
