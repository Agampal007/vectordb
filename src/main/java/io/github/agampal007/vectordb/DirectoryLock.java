package io.github.agampal007.vectordb;

import java.io.Closeable;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

/**
 * Exclusive ownership of a directory, held through an operating-system lock on a {@code LOCK}
 * file inside it.
 *
 * <p>Two writers appending to the same log would interleave their records and destroy it, so
 * whoever opens the directory second must be refused. The operating system releases the lock when
 * the owning process exits, however it exits, so a crash never leaves a directory locked.
 *
 * <p>The lock is advisory: it stops other users of this class, not a program that ignores it.
 */
public final class DirectoryLock implements Closeable {

    static final String LOCK_FILE = "LOCK";

    private final FileChannel channel;
    private final FileLock lock;

    private DirectoryLock(FileChannel channel, FileLock lock) {
        this.channel = channel;
        this.lock = lock;
    }

    /**
     * Takes the lock, creating the directory if needed.
     *
     * @throws IndexLockedException if the directory is already locked
     */
    public static DirectoryLock acquire(Path directory) throws IOException {
        Files.createDirectories(directory);
        FileChannel channel =
                FileChannel.open(directory.resolve(LOCK_FILE), StandardOpenOption.CREATE, StandardOpenOption.WRITE);
        try {
            FileLock lock;
            try {
                lock = channel.tryLock();
            } catch (OverlappingFileLockException e) {
                lock = null; // held elsewhere in this JVM
            }
            if (lock == null) {
                throw new IndexLockedException(directory);
            }
            return new DirectoryLock(channel, lock);
        } catch (IOException | RuntimeException e) {
            channel.close();
            throw e;
        }
    }

    @Override
    public void close() throws IOException {
        try {
            if (lock.isValid()) {
                lock.release();
            }
        } finally {
            channel.close();
        }
    }
}
