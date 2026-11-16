package io.github.agampal007.vectordb;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.agampal007.vectordb.wal.WriteAheadLog.SyncPolicy;
import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

class DirectoryLockTest {

    @TempDir Path dir;

    private DurableIndex open() throws IOException {
        return DurableIndex.open(dir, CrashWriter::emptyIndex, SyncPolicy.MANUAL);
    }

    @Test
    void aSecondOpenInTheSameProcessIsRefusedUntilTheFirstCloses() throws IOException {
        try (DurableIndex first = open()) {
            first.add(1, CrashWriter.vectorFor(1));
            assertThrows(IndexLockedException.class, this::open);
            // The refused open must not have touched the directory.
            first.add(2, CrashWriter.vectorFor(2));
            first.snapshot();
        }
        try (DurableIndex second = open()) {
            assertEquals(2, second.size());
        }
    }

    @Test
    void anOpenThatFailsGivesTheLockBack() throws IOException {
        Files.write(dir.resolve(DurableIndex.SNAPSHOT_FILE), new byte[] {1, 2, 3});
        assertThrows(CorruptSnapshotException.class, this::open);
        Files.delete(dir.resolve(DurableIndex.SNAPSHOT_FILE));
        try (DurableIndex index = open()) {
            assertEquals(0, index.size());
        }
    }

    @Test
    void operationsAfterCloseFail() throws IOException {
        DurableIndex index = open();
        index.close();
        index.close(); // closing twice is harmless
        assertThrows(IllegalStateException.class, () -> index.add(1, CrashWriter.vectorFor(1)));
        assertThrows(IllegalStateException.class, () -> index.search(CrashWriter.vectorFor(1), 1));
    }

    /**
     * The lock must hold across processes, and must not outlive its holder: a process that is
     * killed cannot release anything itself, so the operating system has to.
     */
    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void anotherProcessIsRefusedWhileTheHolderLivesAndAdmittedOnceItIsKilled() throws Exception {
        Process holder = CrashWriter.start(dir, 1_000_000);
        try (BufferedReader output = holder.inputReader()) {
            assertNotNull(output.readLine(), "the holder process did not start"); // READY: it has the lock
            assertThrows(IndexLockedException.class, this::open);
            assertTrue(holder.isAlive());
            holder.destroyForcibly();
        }
        assertTrue(holder.waitFor(30, TimeUnit.SECONDS));
        try (DurableIndex index = open()) {
            assertTrue(index.size() >= 0);
        }
    }
}
