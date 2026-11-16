package io.github.agampal007.vectordb;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.agampal007.vectordb.wal.WriteAheadLog.SyncPolicy;
import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * Kills a real writer process at arbitrary moments and checks what comes back.
 *
 * <p>Each round starts {@link CrashWriter} in its own JVM, lets it acknowledge a random number of
 * writes, waits a random fraction of a millisecond and kills it without warning ({@code SIGKILL} on
 * Linux, {@code TerminateProcess} on Windows). The delay spreads the kills over the writer's
 * phases: waiting on fsync, inserting into the graph, writing a snapshot, or between a snapshot
 * and emptying the log. The next round continues from whatever the previous one left on disk.
 *
 * <p>What this does not cover: a process kill cannot tear a record, because each record is one
 * small write that the kernel completes, and it does not test fsync, because the operating system
 * still flushes its cache afterwards. Torn and corrupt records are covered byte by byte in
 * {@code WriteAheadLogTest}; power loss is not tested.
 */
class KillRecoveryTest {

    private static final int ROUNDS = 16;
    private static final int SNAPSHOT_EVERY = 8;
    private static final int MAX_KILL_DELAY_MICROS = 1_500;

    @TempDir Path dir;

    @Test
    @Timeout(value = 180, unit = TimeUnit.SECONDS)
    void everyAcknowledgedWriteSurvivesKillingTheWriter() throws Exception {
        Random random = new Random(2026);
        long acknowledged = -1;
        for (int round = 0; round < ROUNDS; round++) {
            Process writer = startWriter();
            int killAfter = 10 + random.nextInt(40);
            long killDelayNanos = random.nextInt(MAX_KILL_DELAY_MICROS) * 1_000L;
            try (BufferedReader output = writer.inputReader()) {
                String ready = output.readLine();
                assertNotNull(ready, "the writer process did not start");
                long resumedAt = Long.parseLong(ready.substring("READY ".length()));
                assertTrue(resumedAt > acknowledged, "writer resumed at " + resumedAt + " after ack " + acknowledged);

                String line;
                for (int seen = 0; seen < killAfter && (line = output.readLine()) != null; seen++) {
                    acknowledged = ackedId(line, acknowledged);
                }
                long deadline = System.nanoTime() + killDelayNanos;
                while (System.nanoTime() < deadline) {
                    Thread.onSpinWait(); // sleep() is too coarse to aim inside a single write
                }
                writer.destroyForcibly();
                // Acknowledgements already in the pipe when the kill landed still count.
                try {
                    while ((line = output.readLine()) != null) {
                        acknowledged = ackedId(line, acknowledged);
                    }
                } catch (IOException e) {
                    // On Linux, destroying a process closes its streams, so whatever was still in
                    // the pipe cannot be read. Missing those only makes the check below weaker.
                }
            }
            assertTrue(writer.waitFor(30, TimeUnit.SECONDS));
            assertRecovered(acknowledged, round);
        }
        assertTrue(acknowledged >= ROUNDS * 10 - 1, "too few writes were acknowledged: " + acknowledged);
    }

    private static long ackedId(String line, long previous) {
        return line.matches("ACK \\d+") ? Math.max(previous, Long.parseLong(line.substring(4))) : previous;
    }

    private void assertRecovered(long acknowledged, int round) throws IOException {
        try (DurableIndex index = DurableIndex.open(dir, CrashWriter::emptyIndex, SyncPolicy.EVERY_WRITE)) {
            String context = "round " + round + ", " + index.recovery();
            System.out.println("kill " + context + ", last ack " + acknowledged + ", recovered " + index.size());
            // Nothing acknowledged may be missing. Writes that reached the log before the kill but
            // were not yet acknowledged, or not yet read from the pipe, may also be there.
            long size = index.size();
            assertTrue(size > acknowledged, context + ", size " + size);
            for (long id = 0; id < size; id++) {
                assertArrayEquals(CrashWriter.vectorFor(id), index.get(id), context + ", id " + id);
            }
            HnswIndex uninterrupted = CrashWriter.emptyIndex();
            for (long id = 0; id < size; id++) {
                uninterrupted.add(id, CrashWriter.vectorFor(id));
            }
            TestGraphs.assertSameGraph(uninterrupted, index.index());
            assertEquals(size, index.index().nodeCount());
        }
    }

    private Process startWriter() throws IOException {
        String classpath = System.getProperty("vectordb.test.classpath", System.getProperty("java.class.path"));
        ProcessBuilder builder = new ProcessBuilder(
                Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "-cp",
                classpath,
                CrashWriter.class.getName(),
                dir.toString(),
                Integer.toString(SNAPSHOT_EVERY));
        builder.redirectError(ProcessBuilder.Redirect.INHERIT);
        return builder.start();
    }
}
