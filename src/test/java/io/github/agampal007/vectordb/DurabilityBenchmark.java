package io.github.agampal007.vectordb;

import io.github.agampal007.vectordb.io.VecsFiles;
import io.github.agampal007.vectordb.wal.WriteAheadLog;
import io.github.agampal007.vectordb.wal.WriteAheadLog.Op;
import io.github.agampal007.vectordb.wal.WriteAheadLog.SyncPolicy;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.function.Supplier;
import java.util.stream.Stream;

/**
 * What durability costs and what a snapshot saves, on the first {@code n} SIFT vectors. Run
 * {@code scripts/download_sift.sh}, then {@code ./gradlew durability} (100,000 vectors) or
 * {@code ./gradlew durability -Pn=20000}.
 *
 * <p>Single-threaded, one run, timed with {@link System#nanoTime()}. fsync speed depends heavily on
 * the storage device and on whether it honours flush requests.
 */
public final class DurabilityBenchmark {

    private static final int FSYNC_RECORDS = 2_000;

    private DurabilityBenchmark() {}

    public static void main(String[] args) throws IOException {
        Path sift = Path.of(args.length > 0 ? args[0] : "data/sift");
        int n = args.length > 1 ? Integer.parseInt(args[1]) : 100_000;
        float[][] base = VecsFiles.readFvecs(sift.resolve("sift_base.fvecs"), n);
        float[][] queries = VecsFiles.readFvecs(sift.resolve("sift_query.fvecs"), 100);
        int dim = base[0].length;
        Supplier<HnswIndex> empty = () -> new HnswIndex(dim, Distance.L2, 16, 200, 50, 42L);
        Path work = Files.createTempDirectory("vectordb-durability");
        try {
            Path directory = work.resolve("index");

            long start = System.nanoTime();
            HnswIndex inMemory = empty.get();
            for (int i = 0; i < base.length; i++) {
                inMemory.add(i, base[i]);
            }
            double memorySeconds = seconds(start);

            start = System.nanoTime();
            try (DurableIndex index = DurableIndex.open(directory, empty, SyncPolicy.MANUAL)) {
                for (int i = 0; i < base.length; i++) {
                    index.add(i, base[i]);
                }
                index.sync();
            }
            double loggedSeconds = seconds(start);
            long logBytes = Files.size(directory.resolve(DurableIndex.LOG_FILE));

            start = System.nanoTime();
            double replaySeconds;
            double snapshotSeconds;
            try (DurableIndex index = DurableIndex.open(directory, empty, SyncPolicy.MANUAL)) {
                replaySeconds = seconds(start);
                start = System.nanoTime();
                index.snapshot();
                snapshotSeconds = seconds(start);
            }
            long snapshotBytes = Files.size(directory.resolve(DurableIndex.SNAPSHOT_FILE));

            start = System.nanoTime();
            boolean identical = true;
            double loadSeconds;
            try (DurableIndex index = DurableIndex.open(directory, empty, SyncPolicy.MANUAL)) {
                loadSeconds = seconds(start);
                for (float[] query : queries) {
                    identical &= index.search(query, 10).equals(inMemory.search(query, 10));
                }
            }

            double syncedPerSecond = appendsPerSecond(work.resolve("synced.log"), SyncPolicy.EVERY_WRITE, base);
            double unsyncedPerSecond = appendsPerSecond(work.resolve("unsynced.log"), SyncPolicy.MANUAL, base);

            System.out.printf("SIFT, first %,d vectors, %d dimensions, M = 16, efConstruction = 200%n%n", n, dim);
            System.out.println("| Step | Time | Notes |");
            System.out.println("| --- | --- | --- |");
            System.out.printf("| Build in memory | %.1f s | no durability |%n", memorySeconds);
            System.out.printf(
                    "| Build with every write logged, one fsync at the end | %.1f s | log is %,d MB |%n",
                    loggedSeconds, logBytes >> 20);
            System.out.printf("| Recover by replaying the log | %.1f s | rebuilds the graph insert by insert |%n", replaySeconds);
            System.out.printf("| Write a snapshot | %.2f s | %,d MB, includes fsync |%n", snapshotSeconds, snapshotBytes >> 20);
            System.out.printf(
                    "| Recover from the snapshot | %.2f s | %.0fx faster than replay; results identical to the in-memory build: %b |%n",
                    loadSeconds, replaySeconds / loadSeconds, identical);
            System.out.printf(
                    "%nLog appends alone (%,d records of %d floats): %,.0f per second with an fsync per record, "
                            + "%,.0f per second without.%n",
                    FSYNC_RECORDS, dim, syncedPerSecond, unsyncedPerSecond);
        } finally {
            try (Stream<Path> files = Files.walk(work)) {
                files.sorted(Comparator.reverseOrder()).forEach(path -> path.toFile().delete());
            }
        }
    }

    private static double appendsPerSecond(Path path, SyncPolicy policy, float[][] vectors) throws IOException {
        try (WriteAheadLog log = WriteAheadLog.open(path, policy, 0, entry -> {})) {
            long start = System.nanoTime();
            for (int i = 0; i < FSYNC_RECORDS; i++) {
                log.append(Op.ADD, i, vectors[i % vectors.length]);
            }
            log.sync();
            return FSYNC_RECORDS / seconds(start);
        }
    }

    private static double seconds(long startNanos) {
        return (System.nanoTime() - startNanos) / 1e9;
    }
}
