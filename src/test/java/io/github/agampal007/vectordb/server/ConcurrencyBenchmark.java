package io.github.agampal007.vectordb.server;

import io.github.agampal007.vectordb.Distance;
import io.github.agampal007.vectordb.DurableIndex;
import io.github.agampal007.vectordb.HnswIndex;
import io.github.agampal007.vectordb.io.VecsFiles;
import io.github.agampal007.vectordb.server.VectorDbServer.Options;
import io.github.agampal007.vectordb.wal.WriteAheadLog.SyncPolicy;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpRequest.BodyPublishers;
import java.net.http.HttpResponse.BodyHandlers;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import java.util.stream.Stream;

/**
 * Search throughput as threads are added, with and without a writer running, and the same
 * searches through the HTTP API. Run {@code scripts/download_sift.sh}, then
 * {@code ./gradlew concurrency} (100,000 vectors) or {@code ./gradlew concurrency -Pn=20000}.
 *
 * <p>Every measurement starts from a copy of the same snapshot, so a run that writes does not
 * leave a larger index for the next one. One run each, timed with {@link System#nanoTime()}.
 */
public final class ConcurrencyBenchmark {

    private static final int EF_SEARCH = 100;
    private static final int K = 10;
    private static final long WARM_UP_NANOS = 1_000_000_000L;
    private static final long MEASURE_NANOS = 4_000_000_000L;
    private static final String COLLECTION = "sift";

    private ConcurrencyBenchmark() {}

    private record Run(double searchesPerSecond, double writesPerSecond, long[] latenciesNanos) {}

    public static void main(String[] args) throws Exception {
        Path sift = Path.of(args.length > 0 ? args[0] : "data/sift");
        int n = args.length > 1 ? Integer.parseInt(args[1]) : 100_000;
        int extra = 50_000; // vectors the writer adds; never the ones already indexed
        float[][] all = VecsFiles.readFvecs(sift.resolve("sift_base.fvecs"), n + extra);
        float[][] queries = VecsFiles.readFvecs(sift.resolve("sift_query.fvecs"), 1_000);
        int dim = all[0].length;
        CollectionStore.Settings settings = new CollectionStore.Settings(dim, Distance.L2, 16, 200, EF_SEARCH);
        Supplier<HnswIndex> empty = () -> new HnswIndex(dim, Distance.L2, 16, 200, EF_SEARCH, 42L);
        int cores = Runtime.getRuntime().availableProcessors();
        List<Integer> threadCounts = new ArrayList<>();
        for (int threads : new int[] {1, 2, 4, 8, 16, 24, 32}) {
            if (threads <= cores) {
                threadCounts.add(threads);
            }
        }

        Path work = Files.createTempDirectory("vectordb-concurrency");
        try {
            // Build once through the same code the server uses, so the data directory is one a
            // server can open, and snapshot it.
            Path template = work.resolve("template");
            try (CollectionStore store = CollectionStore.open(template)) {
                DurableIndex index = store.create(COLLECTION, settings).index();
                for (int i = 0; i < n; i++) {
                    index.add(i, all[i]);
                }
            } // closing snapshots

            System.out.printf(
                    "SIFT, first %,d vectors, %d dimensions, M = 16, efConstruction = 200, efSearch = %d, k = %d%n",
                    n, dim, EF_SEARCH, K);
            System.out.printf("%d logical processors, %.0f s per measurement after %.0f s of warm-up%n%n",
                    cores, MEASURE_NANOS / 1e9, WARM_UP_NANOS / 1e9);

            System.out.println("| Search threads | Searches/s, no writer | Per thread | Searches/s, one writer"
                    + " | Writes/s (fsync each) |");
            System.out.println("| --- | --- | --- | --- | --- |");
            int copy = 0;
            for (int threads : threadCounts) {
                Run readOnly;
                Run withWriter;
                Path directory = copyOf(template, work.resolve("run" + copy++));
                try (DurableIndex index =
                        DurableIndex.open(directory.resolve(COLLECTION), empty, SyncPolicy.MANUAL)) {
                    readOnly = measure(threads, searcher(index, queries), null);
                }
                directory = copyOf(template, work.resolve("run" + copy++));
                try (DurableIndex index =
                        DurableIndex.open(directory.resolve(COLLECTION), empty, SyncPolicy.MANUAL)) {
                    withWriter = measure(threads, searcher(index, queries), writer(index, all, n));
                }
                System.out.printf("| %d | %,.0f | %,.0f | %,.0f | %,.0f |%n",
                        threads,
                        readOnly.searchesPerSecond(),
                        readOnly.searchesPerSecond() / threads,
                        withWriter.searchesPerSecond(),
                        withWriter.writesPerSecond());
            }

            System.out.println();
            System.out.println("Through the HTTP API on localhost (JSON in and out, one connection per client).");
            System.out.println("GET /health does no work, so it shows what the HTTP layer alone costs:");
            System.out.println();
            System.out.println("| Request | Clients | Requests/s | Median latency | 99th percentile |");
            System.out.println("| --- | --- | --- | --- | --- |");
            Path directory = copyOf(template, work.resolve("run" + copy));
            try (VectorDbServer server = VectorDbServer.start(directory, "127.0.0.1", 0, Options.defaults());
                    HttpClient client = HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1).build()) {
                String base = "http://127.0.0.1:" + server.port();
                URI health = URI.create(base + "/health");
                URI search = URI.create(base + "/collections/" + COLLECTION + "/search");
                String[] bodies = new String[queries.length];
                for (int i = 0; i < queries.length; i++) {
                    Map<String, Object> body = new LinkedHashMap<>();
                    body.put("vector", queries[i]);
                    body.put("k", (long) K);
                    bodies[i] = Json.write(body);
                }
                for (int clients : new int[] {1, 8, 24}) {
                    if (clients > cores) {
                        continue;
                    }
                    printHttpRow("GET /health", clients, measure(clients, httpRequest(client, health, null), null));
                    printHttpRow("POST search", clients, measure(clients, httpRequest(client, search, bodies), null));
                }
            }
        } finally {
            try (Stream<Path> files = Files.walk(work)) {
                files.sorted(Comparator.reverseOrder()).forEach(path -> path.toFile().delete());
            }
        }
    }

    /** One operation of a worker; {@code step} counts up from the worker's own starting point. */
    private interface Operation {
        void run(int step) throws Exception;
    }

    private static Operation searcher(DurableIndex index, float[][] queries) {
        return step -> index.search(queries[step % queries.length], K, EF_SEARCH);
    }

    /** Adds vectors that are not in the index yet, forcing each one to disk as a one-vector request would. */
    private static Operation writer(DurableIndex index, float[][] all, int n) {
        return step -> {
            int next = n + step;
            if (next >= all.length) {
                throw new IllegalStateException("the writer ran out of vectors");
            }
            index.upsert(next, all[next]);
            index.sync();
        };
    }

    /** A GET when {@code bodies} is null, otherwise a POST of each body in turn. */
    private static Operation httpRequest(HttpClient client, URI uri, String[] bodies) {
        return step -> {
            HttpRequest.Builder request = HttpRequest.newBuilder(uri);
            if (bodies != null) {
                request.POST(BodyPublishers.ofString(bodies[step % bodies.length]));
            }
            int status = client.send(request.build(), BodyHandlers.ofString()).statusCode();
            if (status != 200) {
                throw new IllegalStateException(uri + " answered " + status);
            }
        };
    }

    private static void printHttpRow(String request, int clients, Run run) {
        long[] latencies = run.latenciesNanos();
        System.out.printf("| %s | %d | %,.0f | %.2f ms | %.2f ms |%n",
                request,
                clients,
                run.searchesPerSecond(),
                latencies[latencies.length / 2] / 1e6,
                latencies[(int) (latencies.length * 0.99)] / 1e6);
    }

    /**
     * Runs {@code search} on {@code threads} threads, and {@code write} on one more if given, for
     * the warm-up and then the measured period. Only operations that finish inside the measured
     * period are counted.
     */
    private static Run measure(int threads, Operation search, Operation write) throws Exception {
        long start = System.nanoTime();
        long measureFrom = start + WARM_UP_NANOS;
        long end = measureFrom + MEASURE_NANOS;
        AtomicBoolean failed = new AtomicBoolean();
        ExecutorService pool = Executors.newFixedThreadPool(threads + 1);
        try {
            List<Future<long[]>> searchers = new ArrayList<>();
            for (int t = 0; t < threads; t++) {
                searchers.add(pool.submit(worker(search, t * 97, measureFrom, end, failed)));
            }
            Future<long[]> writer = write == null ? null : pool.submit(worker(write, 0, measureFrom, end, failed));

            int total = 0;
            List<long[]> perThread = new ArrayList<>();
            for (Future<long[]> future : searchers) {
                long[] latencies = future.get();
                perThread.add(latencies);
                total += latencies.length;
            }
            long[] merged = new long[total];
            int offset = 0;
            for (long[] latencies : perThread) {
                System.arraycopy(latencies, 0, merged, offset, latencies.length);
                offset += latencies.length;
            }
            Arrays.sort(merged);
            double seconds = MEASURE_NANOS / 1e9;
            double writes = writer == null ? 0 : writer.get().length / seconds;
            return new Run(total / seconds, writes, merged);
        } finally {
            pool.shutdownNow();
        }
    }

    /** Repeats {@code operation} until {@code end} and returns the duration of each measured call. */
    private static Callable<long[]> worker(
            Operation operation, int firstStep, long measureFrom, long end, AtomicBoolean failed) {
        return () -> {
            long[] latencies = new long[1 << 16];
            int count = 0;
            int step = firstStep;
            try {
                while (!failed.get()) {
                    long before = System.nanoTime();
                    if (before >= end) {
                        break;
                    }
                    operation.run(step++);
                    long after = System.nanoTime();
                    if (before >= measureFrom && after <= end) {
                        if (count == latencies.length) {
                            latencies = Arrays.copyOf(latencies, count * 2);
                        }
                        latencies[count++] = after - before;
                    }
                }
            } catch (Exception | Error e) {
                failed.set(true);
                throw e;
            }
            return Arrays.copyOf(latencies, count);
        };
    }

    private static Path copyOf(Path template, Path target) throws IOException {
        List<Path> sources;
        try (Stream<Path> walk = Files.walk(template)) {
            sources = walk.toList();
        }
        for (Path source : sources) {
            Path destination = target.resolve(template.relativize(source).toString());
            if (Files.isDirectory(source)) {
                Files.createDirectories(destination);
            } else {
                Files.copy(source, destination);
            }
        }
        return target;
    }
}
