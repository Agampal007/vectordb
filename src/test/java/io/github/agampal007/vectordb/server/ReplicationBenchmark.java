package io.github.agampal007.vectordb.server;

import io.github.agampal007.vectordb.io.VecsFiles;
import io.github.agampal007.vectordb.server.VectorDbServer.Options;
import io.github.agampal007.vectordb.server.VectorDbServer.SyncMode;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpRequest.BodyPublishers;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

/**
 * What replication costs, on SIFT vectors, with every server in this JVM on localhost. Run
 * {@code scripts/download_sift.sh}, then {@code ./gradlew replication} (100,000 vectors for the
 * catch-up part) or {@code ./gradlew replication -Pn=20000}.
 *
 * <p>Part 1: latency of single-vector writes as followers are added and as the leader is asked to
 * wait for more of them. Part 2: how long a new follower takes to copy a collection, by replaying
 * the leader's log and by downloading a snapshot. One run each, timed with
 * {@link System#nanoTime()}.
 */
public final class ReplicationBenchmark {

    private static final int WARM_UP = 1_000;
    private static final int MEASURED = 2_000;
    private static final int BATCH = 1_000;

    private ReplicationBenchmark() {}

    private record Setup(String label, int followers, int minReplicas) {}

    public static void main(String[] args) throws Exception {
        Path sift = Path.of(args.length > 0 ? args[0] : "data/sift");
        int n = args.length > 1 ? Integer.parseInt(args[1]) : 100_000;
        float[][] base = VecsFiles.readFvecs(sift.resolve("sift_base.fvecs"), Math.max(n, WARM_UP + MEASURED));
        Path work = Files.createTempDirectory("vectordb-replication");
        try (HttpClient client = HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1).build()) {
            System.out.printf("SIFT, %d dimensions, M = 16, efConstruction = 200; all servers in one JVM on localhost%n%n",
                    base[0].length);
            writeLatency(client, work, base);
            catchUp(client, work, base, n);
        } finally {
            try (Stream<Path> files = Files.walk(work)) {
                files.sorted(Comparator.reverseOrder()).forEach(path -> path.toFile().delete());
            }
        }
    }

    /** Single-vector upserts into an empty collection; the first {@link #WARM_UP} are not timed. */
    private static void writeLatency(HttpClient client, Path work, float[][] base) throws Exception {
        List<Setup> setups = List.of(
                new Setup("no followers", 0, 0),
                new Setup("1 follower, leader does not wait", 1, 0),
                new Setup("1 follower, leader waits for it", 1, 1),
                new Setup("2 followers, leader waits for 1", 2, 1),
                new Setup("2 followers, leader waits for both", 2, 2));
        System.out.printf("Part 1: %,d single-vector writes, one at a time, after %,d untimed; fsync per request%n%n",
                MEASURED, WARM_UP);
        System.out.println("| Setup | Writes/s | Median latency | 99th percentile |");
        System.out.println("| --- | --- | --- | --- |");
        int run = 0;
        for (Setup setup : setups) {
            Path dir = work.resolve("latency-" + run++);
            List<VectorDbServer> servers = new ArrayList<>();
            try {
                VectorDbServer leader = VectorDbServer.start(dir.resolve("leader"), "127.0.0.1", 0,
                        Options.defaults().withReplication(setup.minReplicas(), 10_000, null));
                servers.add(leader);
                String url = "http://127.0.0.1:" + leader.port();
                for (int f = 0; f < setup.followers(); f++) {
                    servers.add(VectorDbServer.start(dir.resolve("follower" + f), "127.0.0.1", 0,
                            Options.defaults().withReplication(0, 10_000, url)));
                }
                check(send(client, "PUT", url + "/collections/sift", "{\"dimension\":" + base[0].length + "}"), 201);
                long[] latencies = new long[MEASURED];
                long start = 0;
                for (int i = 0; i < WARM_UP + MEASURED; i++) {
                    if (i == WARM_UP) {
                        start = System.nanoTime();
                    }
                    long before = System.nanoTime();
                    check(send(client, "POST", url + "/collections/sift/vectors", body(base, i, i + 1)), 200);
                    if (i >= WARM_UP) {
                        latencies[i - WARM_UP] = System.nanoTime() - before;
                    }
                }
                double seconds = (System.nanoTime() - start) / 1e9;
                Arrays.sort(latencies);
                System.out.printf("| %s | %,.0f | %.2f ms | %.2f ms |%n",
                        setup.label(),
                        MEASURED / seconds,
                        latencies[MEASURED / 2] / 1e6,
                        latencies[(int) (MEASURED * 0.99)] / 1e6);
            } finally {
                for (VectorDbServer server : servers.reversed()) {
                    server.close();
                }
            }
        }
        System.out.println();
    }

    /** A follower joining a collection of {@code n} vectors: once from the log, once from a snapshot. */
    private static void catchUp(HttpClient client, Path work, float[][] base, int n) throws Exception {
        Path dir = work.resolve("catch-up");
        // A log limit far above the collection's size, so the leader keeps every operation in its log.
        Options keepsItsLog = new Options(SyncMode.REQUEST, 1L << 40, 32 << 20);
        VectorDbServer leader = VectorDbServer.start(dir.resolve("leader"), "127.0.0.1", 0, keepsItsLog);
        String url = "http://127.0.0.1:" + leader.port();
        List<VectorDbServer> followers = new ArrayList<>();
        try {
            check(send(client, "PUT", url + "/collections/sift", "{\"dimension\":" + base[0].length + "}"), 201);
            long start = System.nanoTime();
            for (int from = 0; from < n; from += BATCH) {
                check(send(client, "POST", url + "/collections/sift/vectors", body(base, from, Math.min(n, from + BATCH))), 200);
            }
            double load = (System.nanoTime() - start) / 1e9;
            System.out.printf("Part 2: a new follower copies a collection of %,d vectors%n%n", n);
            System.out.println("| Step | Time | Notes |");
            System.out.println("| --- | --- | --- |");
            System.out.printf("| Load the leader through the API | %.1f s | batches of %,d vectors |%n", load, BATCH);

            start = System.nanoTime();
            followers.add(VectorDbServer.start(dir.resolve("from-log"), "127.0.0.1", 0, keepsItsLog.withReplication(0, 10_000, url)));
            String fromLog = "http://127.0.0.1:" + followers.get(0).port();
            awaitSequence(client, fromLog, sequence(client, url));
            double logSeconds = (System.nanoTime() - start) / 1e9;
            System.out.printf("| New follower, copying the leader's log | %.1f s | replays %,d operations, building the graph again |%n",
                    logSeconds, sequence(client, url));

            check(send(client, "POST", url + "/collections/sift/snapshot", ""), 200); // the log is emptied
            start = System.nanoTime();
            followers.add(VectorDbServer.start(dir.resolve("from-snapshot"), "127.0.0.1", 0, keepsItsLog.withReplication(0, 10_000, url)));
            String fromSnapshot = "http://127.0.0.1:" + followers.get(1).port();
            awaitSequence(client, fromSnapshot, sequence(client, url));
            double snapshotSeconds = (System.nanoTime() - start) / 1e9;
            long snapshotBytes = snapshot(client, url).length;
            System.out.printf("| New follower, after the leader's log was emptied by a snapshot | %.2f s | downloads a %,d MB snapshot; %.0fx faster |%n",
                    snapshotSeconds, snapshotBytes >> 20, logSeconds / snapshotSeconds);

            byte[] expected = snapshot(client, url);
            boolean identical = Arrays.equals(expected, snapshot(client, fromLog))
                    && Arrays.equals(expected, snapshot(client, fromSnapshot));
            System.out.printf("%nBoth followers' snapshots are byte for byte the leader's: %b%n", identical);
        } finally {
            for (VectorDbServer follower : followers) {
                follower.close();
            }
            leader.close();
        }
    }

    private static String body(float[][] base, int from, int to) {
        List<Object> items = new ArrayList<>(to - from);
        for (int i = from; i < to; i++) {
            Map<String, Object> item = new LinkedHashMap<>();
            item.put("id", (long) i);
            item.put("vector", base[i]);
            items.add(item);
        }
        return Json.write(Map.of("vectors", items));
    }

    private static long sequence(HttpClient client, String url) throws Exception {
        HttpResponse<String> response = send(client, "GET", url + "/replication/status", null);
        if (response.statusCode() != 200) {
            return -1;
        }
        for (Object item : (List<?>) ((Map<?, ?>) Json.parse(response.body())).get("collections")) {
            return (Long) ((Map<?, ?>) item).get("sequence");
        }
        return -1;
    }

    private static void awaitSequence(HttpClient client, String url, long target) throws Exception {
        while (sequence(client, url) != target) {
            Thread.sleep(5);
        }
    }

    private static byte[] snapshot(HttpClient client, String url) throws Exception {
        HttpResponse<String> status = send(client, "GET", url + "/replication/status", null);
        String incarnation = null;
        for (Object item : (List<?>) ((Map<?, ?>) Json.parse(status.body())).get("collections")) {
            incarnation = (String) ((Map<?, ?>) item).get("incarnation");
        }
        HttpRequest request = HttpRequest.newBuilder(URI.create(
                        url + "/replication/collections/sift/snapshot?peer=true&incarnation=" + incarnation))
                .build();
        return client.send(request, BodyHandlers.ofByteArray()).body();
    }

    private static HttpResponse<String> send(HttpClient client, String method, String url, String body)
            throws IOException, InterruptedException {
        HttpRequest request = HttpRequest.newBuilder(URI.create(url))
                .method(method, body == null ? BodyPublishers.noBody() : BodyPublishers.ofString(body))
                .build();
        return client.send(request, BodyHandlers.ofString());
    }

    private static void check(HttpResponse<String> response, int expected) {
        if (response.statusCode() != expected) {
            throw new IllegalStateException("expected " + expected + ", got " + response.statusCode() + ": " + response.body());
        }
    }
}
