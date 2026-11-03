package io.github.agampal007.vectordb;

import io.github.agampal007.vectordb.eval.Recall;
import io.github.agampal007.vectordb.io.VecsFiles;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * HNSW against exact search on the SIFT-1M benchmark: build time, heap, and recall@10, queries per
 * second and latency across {@code efSearch}. Run {@code scripts/download_sift.sh}, then
 * {@code ./gradlew sift} (all 1,000,000 vectors) or {@code ./gradlew sift -Pn=100000} for a quick
 * subset.
 *
 * <p>Single-threaded. Timings use {@link System#nanoTime()} around each query after a warm-up, not
 * JMH. With the full base set, recall is measured against the dataset's published ground truth;
 * with a subset, against this repo's exact search.
 */
public final class SiftBenchmark {

    private static final int K = 10;
    private static final int FULL_SIZE = 1_000_000;
    private static final int[] EF_SEARCH = {10, 20, 40, 80, 160, 320, 640};

    private SiftBenchmark() {}

    public static void main(String[] args) throws IOException {
        Path dir = Path.of(args.length > 0 ? args[0] : "data/sift");
        int n = args.length > 1 ? Integer.parseInt(args[1]) : FULL_SIZE;
        int m = args.length > 2 ? Integer.parseInt(args[2]) : 16;
        int efConstruction = args.length > 3 ? Integer.parseInt(args[3]) : 200;
        int exactQueries = args.length > 4 ? Integer.parseInt(args[4]) : 200;

        float[][] base = VecsFiles.readFvecs(dir.resolve("sift_base.fvecs"), n);
        float[][] queries = VecsFiles.readFvecs(dir.resolve("sift_query.fvecs"), Integer.MAX_VALUE);
        int dim = base[0].length;
        n = base.length;

        FlatIndex exact = new FlatIndex(dim, Distance.L2);
        for (int i = 0; i < n; i++) {
            exact.add(i, base[i]);
        }

        // Ground truth: the published neighbours for the full set, exact search for a subset.
        List<List<SearchResult>> truth = new ArrayList<>();
        if (n == FULL_SIZE) {
            for (int[] ids : VecsFiles.readIvecs(dir.resolve("sift_groundtruth.ivecs"), Integer.MAX_VALUE)) {
                List<SearchResult> row = new ArrayList<>(K);
                for (int j = 0; j < K; j++) {
                    row.add(new SearchResult(ids[j], 0f));
                }
                truth.add(row);
            }
        } else {
            queries = Arrays.copyOf(queries, 1_000);
            for (float[] q : queries) {
                truth.add(exact.search(q, K));
            }
        }

        HnswIndex hnsw = new HnswIndex(dim, Distance.L2, m, efConstruction, 10, 42L);
        long start = System.nanoTime();
        for (int i = 0; i < n; i++) {
            hnsw.add(i, base[i]);
            if ((i + 1) % 100_000 == 0) {
                System.err.printf("built %,d / %,d (%.0f s)%n", i + 1, n, (System.nanoTime() - start) / 1e9);
            }
        }
        double buildSeconds = (System.nanoTime() - start) / 1e9;
        base = null;
        System.gc();
        Runtime rt = Runtime.getRuntime();
        double heapGb = (rt.totalMemory() - rt.freeMemory()) / 1e9;

        System.out.printf(
                "SIFT: %,d base vectors, %,d queries, %d dimensions, L2, k = %d%n", n, queries.length, dim, K);
        System.out.printf(
                "HNSW M = %d, efConstruction = %d: built in %.0f s; heap in use with both indexes: %.2f GB%n%n",
                m, efConstruction, buildSeconds, heapGb);
        System.out.println("| Index | efSearch | Recall@10 | Queries/s | p50 latency (ms) | p99 latency (ms) |");
        System.out.println("| --- | --- | --- | --- | --- | --- |");

        float[][] exactSet = Arrays.copyOf(queries, Math.min(exactQueries, queries.length));
        Timing exactTiming = time(exact, exactSet);
        double exactRecall = Recall.meanAtK(truth.subList(0, exactSet.length), exactTiming.results, K);
        printRow("Exact (FlatIndex)", "-", exactRecall, exactTiming);

        for (int ef : EF_SEARCH) {
            hnsw.setEfSearch(ef);
            time(hnsw, Arrays.copyOf(queries, Math.min(500, queries.length))); // warm-up
            Timing timing = time(hnsw, queries);
            printRow("HNSW", Integer.toString(ef), Recall.meanAtK(truth, timing.results, K), timing);
        }
        System.out.printf("%nExact search timed on the first %d queries.%n", exactSet.length);
    }

    private record Timing(List<List<SearchResult>> results, double qps, double p50Ms, double p99Ms) {}

    private static Timing time(VectorIndex index, float[][] queries) {
        List<List<SearchResult>> results = new ArrayList<>(queries.length);
        long[] nanos = new long[queries.length];
        long total = 0;
        for (int i = 0; i < queries.length; i++) {
            long t0 = System.nanoTime();
            results.add(index.search(queries[i], K));
            nanos[i] = System.nanoTime() - t0;
            total += nanos[i];
        }
        Arrays.sort(nanos);
        return new Timing(
                results,
                queries.length / (total / 1e9),
                nanos[(int) (0.50 * (nanos.length - 1))] / 1e6,
                nanos[(int) (0.99 * (nanos.length - 1))] / 1e6);
    }

    private static void printRow(String index, String ef, double recall, Timing t) {
        System.out.printf(
                "| %s | %s | %.3f | %,.0f | %.3f | %.3f |%n", index, ef, recall, t.qps, t.p50Ms, t.p99Ms);
    }
}
