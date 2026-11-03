package io.github.agampal007.vectordb;

import io.github.agampal007.vectordb.eval.Recall;
import java.util.ArrayList;
import java.util.List;

/**
 * Recall@10 and single-threaded queries per second for HNSW across {@code efSearch}, against
 * exact search, on uniform random vectors. Run with {@code ./gradlew sweep}.
 *
 * <p>A quick check, not the milestone 3 benchmark: random data, one dataset size, and timings from
 * {@link System#nanoTime()} after a short warm-up rather than JMH.
 */
public final class RandomDataSweep {

    private static final int VECTORS = 10_000;
    private static final int DIMENSION = 32;
    private static final int QUERIES = 200;
    private static final int K = 10;
    private static final int TIMED_ROUNDS = 20;

    private RandomDataSweep() {}

    public static void main(String[] args) {
        float[][] vectors = RandomVectors.generate(VECTORS, DIMENSION, 11L);
        float[][] queries = RandomVectors.generate(QUERIES, DIMENSION, 12L);

        FlatIndex exact = new FlatIndex(DIMENSION, Distance.L2);
        HnswIndex hnsw = new HnswIndex(DIMENSION, Distance.L2, 16, 200, 10, 13L);
        long start = System.nanoTime();
        for (int i = 0; i < VECTORS; i++) {
            hnsw.add(i, vectors[i]);
        }
        double buildSeconds = (System.nanoTime() - start) / 1e9;
        for (int i = 0; i < VECTORS; i++) {
            exact.add(i, vectors[i]);
        }

        List<List<SearchResult>> truth = new ArrayList<>();
        for (float[] q : queries) {
            truth.add(exact.search(q, K));
        }
        double exactQps = queriesPerSecond(exact, queries);

        System.out.printf("%,d vectors, %d dimensions, L2, M = 16, efConstruction = 200%n", VECTORS, DIMENSION);
        System.out.printf("HNSW build: %.2f s%n%n", buildSeconds);
        System.out.println("| Index | efSearch | Recall@10 | Queries/s |");
        System.out.println("| --- | --- | --- | --- |");
        System.out.printf("| Exact (FlatIndex) | - | 1.000 | %,.0f |%n", exactQps);
        for (int ef : new int[] {10, 20, 50, 100, 200}) {
            hnsw.setEfSearch(ef);
            List<List<SearchResult>> found = new ArrayList<>();
            for (float[] q : queries) {
                found.add(hnsw.search(q, K));
            }
            System.out.printf(
                    "| HNSW | %d | %.3f | %,.0f |%n",
                    ef, Recall.meanAtK(truth, found, K), queriesPerSecond(hnsw, queries));
        }
    }

    private static double queriesPerSecond(VectorIndex index, float[][] queries) {
        for (int round = 0; round < 10; round++) {
            for (float[] q : queries) {
                index.search(q, K);
            }
        }
        long start = System.nanoTime();
        for (int round = 0; round < TIMED_ROUNDS; round++) {
            for (float[] q : queries) {
                index.search(q, K);
            }
        }
        return (double) TIMED_ROUNDS * queries.length / ((System.nanoTime() - start) / 1e9);
    }
}
