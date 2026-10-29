package io.github.agampal007.vectordb.eval;

import io.github.agampal007.vectordb.SearchResult;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/** Recall@k: the share of the true k nearest neighbours that an approximate search returned. */
public final class Recall {

    private Recall() {}

    /**
     * Recall@k for one query. Compares ids only, so ties at the k-th distance can cost recall even
     * when the returned vector is just as close; the standard ANN benchmarks count it the same way.
     *
     * @param truth exact results, at least {@code k} of them unless the index is smaller than k
     */
    public static double atK(List<SearchResult> truth, List<SearchResult> approximate, int k) {
        if (k <= 0) {
            throw new IllegalArgumentException("k must be positive: " + k);
        }
        int expected = Math.min(k, truth.size());
        if (expected == 0) {
            return 1.0;
        }
        Set<Long> trueIds = new HashSet<>();
        for (int i = 0; i < expected; i++) {
            trueIds.add(truth.get(i).id());
        }
        int hits = 0;
        for (int i = 0; i < Math.min(k, approximate.size()); i++) {
            if (trueIds.contains(approximate.get(i).id())) {
                hits++;
            }
        }
        return (double) hits / expected;
    }

    /** Mean recall@k over a batch of queries; {@code truth.get(q)} pairs with {@code approximate.get(q)}. */
    public static double meanAtK(
            List<List<SearchResult>> truth, List<List<SearchResult>> approximate, int k) {
        if (truth.size() != approximate.size()) {
            throw new IllegalArgumentException(
                    "query count mismatch: " + truth.size() + " vs " + approximate.size());
        }
        if (truth.isEmpty()) {
            throw new IllegalArgumentException("no queries");
        }
        double sum = 0;
        for (int q = 0; q < truth.size(); q++) {
            sum += atK(truth.get(q), approximate.get(q), k);
        }
        return sum / truth.size();
    }
}
