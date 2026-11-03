package io.github.agampal007.vectordb;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.agampal007.vectordb.eval.Recall;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;

class HnswIndexTest {

    @Test
    void levelDistributionMatchesThePaper() {
        // P(level >= l) should be M^-l: with M = 16, about 1/16 of nodes reach layer 1
        // and about 1/256 reach layer 2.
        HnswIndex index = new HnswIndex(4, Distance.L2, 16, 200, 50, 7L);
        int draws = 200_000;
        int atLeast1 = 0;
        int atLeast2 = 0;
        for (int i = 0; i < draws; i++) {
            int level = index.randomLevel();
            if (level >= 1) atLeast1++;
            if (level >= 2) atLeast2++;
        }
        assertEquals(1.0 / 16, (double) atLeast1 / draws, 0.003);
        assertEquals(1.0 / 256, (double) atLeast2 / draws, 0.0006);
    }

    @Test
    void emptyIndexReturnsNothing() {
        assertEquals(List.of(), new HnswIndex(3, Distance.L2).search(new float[3], 5));
    }

    @Test
    void rejectsBadParameters() {
        assertThrows(IllegalArgumentException.class, () -> new HnswIndex(0, Distance.L2));
        assertThrows(IllegalArgumentException.class, () -> new HnswIndex(4, Distance.L2, 1, 200, 50, 0L));
        assertThrows(IllegalArgumentException.class, () -> new HnswIndex(4, Distance.L2, 16, 8, 50, 0L));
        assertThrows(IllegalArgumentException.class, () -> new HnswIndex(4, Distance.L2, 16, 200, 0, 0L));
        assertThrows(IllegalArgumentException.class, () -> new HnswIndex(4, Distance.L2).setEfSearch(0));
    }

    @Test
    void rejectsBadInput() {
        HnswIndex index = new HnswIndex(2, Distance.L2);
        index.add(1, new float[] {0, 0});

        assertThrows(IllegalArgumentException.class, () -> index.add(1, new float[] {1, 1}));
        assertThrows(IllegalArgumentException.class, () -> index.add(2, new float[] {1}));
        assertThrows(IllegalArgumentException.class, () -> index.search(new float[] {1}, 1));
        assertThrows(IllegalArgumentException.class, () -> index.search(new float[] {1, 1}, 0));
    }

    @Test
    void singleVectorIsFound() {
        HnswIndex index = new HnswIndex(2, Distance.L2);
        index.add(42, new float[] {3, 4});

        assertEquals(List.of(new SearchResult(42, 25f)), index.search(new float[] {0, 0}, 5));
    }

    @Test
    void smallIndexIsExact() {
        // With fewer vectors than ef, the beam covers the whole graph, so results must match
        // exact search, distances included.
        float[][] vectors = RandomVectors.generate(40, 4, 3L);
        HnswIndex approximate = new HnswIndex(4, Distance.L2);
        FlatIndex exact = new FlatIndex(4, Distance.L2);
        for (int i = 0; i < vectors.length; i++) {
            approximate.add(i, vectors[i]);
            exact.add(i, vectors[i]);
        }
        for (float[] q : RandomVectors.generate(20, 4, 4L)) {
            assertEquals(exact.search(q, 10), approximate.search(q, 10));
        }
    }

    @Test
    void keepsItsOwnCopyOfAddedVectors() {
        HnswIndex index = new HnswIndex(1, Distance.L2);
        float[] v = {0};
        index.add(1, v);
        v[0] = 100;

        assertEquals(0f, index.search(new float[] {0}, 1).get(0).distance());
    }

    @Test
    void graphRespectsDegreeCapsAndHasNoSelfOrDuplicateLinks() {
        int m = 8;
        HnswIndex index = new HnswIndex(8, Distance.L2, m, 64, 32, 5L);
        float[][] vectors = RandomVectors.generate(3_000, 8, 6L);
        for (int i = 0; i < vectors.length; i++) {
            index.add(i, vectors[i]);
        }
        for (int node = 0; node < index.size(); node++) {
            for (int layer = 0; layer <= index.levelOf(node); layer++) {
                int[] links = index.neighboursOf(node, layer);
                assertTrue(links.length <= (layer == 0 ? 2 * m : m), "degree cap at layer " + layer);
                Set<Integer> unique = new HashSet<>();
                for (int link : links) {
                    assertNotEquals(node, link, "self-link");
                    assertTrue(unique.add(link), "duplicate link");
                    assertTrue(index.levelOf(link) >= layer, "link to a node absent from the layer");
                }
            }
        }
    }

    /**
     * Many exact duplicates are a known weak spot of HNSW: once early copies fill their link
     * lists, ties keep the older links, so later copies get no inbound links and only part of the
     * group stays reachable (34 of 300 here). Search must still work and return true matches.
     */
    @Test
    void identicalVectorsStillReturnExactMatches() {
        HnswIndex index = new HnswIndex(3, Distance.L2);
        for (int i = 0; i < 300; i++) {
            index.add(i, new float[] {1, 1, 1});
        }
        List<SearchResult> results = index.search(new float[] {1, 1, 1}, 10);
        assertEquals(10, results.size());
        assertTrue(results.stream().allMatch(r -> r.distance() == 0f));
    }

    @Test
    void sameSeedGivesSameResults() {
        float[][] vectors = RandomVectors.generate(2_000, 16, 8L);
        float[] query = RandomVectors.generate(1, 16, 9L)[0];
        HnswIndex first = new HnswIndex(16, Distance.L2, 8, 50, 20, 10L);
        HnswIndex second = new HnswIndex(16, Distance.L2, 8, 50, 20, 10L);
        for (int i = 0; i < vectors.length; i++) {
            first.add(i, vectors[i]);
            second.add(i, vectors[i]);
        }
        assertEquals(first.search(query, 10), second.search(query, 10));
    }

    /**
     * The milestone 2 acceptance test: recall@10 against exact search on 10,000 random vectors.
     * Uniform random data is close to the hardest case for graph indexes, so real embeddings
     * usually do better.
     */
    @Test
    void recallAt10BeatsTargetOnRandomData() {
        double recall = recallAt10(Distance.L2, 100);
        assertTrue(recall >= 0.90, "recall@10 was " + recall);
    }

    @Test
    void recallHoldsForCosine() {
        double recall = recallAt10(Distance.COSINE, 100);
        assertTrue(recall >= 0.90, "recall@10 was " + recall);
    }

    @Test
    void widerBeamDoesNotLowerRecall() {
        assertTrue(recallAt10(Distance.L2, 200) >= recallAt10(Distance.L2, 10));
    }

    private static double recallAt10(Distance metric, int efSearch) {
        int dim = 32;
        float[][] vectors = RandomVectors.generate(10_000, dim, 11L);
        float[][] queries = RandomVectors.generate(200, dim, 12L);
        FlatIndex exact = new FlatIndex(dim, metric);
        HnswIndex approximate = new HnswIndex(dim, metric, 16, 200, efSearch, 13L);
        for (int i = 0; i < vectors.length; i++) {
            exact.add(i, vectors[i]);
            approximate.add(i, vectors[i]);
        }

        List<List<SearchResult>> truth = new ArrayList<>();
        List<List<SearchResult>> found = new ArrayList<>();
        for (float[] q : queries) {
            truth.add(exact.search(q, 10));
            found.add(approximate.search(q, 10));
        }
        return Recall.meanAtK(truth, found, 10);
    }
}
