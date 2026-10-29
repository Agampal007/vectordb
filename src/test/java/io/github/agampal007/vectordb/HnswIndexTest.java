package io.github.agampal007.vectordb;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.agampal007.vectordb.eval.Recall;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Disabled;
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

    /**
     * The milestone 2 acceptance test: recall@10 against exact search on 10,000 random vectors.
     * Uniform random data is close to the hardest case for graph indexes, so 0.90 here usually
     * means 0.95+ on real embeddings. Remove {@code @Disabled} once insert and search are written.
     */
    @Test
    @Disabled("milestone 2: HNSW insert and search not implemented yet")
    void recallAt10BeatsTargetOnRandomData() {
        int dim = 32;
        float[][] vectors = RandomVectors.generate(10_000, dim, 11L);
        float[][] queries = RandomVectors.generate(200, dim, 12L);
        FlatIndex exact = new FlatIndex(dim, Distance.L2);
        HnswIndex approximate = new HnswIndex(dim, Distance.L2, 16, 200, 100, 13L);
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

        double recall = Recall.meanAtK(truth, found, 10);
        assertTrue(recall >= 0.90, "recall@10 was " + recall);
    }
}
