package io.github.agampal007.vectordb;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.agampal007.vectordb.eval.Recall;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

class RemovalTest {

    static Stream<Supplier<VectorIndex>> indexes() {
        return Stream.of(() -> new FlatIndex(2, Distance.L2), () -> new HnswIndex(2, Distance.L2));
    }

    @ParameterizedTest
    @MethodSource("indexes")
    void removedVectorIsGoneAndCanBeAddedAgain(Supplier<VectorIndex> factory) {
        VectorIndex index = factory.get();
        index.add(1, new float[] {0, 0});
        index.add(2, new float[] {1, 1});
        index.add(3, new float[] {5, 5});

        assertTrue(index.remove(2));
        assertFalse(index.remove(2));
        assertFalse(index.remove(99));
        assertEquals(2, index.size());
        assertFalse(index.contains(2));
        assertNull(index.get(2));
        assertEquals(List.of(1L, 3L), index.search(new float[] {1, 1}, 5).stream().map(SearchResult::id).toList());

        index.add(2, new float[] {4, 4});
        assertEquals(3, index.size());
        assertArrayEquals(new float[] {4, 4}, index.get(2));
        assertEquals(2L, index.search(new float[] {4, 4}, 1).get(0).id());
    }

    @ParameterizedTest
    @MethodSource("indexes")
    void getReturnsACopy(Supplier<VectorIndex> factory) {
        VectorIndex index = factory.get();
        index.add(7, new float[] {3, 4});
        index.get(7)[0] = 100;
        assertArrayEquals(new float[] {3, 4}, index.get(7));
    }

    @ParameterizedTest
    @MethodSource("indexes")
    void removingEverythingLeavesNothingToFind(Supplier<VectorIndex> factory) {
        VectorIndex index = factory.get();
        for (int i = 0; i < 50; i++) {
            index.add(i, new float[] {i, -i});
        }
        for (int i = 0; i < 50; i++) {
            assertTrue(index.remove(i));
        }
        assertEquals(0, index.size());
        assertEquals(List.of(), index.search(new float[] {1, 1}, 10));
    }

    @Test
    void flatIndexStaysExactAfterRemovingFromTheMiddle() {
        float[][] vectors = RandomVectors.generate(200, 4, 21L);
        FlatIndex index = new FlatIndex(4, Distance.L2);
        FlatIndex expected = new FlatIndex(4, Distance.L2);
        for (int i = 0; i < vectors.length; i++) {
            index.add(i, vectors[i]);
        }
        for (int i = 0; i < vectors.length; i += 3) {
            index.remove(i);
        }
        for (int i = 0; i < vectors.length; i++) {
            if (i % 3 != 0) {
                expected.add(i, vectors[i]);
            }
        }
        for (float[] q : RandomVectors.generate(20, 4, 22L)) {
            assertEquals(expected.search(q, 10), index.search(q, 10));
        }
    }

    /**
     * Removed nodes still carry traffic: after removing a fifth of the vectors, search must never
     * return one of them and recall against exact search over the survivors must hold up.
     */
    @Test
    void hnswKeepsRecallAfterRemovingAFifthOfTheVectors() {
        int dim = 32;
        float[][] vectors = RandomVectors.generate(10_000, dim, 31L);
        HnswIndex hnsw = new HnswIndex(dim, Distance.L2, 16, 200, 100, 33L);
        FlatIndex exact = new FlatIndex(dim, Distance.L2);
        for (int i = 0; i < vectors.length; i++) {
            hnsw.add(i, vectors[i]);
            exact.add(i, vectors[i]);
        }
        for (int i = 0; i < vectors.length; i += 5) {
            hnsw.remove(i);
            exact.remove(i);
        }
        assertEquals(8_000, hnsw.size());

        List<List<SearchResult>> truth = new ArrayList<>();
        List<List<SearchResult>> found = new ArrayList<>();
        for (float[] q : RandomVectors.generate(200, dim, 32L)) {
            List<SearchResult> results = hnsw.search(q, 10);
            assertEquals(10, results.size());
            assertTrue(results.stream().noneMatch(r -> r.id() % 5 == 0), "returned a removed vector");
            truth.add(exact.search(q, 10));
            found.add(results);
        }
        double recall = Recall.meanAtK(truth, found, 10);
        assertTrue(recall >= 0.90, "recall@10 after removals was " + recall);
    }
}
