package io.github.agampal007.vectordb;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.Test;

class FlatIndexTest {

    @Test
    void returnsNearestFirst() {
        FlatIndex index = new FlatIndex(2, Distance.L2);
        index.add(1, new float[] {0, 0});
        index.add(2, new float[] {10, 10});
        index.add(3, new float[] {1, 1});
        index.add(4, new float[] {5, 5});

        List<SearchResult> results = index.search(new float[] {0.9f, 0.9f}, 3);

        assertEquals(List.of(3L, 1L, 4L), results.stream().map(SearchResult::id).toList());
    }

    @Test
    void returnsEverythingWhenKExceedsSize() {
        FlatIndex index = new FlatIndex(1, Distance.L2);
        index.add(7, new float[] {1});
        index.add(8, new float[] {2});

        assertEquals(2, index.search(new float[] {0}, 10).size());
    }

    @Test
    void emptyIndexReturnsNothing() {
        assertEquals(List.of(), new FlatIndex(3, Distance.COSINE).search(new float[3], 5));
    }

    @Test
    void breaksDistanceTiesById() {
        FlatIndex index = new FlatIndex(1, Distance.L2);
        index.add(5, new float[] {1});
        index.add(3, new float[] {-1});

        assertEquals(3L, index.search(new float[] {0}, 1).get(0).id());
    }

    @Test
    void keepsItsOwnCopyOfAddedVectors() {
        FlatIndex index = new FlatIndex(1, Distance.L2);
        float[] v = {0};
        index.add(1, v);
        v[0] = 100;

        assertEquals(0f, index.search(new float[] {0}, 1).get(0).distance());
    }

    @Test
    void matchesSortingEveryDistanceAcrossGrowth() {
        int dim = 8;
        float[][] vectors = RandomVectors.generate(1_000, dim, 1L);
        FlatIndex index = new FlatIndex(dim, Distance.COSINE);
        for (int i = 0; i < vectors.length; i++) {
            index.add(i, vectors[i]);
        }
        float[] query = RandomVectors.generate(1, dim, 2L)[0];

        List<SearchResult> expected = new ArrayList<>();
        for (int i = 0; i < vectors.length; i++) {
            expected.add(new SearchResult(i, Distance.COSINE.between(query, vectors[i])));
        }
        Collections.sort(expected);

        assertEquals(expected.subList(0, 10), index.search(query, 10));
    }

    @Test
    void rejectsBadInput() {
        FlatIndex index = new FlatIndex(2, Distance.L2);
        index.add(1, new float[] {0, 0});

        assertThrows(IllegalArgumentException.class, () -> index.add(1, new float[] {1, 1}));
        assertThrows(IllegalArgumentException.class, () -> index.add(2, new float[] {1}));
        assertThrows(IllegalArgumentException.class, () -> index.search(new float[] {1}, 1));
        assertThrows(IllegalArgumentException.class, () -> index.search(new float[] {1, 1}, 0));
        assertThrows(IllegalArgumentException.class, () -> new FlatIndex(0, Distance.L2));
    }
}
