package io.github.agampal007.vectordb;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

/** The two halves of an insert, used separately. */
class PrepareCommitTest {

    private static final int DIM = 12;
    private static final float[][] VECTORS = RandomVectors.generate(800, DIM, 61L);
    private static final float[][] QUERIES = RandomVectors.generate(40, DIM, 62L);

    private static HnswIndex index(int count) {
        HnswIndex index = new HnswIndex(DIM, Distance.L2, 6, 30, 30, 8L);
        for (int i = 0; i < count; i++) {
            index.add(i, VECTORS[i]);
        }
        return index;
    }

    private static List<List<SearchResult>> searchAll(HnswIndex index) {
        List<List<SearchResult>> results = new ArrayList<>();
        for (float[] query : QUERIES) {
            results.add(index.search(query, 10));
        }
        return results;
    }

    /** Searches run while an insert is being prepared, so preparing must not change what they see. */
    @Test
    void preparingChangesNothingASearchCanSee() {
        HnswIndex index = index(600);
        List<List<SearchResult>> before = searchAll(index);
        int[][] linksBefore = new int[index.nodeCount()][];
        for (int node = 0; node < linksBefore.length; node++) {
            linksBefore[node] = index.neighboursOf(node, 0);
        }

        HnswIndex.PendingAdd pending = index.prepareAdd(600, VECTORS[600]);

        assertEquals(600, index.size());
        assertEquals(600, index.nodeCount());
        assertFalse(index.contains(600));
        assertNull(index.get(600));
        assertEquals(before, searchAll(index));
        for (int node = 0; node < linksBefore.length; node++) {
            assertArrayEquals(linksBefore[node], index.neighboursOf(node, 0), "links of node " + node);
        }

        index.commit(pending);

        assertEquals(601, index.size());
        assertArrayEquals(VECTORS[600], index.get(600));
        assertEquals(new SearchResult(600, 0f), index.search(VECTORS[600], 1).get(0));
        // The new node is linked in both directions: some neighbour of it links back.
        int[] own = index.neighboursOf(600, 0);
        assertTrue(own.length > 0);
        boolean linkedBack = false;
        for (int neighbour : own) {
            for (int back : index.neighboursOf(neighbour, 0)) {
                linkedBack |= back == 600;
            }
        }
        assertTrue(linkedBack);
    }

    @Test
    void prepareThenCommitBuildsTheGraphThatAddBuilds() {
        HnswIndex oneStep = index(800);
        HnswIndex twoSteps = new HnswIndex(DIM, Distance.L2, 6, 30, 30, 8L);
        for (int i = 0; i < 800; i++) {
            twoSteps.commit(twoSteps.prepareAdd(i, VECTORS[i]));
        }
        TestGraphs.assertSameGraph(oneStep, twoSteps);
    }

    /** A plan describes link lists as they will be; applying it to a graph that has moved on would corrupt it. */
    @Test
    void aPlanIsRefusedOnceTheGraphHasChanged() {
        HnswIndex index = index(300);
        HnswIndex.PendingAdd stale = index.prepareAdd(300, VECTORS[300]);
        index.add(301, VECTORS[301]);
        List<List<SearchResult>> before = searchAll(index);

        assertThrows(IllegalStateException.class, () -> index.commit(stale));

        assertEquals(301, index.size());
        assertFalse(index.contains(300));
        assertEquals(before, searchAll(index));
    }

    @Test
    void committingAnIdThatIsPresentIsRefused() {
        HnswIndex index = index(100);
        HnswIndex.PendingAdd duplicate = index.prepareAdd(42, VECTORS[500]);
        assertThrows(IllegalArgumentException.class, () -> index.commit(duplicate));
        assertEquals(100, index.size());
        assertArrayEquals(VECTORS[42], index.get(42));
        assertThrows(IllegalArgumentException.class, () -> index.prepareAdd(1000, new float[DIM - 1]));
    }

    @Test
    void theFirstInsertNeedsNoNeighbours() {
        HnswIndex index = new HnswIndex(DIM, Distance.L2, 6, 30, 30, 8L);
        index.commit(index.prepareAdd(5, VECTORS[0]));
        assertEquals(List.of(new SearchResult(5, 0f)), index.search(VECTORS[0], 3));
    }
}
