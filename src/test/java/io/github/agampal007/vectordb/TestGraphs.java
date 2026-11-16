package io.github.agampal007.vectordb;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

/** Graph comparisons shared by the recovery tests. */
final class TestGraphs {

    private TestGraphs() {}

    /** Same nodes, levels, links and tombstones: the two indexes are the same graph. */
    static void assertSameGraph(HnswIndex expected, HnswIndex actual) {
        assertEquals(expected.nodeCount(), actual.nodeCount());
        assertEquals(expected.size(), actual.size());
        for (int node = 0; node < expected.nodeCount(); node++) {
            assertEquals(expected.levelOf(node), actual.levelOf(node), "level of node " + node);
            for (int layer = 0; layer <= expected.levelOf(node); layer++) {
                assertArrayEquals(
                        expected.neighboursOf(node, layer),
                        actual.neighboursOf(node, layer),
                        "links of node " + node + " on layer " + layer);
            }
        }
        for (float[] query : RandomVectors.generate(20, expected.dimension(), 78L)) {
            assertEquals(expected.search(query, 10), actual.search(query, 10));
        }
    }
}
