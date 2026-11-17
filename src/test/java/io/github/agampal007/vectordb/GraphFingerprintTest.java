package io.github.agampal007.vectordb;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.Random;
import org.junit.jupiter.api.Test;

/**
 * Pins the exact graph that a fixed sequence of operations builds, as a SHA-256 of the index's
 * serialised form.
 *
 * <p>A write-ahead log stores operations, not the graph, so recovery is only correct if replaying
 * a log written by an older build produces the graph that build had in memory. Any change to how
 * an insert chooses or prunes links would silently break that. The hashes below were produced by
 * the insert as it was before it was split into prepare and commit; they must not change unless
 * the log format is versioned to match.
 *
 * <p>The cases cover each metric, a small {@code M} (so most back-links are pruned), removed ids
 * that are added again, and vectors on a coarse grid, where many distances tie and the order of
 * ties decides the links.
 */
class GraphFingerprintTest {

    @Test
    void euclidean() throws Exception {
        assertEquals(
                "776803d606d4b01bbc46286114ec2e9ac4091626fbb50d20a1e65020d3666c4d",
                fingerprint(Distance.L2, 16, 8, 40, 3000, 1L, false));
    }

    @Test
    void cosine() throws Exception {
        assertEquals(
                "c4fead5f72e39410695a3e9c01d17fb6ee8fd04a7ca19f29d737c6e382ddca23",
                fingerprint(Distance.COSINE, 24, 16, 100, 2000, 2L, false));
    }

    @Test
    void dotProductWithSmallM() throws Exception {
        assertEquals(
                "42c10a790e09545fb78b2266e7d126bf0706844648e86f561874d2d2a8825b2f",
                fingerprint(Distance.DOT, 8, 4, 20, 2500, 3L, false));
    }

    @Test
    void manyTiedDistances() throws Exception {
        assertEquals(
                "01c135b95b86d90c661570adb337b3dbf60c08ee70915dbfe1e5d5da48ca52c6",
                fingerprint(Distance.L2, 4, 6, 30, 1500, 4L, true));
    }

    private static String fingerprint(
            Distance distance, int dim, int m, int efConstruction, int count, long seed, boolean grid)
            throws Exception {
        Random random = new Random(seed);
        HnswIndex index = new HnswIndex(dim, distance, m, efConstruction, 50, seed);
        for (int id = 0; id < count; id++) {
            index.add(id, vector(random, dim, grid));
            if (id % 7 == 6) {
                index.remove(id - 3);
            }
            if (id % 21 == 20) {
                index.add(id - 3, vector(random, dim, grid)); // an id that was removed comes back
            }
        }
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        index.writeTo(new DataOutputStream(bytes));
        return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes.toByteArray()));
    }

    private static float[] vector(Random random, int dim, boolean grid) {
        float[] vector = new float[dim];
        for (int i = 0; i < dim; i++) {
            vector[i] = grid ? random.nextInt(3) : random.nextFloat() * 2f - 1f;
        }
        return vector;
    }
}
