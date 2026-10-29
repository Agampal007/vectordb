package io.github.agampal007.vectordb;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class DistanceTest {

    private static final float EPS = 1e-6f;

    @Test
    void l2IsSquaredEuclidean() {
        assertEquals(25f, Distance.L2.between(new float[] {0, 0}, new float[] {3, 4}), EPS);
        assertEquals(0f, Distance.L2.between(new float[] {1, 2, 3}, new float[] {1, 2, 3}), EPS);
    }

    @Test
    void cosineIgnoresMagnitude() {
        assertEquals(0f, Distance.COSINE.between(new float[] {1, 2}, new float[] {2, 4}), EPS);
        assertEquals(1f, Distance.COSINE.between(new float[] {1, 0}, new float[] {0, 5}), EPS);
        assertEquals(2f, Distance.COSINE.between(new float[] {1, 1}, new float[] {-3, -3}), EPS);
    }

    @Test
    void cosineTreatsZeroVectorAsOrthogonal() {
        assertEquals(1f, Distance.COSINE.between(new float[] {0, 0}, new float[] {1, 1}), EPS);
    }

    @Test
    void dotIsNegatedSoLargerProductRanksCloser() {
        float[] q = {1, 1};
        assertEquals(-7f, Distance.DOT.between(q, new float[] {3, 4}), EPS);
        assertTrue(Distance.DOT.between(q, new float[] {3, 4}) < Distance.DOT.between(q, new float[] {1, 1}));
    }

    @Test
    void offsetReadsTheRightRow() {
        float[] packed = {9, 9, 3, 4};
        assertEquals(25f, Distance.L2.compute(new float[] {0, 0}, packed, 2), EPS);
    }

    @Test
    void rejectsDimensionMismatch() {
        for (Distance d : Distance.values()) {
            assertThrows(IllegalArgumentException.class, () -> d.between(new float[2], new float[3]));
        }
    }
}
