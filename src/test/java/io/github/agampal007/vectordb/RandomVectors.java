package io.github.agampal007.vectordb;

import java.util.Random;

/** Seeded random vectors for tests, uniform on [-1, 1) in each dimension. */
final class RandomVectors {

    private RandomVectors() {}

    static float[][] generate(int count, int dimension, long seed) {
        Random random = new Random(seed);
        float[][] vectors = new float[count][dimension];
        for (float[] v : vectors) {
            for (int i = 0; i < dimension; i++) {
                v[i] = random.nextFloat() * 2f - 1f;
            }
        }
        return vectors;
    }
}
