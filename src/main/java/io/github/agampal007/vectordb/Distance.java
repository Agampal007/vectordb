package io.github.agampal007.vectordb;

/**
 * Distance functions between two vectors of equal dimension. Every metric returns a value where
 * smaller means closer, so indexes can treat them uniformly.
 */
public enum Distance {

    /** Squared Euclidean distance. The square root is skipped because it does not change ranking. */
    L2 {
        @Override
        float compute(float[] a, int aOffset, float[] b, int bOffset, int length) {
            float sum = 0f;
            for (int i = 0; i < length; i++) {
                float d = a[aOffset + i] - b[bOffset + i];
                sum += d * d;
            }
            return sum;
        }
    },

    /** {@code 1 - cos(a, b)}, in [0, 2]. A zero vector is treated as orthogonal to everything. */
    COSINE {
        @Override
        float compute(float[] a, int aOffset, float[] b, int bOffset, int length) {
            float dot = 0f;
            float normA = 0f;
            float normB = 0f;
            for (int i = 0; i < length; i++) {
                float x = a[aOffset + i];
                float y = b[bOffset + i];
                dot += x * y;
                normA += x * x;
                normB += y * y;
            }
            if (normA == 0f || normB == 0f) {
                return 1f;
            }
            return 1f - (float) (dot / (Math.sqrt(normA) * Math.sqrt(normB)));
        }
    },

    /** Negative inner product, for embeddings trained for maximum inner product search. */
    DOT {
        @Override
        float compute(float[] a, int aOffset, float[] b, int bOffset, int length) {
            float dot = 0f;
            for (int i = 0; i < length; i++) {
                dot += a[aOffset + i] * b[bOffset + i];
            }
            return -dot;
        }
    };

    /** Returns the distance between {@code a} and {@code b}. */
    public float between(float[] a, float[] b) {
        if (a.length != b.length) {
            throw new IllegalArgumentException(
                    "dimension mismatch: " + a.length + " vs " + b.length);
        }
        return compute(a, 0, b, 0, a.length);
    }

    /**
     * Distance between {@code a} and the {@code a.length} floats of {@code b} starting at
     * {@code bOffset}. Lets indexes that pack vectors into one array skip a copy per comparison.
     * No bounds checks beyond the JVM's: callers inside the package validate dimensions first.
     */
    float compute(float[] a, float[] b, int bOffset) {
        return compute(a, 0, b, bOffset, a.length);
    }

    /**
     * Distance between {@code length} floats of {@code a} from {@code aOffset} and of {@code b}
     * from {@code bOffset}. Used to compare two vectors stored in the same packed array.
     */
    abstract float compute(float[] a, int aOffset, float[] b, int bOffset, int length);
}
