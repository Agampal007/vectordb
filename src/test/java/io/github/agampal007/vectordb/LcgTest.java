package io.github.agampal007.vectordb;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Random;
import org.junit.jupiter.api.Test;

class LcgTest {

    @Test
    void matchesJavaUtilRandomForTheSameSeed() {
        for (long seed : new long[] {0L, 42L, -7L, Long.MAX_VALUE}) {
            Random reference = new Random(seed);
            Lcg lcg = new Lcg(seed);
            for (int i = 0; i < 1_000; i++) {
                assertEquals(reference.nextDouble(), lcg.nextDouble(), "seed " + seed + ", draw " + i);
            }
        }
    }

    @Test
    void restoredStateContinuesTheSequence() {
        Lcg original = new Lcg(13L);
        for (int i = 0; i < 100; i++) {
            original.nextDouble();
        }
        Lcg restored = Lcg.fromState(original.state());
        for (int i = 0; i < 100; i++) {
            assertEquals(original.nextDouble(), restored.nextDouble());
        }
    }
}
