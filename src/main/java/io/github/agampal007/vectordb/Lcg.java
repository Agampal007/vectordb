package io.github.agampal007.vectordb;

/**
 * The 48-bit linear congruential generator specified for {@link java.util.Random}, with its state
 * exposed.
 *
 * <p>{@code java.util.Random} cannot report its state, so an index that used it could not be
 * snapshotted and then continue exactly as if it had never stopped. This class produces the same
 * sequence for the same seed, so graphs built before it existed are unchanged, and
 * {@link #state()} / {@link #fromState(long)} let a snapshot save and restore the generator.
 */
final class Lcg {

    private static final long MULTIPLIER = 0x5DEECE66DL;
    private static final long INCREMENT = 0xBL;
    private static final long MASK = (1L << 48) - 1;

    private long state;

    Lcg(long seed) {
        this.state = (seed ^ MULTIPLIER) & MASK;
    }

    static Lcg fromState(long state) {
        Lcg lcg = new Lcg(0L);
        lcg.state = state & MASK;
        return lcg;
    }

    long state() {
        return state;
    }

    /** Uniform on [0, 1), with 53 random bits, as {@link java.util.Random#nextDouble()}. */
    double nextDouble() {
        return (((long) next(26) << 27) + next(27)) * 0x1.0p-53;
    }

    private int next(int bits) {
        state = (state * MULTIPLIER + INCREMENT) & MASK;
        return (int) (state >>> (48 - bits));
    }
}
