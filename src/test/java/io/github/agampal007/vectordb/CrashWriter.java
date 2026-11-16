package io.github.agampal007.vectordb;

import io.github.agampal007.vectordb.wal.WriteAheadLog.SyncPolicy;
import java.nio.file.Path;
import java.util.Random;

/**
 * The process that {@link KillRecoveryTest} kills. It adds vectors to a durable index for ever,
 * printing {@code ACK <id>} once each add has returned, and snapshots every few adds so that some
 * kills land in the middle of a snapshot.
 */
public final class CrashWriter {

    static final int DIMENSION = 16;

    private CrashWriter() {}

    static HnswIndex emptyIndex() {
        return new HnswIndex(DIMENSION, Distance.L2, 8, 40, 20, 9L);
    }

    /** The vector stored under {@code id}; the test regenerates it to check what was recovered. */
    static float[] vectorFor(long id) {
        Random random = new Random(id);
        float[] vector = new float[DIMENSION];
        for (int i = 0; i < DIMENSION; i++) {
            vector[i] = random.nextFloat() * 2f - 1f;
        }
        return vector;
    }

    public static void main(String[] args) throws Exception {
        Path directory = Path.of(args[0]);
        int snapshotEvery = Integer.parseInt(args[1]);
        try (DurableIndex index = DurableIndex.open(directory, CrashWriter::emptyIndex, SyncPolicy.EVERY_WRITE)) {
            long next = index.size();
            System.out.println("READY " + next);
            System.out.flush();
            while (true) {
                index.add(next, vectorFor(next));
                System.out.println("ACK " + next);
                System.out.flush();
                next++;
                if (next % snapshotEvery == 0) {
                    index.snapshot();
                }
            }
        }
    }
}
