package io.github.agampal007.vectordb.io;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

/**
 * Readers for the {@code .fvecs} and {@code .ivecs} formats used by the SIFT and GIST ANN
 * benchmarks (http://corpus-texmex.irisa.fr/). Each record is a little-endian int32 dimension
 * followed by that many little-endian float32 or int32 values.
 */
public final class VecsFiles {

    private VecsFiles() {}

    /** Reads up to {@code limit} float vectors; pass {@link Integer#MAX_VALUE} for all of them. */
    public static float[][] readFvecs(Path path, int limit) throws IOException {
        ByteBuffer buffer = map(path);
        int count = recordCount(path, buffer);
        float[][] out = new float[Math.min(limit, count)][];
        for (int i = 0; i < out.length; i++) {
            float[] v = new float[buffer.getInt()];
            buffer.asFloatBuffer().get(v);
            buffer.position(buffer.position() + 4 * v.length);
            out[i] = v;
        }
        return out;
    }

    /** Reads up to {@code limit} int vectors, such as ground-truth neighbour ids. */
    public static int[][] readIvecs(Path path, int limit) throws IOException {
        ByteBuffer buffer = map(path);
        int count = recordCount(path, buffer);
        int[][] out = new int[Math.min(limit, count)][];
        for (int i = 0; i < out.length; i++) {
            int[] v = new int[buffer.getInt()];
            buffer.asIntBuffer().get(v);
            buffer.position(buffer.position() + 4 * v.length);
            out[i] = v;
        }
        return out;
    }

    private static ByteBuffer map(Path path) throws IOException {
        try (FileChannel channel = FileChannel.open(path, StandardOpenOption.READ)) {
            return channel.map(FileChannel.MapMode.READ_ONLY, 0, channel.size()).order(ByteOrder.LITTLE_ENDIAN);
        }
    }

    /** Number of records, assuming every record has the dimension of the first. */
    private static int recordCount(Path path, ByteBuffer buffer) {
        if (buffer.remaining() < 4) {
            throw new IllegalArgumentException(path + ": empty file");
        }
        int dimension = buffer.getInt(0);
        long recordBytes = 4L + 4L * dimension;
        if (dimension <= 0 || buffer.remaining() % recordBytes != 0) {
            throw new IllegalArgumentException(
                    path + ": size " + buffer.remaining() + " is not a whole number of records of dimension "
                            + dimension);
        }
        return Math.toIntExact(buffer.remaining() / recordBytes);
    }
}
