package io.github.agampal007.vectordb.io;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class VecsFilesTest {

    @TempDir Path dir;

    private Path write(String name, int dimension, Number... values) throws IOException {
        int records = values.length / dimension;
        ByteBuffer buffer = ByteBuffer.allocate(records * (4 + 4 * dimension)).order(ByteOrder.LITTLE_ENDIAN);
        for (int r = 0; r < records; r++) {
            buffer.putInt(dimension);
            for (int i = 0; i < dimension; i++) {
                Number v = values[r * dimension + i];
                if (v instanceof Float f) {
                    buffer.putFloat(f);
                } else {
                    buffer.putInt(v.intValue());
                }
            }
        }
        Path path = dir.resolve(name);
        Files.write(path, buffer.array());
        return path;
    }

    @Test
    void readsLittleEndianFloatRecords() throws IOException {
        Path path = write("a.fvecs", 3, 1.5f, -2f, 0.25f, 4f, 5f, 6f);
        float[][] vectors = VecsFiles.readFvecs(path, Integer.MAX_VALUE);
        assertEquals(2, vectors.length);
        assertArrayEquals(new float[] {1.5f, -2f, 0.25f}, vectors[0]);
        assertArrayEquals(new float[] {4f, 5f, 6f}, vectors[1]);
    }

    @Test
    void limitStopsEarly() throws IOException {
        Path path = write("a.fvecs", 2, 1f, 2f, 3f, 4f, 5f, 6f);
        assertEquals(2, VecsFiles.readFvecs(path, 2).length);
    }

    @Test
    void readsIntRecords() throws IOException {
        Path path = write("gt.ivecs", 4, 7, 3, 9, 1, 2, 4, 6, 8);
        int[][] ids = VecsFiles.readIvecs(path, Integer.MAX_VALUE);
        assertArrayEquals(new int[] {7, 3, 9, 1}, ids[0]);
        assertArrayEquals(new int[] {2, 4, 6, 8}, ids[1]);
    }

    @Test
    void rejectsATruncatedFile() throws IOException {
        Path path = dir.resolve("bad.fvecs");
        Files.write(path, new byte[] {3, 0, 0, 0, 1, 2, 3});
        assertThrows(IllegalArgumentException.class, () -> VecsFiles.readFvecs(path, 10));
    }
}
