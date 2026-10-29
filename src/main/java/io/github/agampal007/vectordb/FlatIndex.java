package io.github.agampal007.vectordb;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;

/**
 * Exact nearest-neighbour search by scanning every vector. It is the ground truth that approximate
 * indexes are measured against, and the baseline they have to beat on speed.
 *
 * <p>Vectors live in one contiguous {@code float[]} (row {@code i} starts at {@code i * dim}) rather
 * than one array per vector, which keeps the scan sequential in memory. Not thread-safe.
 */
public final class FlatIndex implements VectorIndex {

    private static final int INITIAL_CAPACITY = 16;

    private final int dimension;
    private final Distance distance;
    private final Map<Long, Integer> rowById = new HashMap<>();
    private float[] data;
    private long[] ids;
    private int size;

    public FlatIndex(int dimension, Distance distance) {
        if (dimension <= 0) {
            throw new IllegalArgumentException("dimension must be positive: " + dimension);
        }
        this.dimension = dimension;
        this.distance = distance;
        this.data = new float[INITIAL_CAPACITY * dimension];
        this.ids = new long[INITIAL_CAPACITY];
    }

    @Override
    public int dimension() {
        return dimension;
    }

    @Override
    public Distance distance() {
        return distance;
    }

    @Override
    public int size() {
        return size;
    }

    @Override
    public void add(long id, float[] vector) {
        checkDimension(vector);
        if (rowById.containsKey(id)) {
            throw new IllegalArgumentException("duplicate id: " + id);
        }
        if (size == ids.length) {
            int capacity = ids.length * 2;
            ids = Arrays.copyOf(ids, capacity);
            data = Arrays.copyOf(data, capacity * dimension);
        }
        System.arraycopy(vector, 0, data, size * dimension, dimension);
        ids[size] = id;
        rowById.put(id, size);
        size++;
    }

    @Override
    public List<SearchResult> search(float[] query, int k) {
        checkDimension(query);
        if (k <= 0) {
            throw new IllegalArgumentException("k must be positive: " + k);
        }
        // Max-heap of the best k so far: the root is the worst kept result, evicted first.
        PriorityQueue<SearchResult> best = new PriorityQueue<>(k, Collections.reverseOrder());
        for (int i = 0; i < size; i++) {
            SearchResult candidate =
                    new SearchResult(ids[i], distance.compute(query, data, i * dimension));
            if (best.size() < k) {
                best.add(candidate);
            } else if (candidate.compareTo(best.peek()) < 0) {
                best.poll();
                best.add(candidate);
            }
        }
        List<SearchResult> results = new ArrayList<>(best);
        Collections.sort(results);
        return results;
    }

    private void checkDimension(float[] vector) {
        if (vector.length != dimension) {
            throw new IllegalArgumentException(
                    "expected dimension " + dimension + ", got " + vector.length);
        }
    }
}
