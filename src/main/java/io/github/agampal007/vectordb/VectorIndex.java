package io.github.agampal007.vectordb;

import java.util.List;

/** An in-memory index of fixed-dimension vectors, each identified by a caller-chosen id. */
public interface VectorIndex {

    int dimension();

    Distance distance();

    /** Number of vectors in the index. */
    int size();

    /**
     * Adds a vector. The index keeps its own copy, so the caller may reuse the array.
     *
     * @throws IllegalArgumentException if the id is already present or the dimension is wrong
     */
    void add(long id, float[] vector);

    /**
     * Returns up to {@code k} vectors closest to {@code query}, closest first. Returns fewer than
     * {@code k} only when the index holds fewer than {@code k} vectors.
     */
    List<SearchResult> search(float[] query, int k);
}
