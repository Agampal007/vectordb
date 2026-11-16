package io.github.agampal007.vectordb;

import java.util.List;

/** An index of fixed-dimension vectors, each identified by a caller-chosen id. */
public interface VectorIndex {

    int dimension();

    Distance distance();

    /** Number of vectors in the index, not counting removed ones. */
    int size();

    /**
     * Adds a vector. The index keeps its own copy, so the caller may reuse the array.
     *
     * @throws IllegalArgumentException if the id is already present or the dimension is wrong
     */
    void add(long id, float[] vector);

    /** Whether a vector with this id is in the index. */
    boolean contains(long id);

    /** A copy of the vector stored under {@code id}, or {@code null} if there is none. */
    float[] get(long id);

    /**
     * Removes the vector stored under {@code id}. The id can be added again afterwards.
     *
     * @return {@code false} if there was no such vector
     */
    boolean remove(long id);

    /**
     * Returns up to {@code k} vectors closest to {@code query}, closest first. Returns fewer than
     * {@code k} only when the index holds fewer than {@code k} vectors.
     */
    List<SearchResult> search(float[] query, int k);
}
