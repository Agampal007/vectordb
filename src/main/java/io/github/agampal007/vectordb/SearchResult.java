package io.github.agampal007.vectordb;

/**
 * One hit from a nearest-neighbour search. Results order by distance, then by id, so that ties
 * are broken the same way on every run.
 */
public record SearchResult(long id, float distance) implements Comparable<SearchResult> {

    @Override
    public int compareTo(SearchResult other) {
        int byDistance = Float.compare(distance, other.distance);
        return byDistance != 0 ? byDistance : Long.compare(id, other.id);
    }
}
