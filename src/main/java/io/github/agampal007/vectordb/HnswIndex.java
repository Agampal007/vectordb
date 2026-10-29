package io.github.agampal007.vectordb;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * Approximate nearest-neighbour index using a Hierarchical Navigable Small World graph.
 *
 * <p>Reference: Y. Malkov and D. Yashunin, "Efficient and robust approximate nearest neighbor
 * search using Hierarchical Navigable Small World graphs", IEEE TPAMI 2018 (arXiv:1603.09320).
 * Algorithm numbers in the comments below refer to that paper.
 *
 * <p>Each vector becomes a node with a random top layer drawn from an exponential distribution, so
 * layer {@code l} holds about {@code M^-l} of the nodes. Upper layers are sparse long-range
 * shortcuts; layer 0 holds every node. A search descends greedily from the top and does a wider
 * beam search ({@code ef} candidates) on the bottom layer.
 *
 * <p>Status: milestone 2 in progress. Parameters, storage and level assignment are done; graph
 * construction and search are not. Not thread-safe.
 */
public final class HnswIndex implements VectorIndex {

    /** One graph node. {@code neighbours[l]} holds node indices on layer {@code l}. */
    private static final class Node {
        final long id;
        final int[][] neighbours;
        final int[] neighbourCount;

        Node(long id, int level, int maxConnections, int maxConnectionsLayer0) {
            this.id = id;
            this.neighbours = new int[level + 1][];
            this.neighbourCount = new int[level + 1];
            for (int l = 0; l <= level; l++) {
                neighbours[l] = new int[l == 0 ? maxConnectionsLayer0 : maxConnections];
            }
        }

        int level() {
            return neighbours.length - 1;
        }
    }

    private static final int INITIAL_CAPACITY = 16;

    private final int dimension;
    private final Distance distance;
    /** {@code M}: neighbours kept per node on layers above 0. */
    private final int maxConnections;
    /** {@code Mmax0}: neighbours kept per node on layer 0. The paper recommends {@code 2 * M}. */
    private final int maxConnectionsLayer0;
    /** Beam width while inserting. Higher builds a better graph, more slowly. */
    private final int efConstruction;
    /** Beam width while querying. Trades recall for speed; adjustable after building. */
    private int efSearch;
    /** {@code mL = 1 / ln(M)}, the level normaliser from the paper. */
    private final double levelMultiplier;
    private final Random random;

    private final List<Node> nodes = new ArrayList<>();
    private final Map<Long, Integer> nodeById = new HashMap<>();
    /** Vectors packed row-major, as in {@link FlatIndex}; row {@code i} belongs to node {@code i}. */
    private float[] data;
    private int entryPoint = -1;

    public HnswIndex(int dimension, Distance distance) {
        this(dimension, distance, 16, 200, 50, 42L);
    }

    public HnswIndex(
            int dimension,
            Distance distance,
            int maxConnections,
            int efConstruction,
            int efSearch,
            long seed) {
        if (dimension <= 0) {
            throw new IllegalArgumentException("dimension must be positive: " + dimension);
        }
        if (maxConnections < 2) {
            throw new IllegalArgumentException("M must be at least 2: " + maxConnections);
        }
        if (efConstruction < maxConnections) {
            throw new IllegalArgumentException(
                    "efConstruction (" + efConstruction + ") must be at least M (" + maxConnections + ")");
        }
        if (efSearch <= 0) {
            throw new IllegalArgumentException("efSearch must be positive: " + efSearch);
        }
        this.dimension = dimension;
        this.distance = distance;
        this.maxConnections = maxConnections;
        this.maxConnectionsLayer0 = 2 * maxConnections;
        this.efConstruction = efConstruction;
        this.efSearch = efSearch;
        this.levelMultiplier = 1.0 / Math.log(maxConnections);
        this.random = new Random(seed);
        this.data = new float[INITIAL_CAPACITY * dimension];
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
        return nodes.size();
    }

    public int efSearch() {
        return efSearch;
    }

    public void setEfSearch(int efSearch) {
        if (efSearch <= 0) {
            throw new IllegalArgumentException("efSearch must be positive: " + efSearch);
        }
        this.efSearch = efSearch;
    }

    /**
     * Inserts a vector (Algorithm 1).
     *
     * <ol>
     *   <li>Draw the node's top layer with {@link #randomLevel()} and store its vector.
     *   <li>If the index is empty, make it the entry point and stop.
     *   <li>From the entry point, descend greedily ({@code ef = 1}) through every layer above the
     *       node's top layer, keeping the closest node found as the next start point.
     *   <li>On each layer from {@code min(top, entryLevel)} down to 0: run {@code searchLayer} with
     *       {@code efConstruction}, pick neighbours with the heuristic (Algorithm 4), link both
     *       ways, and prune any neighbour whose list now exceeds its cap.
     *   <li>If the node's top layer is above the current entry point's, it becomes the entry point.
     * </ol>
     */
    @Override
    public void add(long id, float[] vector) {
        checkDimension(vector);
        if (nodeById.containsKey(id)) {
            throw new IllegalArgumentException("duplicate id: " + id);
        }
        // TODO(milestone 2): implement the steps above. Store the vector with storeVector(),
        // create the Node, then register it in nodes and nodeById.
        throw new UnsupportedOperationException("HNSW insert is not implemented yet");
    }

    /**
     * Searches the graph (Algorithm 5): greedy descent with {@code ef = 1} from the entry point to
     * layer 1, then {@code searchLayer} on layer 0 with {@code max(efSearch, k)}, returning the
     * closest {@code k}.
     */
    @Override
    public List<SearchResult> search(float[] query, int k) {
        checkDimension(query);
        if (k <= 0) {
            throw new IllegalArgumentException("k must be positive: " + k);
        }
        if (entryPoint < 0) {
            return List.of();
        }
        // TODO(milestone 2): implement. searchLayer (Algorithm 2) is the shared building block:
        // a min-heap of candidates to expand, a max-heap of the best ef found, and a visited set.
        throw new UnsupportedOperationException("HNSW search is not implemented yet");
    }

    /**
     * Draws a top layer: {@code floor(-ln(U) * mL)} with {@code U} uniform on (0, 1]. With
     * {@code mL = 1 / ln(M)}, a node reaches layer {@code l} or higher with probability {@code M^-l}.
     */
    int randomLevel() {
        double uniform = 1.0 - random.nextDouble();
        return (int) Math.floor(-Math.log(uniform) * levelMultiplier);
    }

    /** Copies {@code vector} into row {@code row} of {@link #data}, growing the array if needed. */
    @SuppressWarnings("unused") // called from add() once milestone 2 lands
    private void storeVector(int row, float[] vector) {
        if ((row + 1) * dimension > data.length) {
            data = Arrays.copyOf(data, Math.max(data.length * 2, (row + 1) * dimension));
        }
        System.arraycopy(vector, 0, data, row * dimension, dimension);
    }

    /** Distance from {@code query} to the vector stored for node {@code node}. */
    @SuppressWarnings("unused") // used by searchLayer once milestone 2 lands
    private float distanceTo(float[] query, int node) {
        return distance.compute(query, data, node * dimension);
    }

    private void checkDimension(float[] vector) {
        if (vector.length != dimension) {
            throw new IllegalArgumentException(
                    "expected dimension " + dimension + ", got " + vector.length);
        }
    }
}
