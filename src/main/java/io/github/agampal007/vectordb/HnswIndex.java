package io.github.agampal007.vectordb;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
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
 * <p>Neighbours are chosen with the paper's heuristic (Algorithm 4) without its optional
 * candidate extension or kept-pruned connections, which is also what hnswlib does. Not
 * thread-safe.
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

    /** A node paired with its distance to whatever is being searched for. */
    private record Candidate(int node, float distance) {}

    /** Closest first; ties broken by node index so builds and searches are reproducible. */
    private static final Comparator<Candidate> CLOSEST_FIRST = (a, b) -> {
        int byDistance = Float.compare(a.distance(), b.distance());
        return byDistance != 0 ? byDistance : Integer.compare(a.node(), b.node());
    };

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

    /**
     * {@code visited[n] == visitEpoch} marks node {@code n} as seen in the current layer search.
     * Bumping the epoch clears every mark at once instead of allocating a set per search.
     */
    private int[] visited = new int[INITIAL_CAPACITY];
    private int visitEpoch;

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

    /** Inserts a vector (Algorithm 1). */
    @Override
    public void add(long id, float[] vector) {
        checkDimension(vector);
        if (nodeById.containsKey(id)) {
            throw new IllegalArgumentException("duplicate id: " + id);
        }
        int node = nodes.size();
        int level = randomLevel();
        storeVector(node, vector);
        nodes.add(new Node(id, level, maxConnections, maxConnectionsLayer0));
        nodeById.put(id, node);
        if (entryPoint < 0) {
            entryPoint = node;
            return;
        }

        // Above the new node's top layer, only the single closest node is carried down.
        int entryLevel = nodes.get(entryPoint).level();
        Candidate nearest = new Candidate(entryPoint, distanceTo(vector, entryPoint));
        for (int layer = entryLevel; layer > level; layer--) {
            nearest = greedyClosest(vector, nearest, layer);
        }

        // From there down, search wide, link to a diverse subset, and seed the next layer with
        // everything found.
        List<Candidate> entryPoints = List.of(nearest);
        Node inserted = nodes.get(node);
        for (int layer = Math.min(level, entryLevel); layer >= 0; layer--) {
            List<Candidate> found = searchLayer(vector, entryPoints, efConstruction, layer);
            for (Candidate neighbour : selectNeighbours(found, maxConnections)) {
                inserted.neighbours[layer][inserted.neighbourCount[layer]++] = neighbour.node();
                link(neighbour.node(), node, neighbour.distance(), layer);
            }
            entryPoints = found;
        }
        if (level > entryLevel) {
            entryPoint = node;
        }
    }

    /**
     * Searches the graph (Algorithm 5): greedy descent from the entry point to layer 1, then a
     * beam search of width {@code max(efSearch, k)} on layer 0.
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
        Candidate nearest = new Candidate(entryPoint, distanceTo(query, entryPoint));
        for (int layer = nodes.get(entryPoint).level(); layer > 0; layer--) {
            nearest = greedyClosest(query, nearest, layer);
        }
        List<Candidate> found = searchLayer(query, List.of(nearest), Math.max(efSearch, k), 0);

        int count = Math.min(k, found.size());
        List<SearchResult> results = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            Candidate c = found.get(i);
            results.add(new SearchResult(nodes.get(c.node()).id, c.distance()));
        }
        Collections.sort(results);
        return results;
    }

    /**
     * Beam search on one layer (Algorithm 2). Expands the closest unexpanded candidate until the
     * closest one left is further than the worst of the {@code ef} best found so far.
     *
     * @return up to {@code ef} nodes, closest first
     */
    private List<Candidate> searchLayer(
            float[] query, List<Candidate> entryPoints, int ef, int layer) {
        int epoch = nextVisitEpoch();
        PriorityQueue<Candidate> toExpand = new PriorityQueue<>(CLOSEST_FIRST);
        PriorityQueue<Candidate> best = new PriorityQueue<>(CLOSEST_FIRST.reversed());
        for (Candidate entry : entryPoints) {
            visited[entry.node()] = epoch;
            toExpand.add(entry);
            best.add(entry);
            if (best.size() > ef) {
                best.poll();
            }
        }

        while (!toExpand.isEmpty()) {
            Candidate current = toExpand.poll();
            if (current.distance() > best.peek().distance()) {
                break;
            }
            Node node = nodes.get(current.node());
            int[] links = node.neighbours[layer];
            for (int i = 0; i < node.neighbourCount[layer]; i++) {
                int next = links[i];
                if (visited[next] == epoch) {
                    continue;
                }
                visited[next] = epoch;
                float d = distanceTo(query, next);
                if (best.size() < ef || d < best.peek().distance()) {
                    Candidate candidate = new Candidate(next, d);
                    toExpand.add(candidate);
                    best.add(candidate);
                    if (best.size() > ef) {
                        best.poll();
                    }
                }
            }
        }

        List<Candidate> result = new ArrayList<>(best);
        result.sort(CLOSEST_FIRST);
        return result;
    }

    /** Hill-climbs to the closest node reachable on {@code layer}: a layer search with ef = 1. */
    private Candidate greedyClosest(float[] query, Candidate start, int layer) {
        Candidate best = start;
        boolean improved = true;
        while (improved) {
            improved = false;
            Node node = nodes.get(best.node());
            for (int i = 0; i < node.neighbourCount[layer]; i++) {
                int next = node.neighbours[layer][i];
                float d = distanceTo(query, next);
                if (d < best.distance()) {
                    best = new Candidate(next, d);
                    improved = true;
                }
            }
        }
        return best;
    }

    /**
     * Neighbour selection heuristic (Algorithm 4). Walks candidates closest first and keeps one
     * only if it is closer to the base than to every neighbour already kept. This skips
     * candidates that sit in the same direction as a kept neighbour, so links spread out and the
     * graph stays connected across clusters.
     *
     * @param candidates closest to the base first, with distances measured from the base
     */
    private List<Candidate> selectNeighbours(List<Candidate> candidates, int max) {
        List<Candidate> selected = new ArrayList<>(max);
        for (Candidate candidate : candidates) {
            if (selected.size() == max) {
                break;
            }
            boolean diverse = true;
            for (Candidate kept : selected) {
                if (distanceBetween(candidate.node(), kept.node()) < candidate.distance()) {
                    diverse = false;
                    break;
                }
            }
            if (diverse) {
                selected.add(candidate);
            }
        }
        return selected;
    }

    /**
     * Adds the back-link {@code target -> newNeighbour}. When {@code target} is already at its
     * cap, its links are re-chosen with the heuristic from the old set plus the new node.
     */
    private void link(int target, int newNeighbour, float distanceToTarget, int layer) {
        Node node = nodes.get(target);
        int[] links = node.neighbours[layer];
        int count = node.neighbourCount[layer];
        if (count < links.length) {
            links[count] = newNeighbour;
            node.neighbourCount[layer]++;
            return;
        }
        List<Candidate> candidates = new ArrayList<>(count + 1);
        candidates.add(new Candidate(newNeighbour, distanceToTarget));
        for (int i = 0; i < count; i++) {
            candidates.add(new Candidate(links[i], distanceBetween(target, links[i])));
        }
        candidates.sort(CLOSEST_FIRST);
        List<Candidate> kept = selectNeighbours(candidates, links.length);
        for (int i = 0; i < kept.size(); i++) {
            links[i] = kept.get(i).node();
        }
        node.neighbourCount[layer] = kept.size();
    }

    /**
     * Draws a top layer: {@code floor(-ln(U) * mL)} with {@code U} uniform on (0, 1]. With
     * {@code mL = 1 / ln(M)}, a node reaches layer {@code l} or higher with probability {@code M^-l}.
     */
    int randomLevel() {
        double uniform = 1.0 - random.nextDouble();
        return (int) Math.floor(-Math.log(uniform) * levelMultiplier);
    }

    /** Top layer of node {@code node}, in insertion order. For tests. */
    int levelOf(int node) {
        return nodes.get(node).level();
    }

    /** Copy of node {@code node}'s links on {@code layer}. For tests. */
    int[] neighboursOf(int node, int layer) {
        Node n = nodes.get(node);
        return Arrays.copyOf(n.neighbours[layer], n.neighbourCount[layer]);
    }

    private int nextVisitEpoch() {
        if (visited.length < nodes.size()) {
            visited = Arrays.copyOf(visited, Math.max(visited.length * 2, nodes.size()));
        }
        if (visitEpoch == Integer.MAX_VALUE) {
            Arrays.fill(visited, 0);
            visitEpoch = 0;
        }
        return ++visitEpoch;
    }

    /** Copies {@code vector} into row {@code row} of {@link #data}, growing the array if needed. */
    private void storeVector(int row, float[] vector) {
        if ((row + 1) * dimension > data.length) {
            data = Arrays.copyOf(data, Math.max(data.length * 2, (row + 1) * dimension));
        }
        System.arraycopy(vector, 0, data, row * dimension, dimension);
    }

    /** Distance from {@code query} to the vector stored for node {@code node}. */
    private float distanceTo(float[] query, int node) {
        return distance.compute(query, data, node * dimension);
    }

    /** Distance between the stored vectors of nodes {@code a} and {@code b}. */
    private float distanceBetween(int a, int b) {
        return distance.compute(data, a * dimension, data, b * dimension, dimension);
    }

    private void checkDimension(float[] vector) {
        if (vector.length != dimension) {
            throw new IllegalArgumentException(
                    "expected dimension " + dimension + ", got " + vector.length);
        }
    }
}
