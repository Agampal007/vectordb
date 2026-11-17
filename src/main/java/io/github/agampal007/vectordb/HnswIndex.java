package io.github.agampal007.vectordb;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.concurrent.ConcurrentLinkedQueue;

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
 * candidate extension or kept-pruned connections, which is also what hnswlib does.
 *
 * <p>Removal is by tombstone: a removed node stays in the graph so that searches can still pass
 * through it, but it is never returned. Its memory is not reclaimed, so an index with heavy churn
 * should be rebuilt.
 *
 * <p>An insert has two halves. {@link #prepareAdd} does the expensive part, finding the new
 * node's neighbours and working out their new link lists, without changing anything a search
 * reads. {@link #commit} then makes the change, which is a handful of array copies.
 *
 * <p>Threading: any number of threads may {@link #search} at the same time, because each search
 * borrows its own scratch space. Searches may also run during {@link #prepareAdd}. Only one thread
 * may write at a time, and {@link #commit}, {@link #add} and {@link #remove} need the graph to
 * themselves, with no search running. This class does not enforce any of that;
 * {@link DurableIndex} does, with a writer lock and a read-write lock.
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

    /**
     * Which nodes one layer search has seen. {@code marks[n] == epoch} means node {@code n} was
     * seen in the current search; bumping the epoch clears every mark at once instead of
     * allocating a set per search.
     */
    private static final class Visited {
        int[] marks = new int[INITIAL_CAPACITY];
        int epoch;

        /** Starts a search over {@code nodeCount} nodes and returns the epoch that marks it. */
        int nextEpoch(int nodeCount) {
            if (marks.length < nodeCount) {
                marks = Arrays.copyOf(marks, Math.max(marks.length * 2, nodeCount));
            }
            if (epoch == Integer.MAX_VALUE) {
                Arrays.fill(marks, 0);
                epoch = 0;
            }
            return ++epoch;
        }
    }

    /** A node paired with its distance to whatever is being searched for. */
    private record Candidate(int node, float distance) {}

    /** The complete new link list of node {@code target} on {@code layer}. */
    private record LinkUpdate(int target, int layer, int[] links) {}

    /**
     * An insert that has been worked out but not made: see {@link #prepareAdd} and
     * {@link #commit}.
     */
    public static final class PendingAdd {
        private final long id;
        private final float[] vector;
        /** The index the new node will have, which is how {@link #commit} detects a stale plan. */
        private final int node;
        private final int level;
        /** The new node's links on each of its layers. */
        private final int[][] links;
        private final List<LinkUpdate> updates;
        private final boolean becomesEntryPoint;

        private PendingAdd(
                long id,
                float[] vector,
                int node,
                int level,
                int[][] links,
                List<LinkUpdate> updates,
                boolean becomesEntryPoint) {
            this.id = id;
            this.vector = vector;
            this.node = node;
            this.level = level;
            this.links = links;
            this.updates = updates;
            this.becomesEntryPoint = becomesEntryPoint;
        }
    }

    /** Closest first; ties broken by node index so builds and searches are reproducible. */
    private static final Comparator<Candidate> CLOSEST_FIRST = (a, b) -> {
        int byDistance = Float.compare(a.distance(), b.distance());
        return byDistance != 0 ? byDistance : Integer.compare(a.node(), b.node());
    };

    private static final int INITIAL_CAPACITY = 16;
    private static final int[] NO_LINKS = new int[0];
    /** Passed where a pending node is expected and there is none. */
    private static final int NO_NODE = -1;
    /** Floats converted per chunk when reading or writing vectors. */
    private static final int VECTOR_IO_CHUNK = 1 << 16;
    /** Far above any level the exponential distribution produces; guards against corrupt input. */
    private static final int MAX_LEVEL = 64;

    private final int dimension;
    private final Distance distance;
    /** {@code M}: neighbours kept per node on layers above 0. */
    private final int maxConnections;
    /** {@code Mmax0}: neighbours kept per node on layer 0. The paper recommends {@code 2 * M}. */
    private final int maxConnectionsLayer0;
    /** Beam width while inserting. Higher builds a better graph, more slowly. */
    private final int efConstruction;
    /** Beam width while querying. Trades recall for speed; adjustable after building. */
    private volatile int efSearch;
    /** {@code mL = 1 / ln(M)}, the level normaliser from the paper. */
    private final double levelMultiplier;
    private final Lcg random;

    private final List<Node> nodes = new ArrayList<>();
    /** Live ids only: a removed node keeps its slot in {@link #nodes} but leaves this map. */
    private final Map<Long, Integer> nodeById = new HashMap<>();
    /** Tombstones, by node index. */
    private final BitSet removed = new BitSet();
    /** Vectors packed row-major, as in {@link FlatIndex}; row {@code i} belongs to node {@code i}. */
    private float[] data;
    private int entryPoint = -1;

    /** Scratch space for {@link #prepareAdd}: one writer at a time, so one set is enough. */
    private final Visited buildVisited = new Visited();
    /** Spare scratch space for searches: each one takes a set and puts it back when done. */
    private final ConcurrentLinkedQueue<Visited> spareVisited = new ConcurrentLinkedQueue<>();

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
        this(dimension, distance, maxConnections, efConstruction, efSearch, new Lcg(seed));
    }

    private HnswIndex(
            int dimension,
            Distance distance,
            int maxConnections,
            int efConstruction,
            int efSearch,
            Lcg random) {
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
        this.random = random;
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
        return nodeById.size();
    }

    @Override
    public boolean contains(long id) {
        return nodeById.containsKey(id);
    }

    @Override
    public float[] get(long id) {
        Integer node = nodeById.get(id);
        return node == null ? null : Arrays.copyOfRange(data, node * dimension, (node + 1) * dimension);
    }

    /** Marks the node as removed. It stays linked into the graph but is never returned. */
    @Override
    public boolean remove(long id) {
        Integer node = nodeById.remove(id);
        if (node == null) {
            return false;
        }
        removed.set(node);
        return true;
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

    /** Inserts a vector (Algorithm 1): {@link #prepareAdd} followed by {@link #commit}. */
    @Override
    public void add(long id, float[] vector) {
        checkDimension(vector);
        if (nodeById.containsKey(id)) {
            throw new IllegalArgumentException("duplicate id: " + id);
        }
        commit(prepareAdd(id, vector));
    }

    /**
     * Works out an insert without making it: draws the new node's top layer, searches each layer
     * for its neighbours, and computes the link list each of those neighbours will have once the
     * new node is linked back to them. Nothing a search reads is changed, so searches may keep
     * running. The result must be passed to {@link #commit} before any other write.
     *
     * <p>This is exact, not an approximation of what a one-step insert would do. On each layer
     * the search looks only at that layer's links, the new node is not reachable until it is
     * linked, and the neighbours chosen on one layer are distinct, so no step depends on a change
     * that an earlier step would have made.
     *
     * <p>Does not check for a duplicate id; {@link #commit} does.
     */
    public PendingAdd prepareAdd(long id, float[] vector) {
        checkDimension(vector);
        int node = nodes.size();
        int level = randomLevel();
        int[][] links = new int[level + 1][];
        Arrays.fill(links, NO_LINKS);
        List<LinkUpdate> updates = new ArrayList<>();
        if (entryPoint < 0) {
            return new PendingAdd(id, vector, node, level, links, updates, true);
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
        for (int layer = Math.min(level, entryLevel); layer >= 0; layer--) {
            List<Candidate> found = searchLayer(vector, entryPoints, efConstruction, layer, false, buildVisited);
            List<Candidate> selected = selectNeighbours(found, maxConnections, NO_NODE, null);
            int[] own = new int[selected.size()];
            for (int i = 0; i < own.length; i++) {
                Candidate neighbour = selected.get(i);
                own[i] = neighbour.node();
                updates.add(new LinkUpdate(
                        neighbour.node(),
                        layer,
                        linksAfterAdding(neighbour.node(), node, vector, neighbour.distance(), layer)));
            }
            links[layer] = own;
            entryPoints = found;
        }
        return new PendingAdd(id, vector, node, level, links, updates, level > entryLevel);
    }

    /**
     * Makes a prepared insert: stores the vector, adds the node with its links, and replaces the
     * link lists of its neighbours. No search may be running.
     *
     * @throws IllegalStateException if a node was added since the insert was prepared
     * @throws IllegalArgumentException if the id is present
     */
    public void commit(PendingAdd pending) {
        if (pending.node != nodes.size()) {
            throw new IllegalStateException("the graph changed after this insert was prepared");
        }
        if (nodeById.containsKey(pending.id)) {
            throw new IllegalArgumentException("duplicate id: " + pending.id);
        }
        storeVector(pending.node, pending.vector);
        Node inserted = new Node(pending.id, pending.level, maxConnections, maxConnectionsLayer0);
        for (int layer = 0; layer <= pending.level; layer++) {
            setLinks(inserted, layer, pending.links[layer]);
        }
        nodes.add(inserted);
        nodeById.put(pending.id, pending.node);
        for (LinkUpdate update : pending.updates) {
            setLinks(nodes.get(update.target()), update.layer(), update.links());
        }
        if (pending.becomesEntryPoint) {
            entryPoint = pending.node;
        }
    }

    private static void setLinks(Node node, int layer, int[] links) {
        System.arraycopy(links, 0, node.neighbours[layer], 0, links.length);
        node.neighbourCount[layer] = links.length;
    }

    /**
     * Searches the graph (Algorithm 5): greedy descent from the entry point to layer 1, then a
     * beam search of width {@code max(efSearch, k)} on layer 0.
     */
    @Override
    public List<SearchResult> search(float[] query, int k) {
        return search(query, k, efSearch);
    }

    /** As {@link #search(float[], int)}, with a beam width for this query only. */
    public List<SearchResult> search(float[] query, int k, int efSearch) {
        checkDimension(query);
        if (k <= 0) {
            throw new IllegalArgumentException("k must be positive: " + k);
        }
        if (efSearch <= 0) {
            throw new IllegalArgumentException("efSearch must be positive: " + efSearch);
        }
        if (entryPoint < 0) {
            return List.of();
        }
        Candidate nearest = new Candidate(entryPoint, distanceTo(query, entryPoint));
        for (int layer = nodes.get(entryPoint).level(); layer > 0; layer--) {
            nearest = greedyClosest(query, nearest, layer);
        }
        Visited visited = spareVisited.poll();
        if (visited == null) {
            visited = new Visited();
        }
        List<Candidate> found;
        try {
            found = searchLayer(query, List.of(nearest), Math.max(efSearch, k), 0, true, visited);
        } finally {
            spareVisited.offer(visited);
        }

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
     * <p>With {@code liveOnly}, removed nodes are still expanded, because the path to a live
     * neighbour may run through them, but they do not take a place among the results.
     *
     * @return up to {@code ef} nodes, closest first
     */
    private List<Candidate> searchLayer(
            float[] query, List<Candidate> entryPoints, int ef, int layer, boolean liveOnly, Visited seen) {
        int epoch = seen.nextEpoch(nodes.size());
        int[] visited = seen.marks;
        PriorityQueue<Candidate> toExpand = new PriorityQueue<>(CLOSEST_FIRST);
        PriorityQueue<Candidate> best = new PriorityQueue<>(CLOSEST_FIRST.reversed());
        for (Candidate entry : entryPoints) {
            visited[entry.node()] = epoch;
            toExpand.add(entry);
            if (!liveOnly || !removed.get(entry.node())) {
                best.add(entry);
                if (best.size() > ef) {
                    best.poll();
                }
            }
        }

        while (!toExpand.isEmpty()) {
            Candidate current = toExpand.poll();
            if (best.size() >= ef && current.distance() > best.peek().distance()) {
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
                    if (!liveOnly || !removed.get(next)) {
                        best.add(candidate);
                        if (best.size() > ef) {
                            best.poll();
                        }
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
     * @param pendingNode a candidate that is not in the graph yet, or {@link #NO_NODE}
     * @param pendingVector that candidate's vector
     */
    private List<Candidate> selectNeighbours(
            List<Candidate> candidates, int max, int pendingNode, float[] pendingVector) {
        List<Candidate> selected = new ArrayList<>(max);
        for (Candidate candidate : candidates) {
            if (selected.size() == max) {
                break;
            }
            boolean diverse = true;
            for (Candidate kept : selected) {
                if (distanceBetween(candidate.node(), kept.node(), pendingNode, pendingVector)
                        < candidate.distance()) {
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
     * The links {@code target} will have on {@code layer} once {@code newNode} is linked back to
     * it. With room to spare the new node is appended. At the cap, the links are chosen again
     * with the heuristic from the old set plus the new node.
     *
     * @param newVector the new node's vector, which is not stored in the index yet
     */
    private int[] linksAfterAdding(int target, int newNode, float[] newVector, float distanceToTarget, int layer) {
        Node node = nodes.get(target);
        int[] links = node.neighbours[layer];
        int count = node.neighbourCount[layer];
        if (count < links.length) {
            int[] appended = Arrays.copyOf(links, count + 1);
            appended[count] = newNode;
            return appended;
        }
        List<Candidate> candidates = new ArrayList<>(count + 1);
        candidates.add(new Candidate(newNode, distanceToTarget));
        for (int i = 0; i < count; i++) {
            candidates.add(new Candidate(links[i], distanceBetween(target, links[i])));
        }
        candidates.sort(CLOSEST_FIRST);
        List<Candidate> kept = selectNeighbours(candidates, links.length, newNode, newVector);
        int[] chosen = new int[kept.size()];
        for (int i = 0; i < chosen.length; i++) {
            chosen[i] = kept.get(i).node();
        }
        return chosen;
    }

    /**
     * Draws a top layer: {@code floor(-ln(U) * mL)} with {@code U} uniform on (0, 1]. With
     * {@code mL = 1 / ln(M)}, a node reaches layer {@code l} or higher with probability {@code M^-l}.
     */
    int randomLevel() {
        double uniform = 1.0 - random.nextDouble();
        return (int) Math.floor(-Math.log(uniform) * levelMultiplier);
    }

    /**
     * Writes the complete state: parameters, level generator, graph, tombstones and vectors. An
     * index read back with {@link #readFrom} behaves exactly as this one would from here on.
     */
    public void writeTo(DataOutputStream out) throws IOException {
        out.writeInt(dimension);
        out.writeUTF(distance.name());
        out.writeInt(maxConnections);
        out.writeInt(efConstruction);
        out.writeInt(efSearch);
        out.writeLong(random.state());
        out.writeInt(nodes.size());
        out.writeInt(entryPoint);
        for (int i = 0; i < nodes.size(); i++) {
            Node node = nodes.get(i);
            out.writeLong(node.id);
            out.writeBoolean(removed.get(i));
            out.writeInt(node.level());
            for (int layer = 0; layer <= node.level(); layer++) {
                out.writeInt(node.neighbourCount[layer]);
                for (int j = 0; j < node.neighbourCount[layer]; j++) {
                    out.writeInt(node.neighbours[layer][j]);
                }
            }
        }
        ByteBuffer chunk = ByteBuffer.allocate(4 * VECTOR_IO_CHUNK);
        int total = nodes.size() * dimension;
        for (int offset = 0; offset < total; offset += VECTOR_IO_CHUNK) {
            int count = Math.min(VECTOR_IO_CHUNK, total - offset);
            chunk.asFloatBuffer().put(data, offset, count);
            out.write(chunk.array(), 0, 4 * count);
        }
    }

    /**
     * Reads an index written by {@link #writeTo}.
     *
     * @param maxBytes size of the source, used to reject impossible counts before allocating
     * @throws IOException if the content is not a valid index
     */
    public static HnswIndex readFrom(DataInputStream in, long maxBytes) throws IOException {
        int dimension = in.readInt();
        Distance distance;
        try {
            distance = Distance.valueOf(in.readUTF());
        } catch (IllegalArgumentException e) {
            throw new IOException("unknown distance metric", e);
        }
        int maxConnections = in.readInt();
        int efConstruction = in.readInt();
        int efSearch = in.readInt();
        Lcg random = Lcg.fromState(in.readLong());
        int nodeCount = in.readInt();
        int entryPoint = in.readInt();
        HnswIndex index;
        try {
            index = new HnswIndex(dimension, distance, maxConnections, efConstruction, efSearch, random);
        } catch (IllegalArgumentException e) {
            throw new IOException("invalid index parameters", e);
        }
        if (nodeCount < 0 || 4L * nodeCount * dimension > maxBytes || entryPoint < -1 || entryPoint >= nodeCount) {
            throw new IOException("impossible node count or entry point");
        }
        for (int i = 0; i < nodeCount; i++) {
            long id = in.readLong();
            boolean isRemoved = in.readBoolean();
            int level = in.readInt();
            if (level < 0 || level > MAX_LEVEL) {
                throw new IOException("impossible level " + level);
            }
            Node node = new Node(id, level, maxConnections, index.maxConnectionsLayer0);
            for (int layer = 0; layer <= level; layer++) {
                int count = in.readInt();
                if (count < 0 || count > node.neighbours[layer].length) {
                    throw new IOException("impossible neighbour count " + count);
                }
                node.neighbourCount[layer] = count;
                for (int j = 0; j < count; j++) {
                    int neighbour = in.readInt();
                    if (neighbour < 0 || neighbour >= nodeCount) {
                        throw new IOException("link to a node that does not exist");
                    }
                    node.neighbours[layer][j] = neighbour;
                }
            }
            index.nodes.add(node);
            if (isRemoved) {
                index.removed.set(i);
            } else if (index.nodeById.put(id, i) != null) {
                throw new IOException("duplicate id " + id);
            }
        }
        int total = nodeCount * dimension;
        index.data = new float[Math.max(total, INITIAL_CAPACITY * dimension)];
        byte[] bytes = new byte[4 * VECTOR_IO_CHUNK];
        for (int offset = 0; offset < total; offset += VECTOR_IO_CHUNK) {
            int count = Math.min(VECTOR_IO_CHUNK, total - offset);
            in.readFully(bytes, 0, 4 * count);
            ByteBuffer.wrap(bytes, 0, 4 * count).asFloatBuffer().get(index.data, offset, count);
        }
        index.entryPoint = entryPoint;
        return index;
    }

    /** Nodes in the graph, including removed ones. For tests. */
    int nodeCount() {
        return nodes.size();
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

    /**
     * As {@link #distanceBetween(int, int)}, where {@code pendingNode} is not stored yet and
     * {@code pendingVector} stands in for its row. The arguments keep their order, so the result
     * is bit for bit what it would be with the vector stored.
     */
    private float distanceBetween(int a, int b, int pendingNode, float[] pendingVector) {
        if (a == pendingNode) {
            return distance.compute(pendingVector, 0, data, b * dimension, dimension);
        }
        if (b == pendingNode) {
            return distance.compute(data, a * dimension, pendingVector, 0, dimension);
        }
        return distanceBetween(a, b);
    }

    private void checkDimension(float[] vector) {
        if (vector.length != dimension) {
            throw new IllegalArgumentException(
                    "expected dimension " + dimension + ", got " + vector.length);
        }
    }
}
