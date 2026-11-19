package io.github.agampal007.vectordb.server;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import io.github.agampal007.vectordb.Distance;
import io.github.agampal007.vectordb.DurableIndex;
import io.github.agampal007.vectordb.SearchResult;
import io.github.agampal007.vectordb.server.CollectionStore.Collection;
import io.github.agampal007.vectordb.server.CollectionStore.Settings;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * The HTTP API. JSON in, JSON out, on the JDK's built-in HTTP server with one virtual thread per
 * request.
 *
 * <pre>
 * GET    /health
 * GET    /collections
 * PUT    /collections/{name}                 {"dimension": 128, "distance": "l2"}
 * GET    /collections/{name}
 * DELETE /collections/{name}
 * POST   /collections/{name}/vectors         {"vectors": [{"id": 1, "vector": [..]}, ..]}
 * GET    /collections/{name}/vectors/{id}
 * DELETE /collections/{name}/vectors/{id}
 * POST   /collections/{name}/search          {"vector": [..], "k": 10}
 * POST   /collections/{name}/snapshot
 * </pre>
 *
 * <p>plus the replication and failover endpoints described in {@link Replication}.
 *
 * <p>A write is answered only after it is in the log and, with {@link SyncMode#REQUEST}, after one
 * fsync covering the whole request; on a leader with {@code minReplicas} set, only after that many
 * followers hold it too. There is no authentication and no TLS: the server is meant to sit behind
 * something that provides both, and listens on the loopback address unless told otherwise.
 */
public final class VectorDbServer implements Closeable {

    /** When the log is forced to the storage device. */
    public enum SyncMode {
        /** Once per write request, before it is answered: an answered write survives power loss. */
        REQUEST,
        /** Never explicitly. An answered write survives a crash of the server, not of the machine. */
        NONE
    }

    /**
     * @param snapshotLogBytes a collection is snapshotted when its log grows past this
     * @param maxBodyBytes larger request bodies are refused with 413
     * @param minReplicas on a leader, how many followers must hold a write before it is answered
     * @param replicationTimeoutMillis how long a leader waits for them before answering 503
     * @param follow for a new data directory, the leader to follow; null to lead. An existing data
     *     directory keeps the role it has.
     */
    public record Options(
            SyncMode sync,
            long snapshotLogBytes,
            int maxBodyBytes,
            int minReplicas,
            long replicationTimeoutMillis,
            String follow) {

        public Options {
            if (minReplicas < 0) {
                throw new IllegalArgumentException("minReplicas must not be negative: " + minReplicas);
            }
            if (replicationTimeoutMillis < 1) {
                throw new IllegalArgumentException("the replication timeout must be positive");
            }
            if (follow != null) {
                follow = Replication.baseUrl(follow, "follow");
            }
        }

        /** A node on its own: no replicas, and it leads. */
        public Options(SyncMode sync, long snapshotLogBytes, int maxBodyBytes) {
            this(sync, snapshotLogBytes, maxBodyBytes, 0, 5_000, null);
        }

        public static Options defaults() {
            return new Options(SyncMode.REQUEST, 64L << 20, 32 << 20);
        }

        public Options withReplication(int minReplicas, long replicationTimeoutMillis, String follow) {
            return new Options(sync, snapshotLogBytes, maxBodyBytes, minReplicas, replicationTimeoutMillis, follow);
        }
    }

    /** Returned by a handler that has already sent its response. */
    static final Object RESPONDED = new Object();

    private static final int MAX_K = 10_000;

    private final CollectionStore store;
    private final Options options;
    private final Replication replication;
    private final HttpServer http;
    private final ExecutorService executor;

    private VectorDbServer(
            CollectionStore store, Options options, Replication replication, HttpServer http, ExecutorService executor) {
        this.store = store;
        this.options = options;
        this.replication = replication;
        this.http = http;
        this.executor = executor;
    }

    /**
     * Opens the data directory, replays its logs, starts listening and, on a follower, starts
     * following.
     *
     * @param port 0 picks a free port; see {@link #port()}
     */
    public static VectorDbServer start(Path dataDirectory, String host, int port, Options options)
            throws IOException {
        CollectionStore store = CollectionStore.open(dataDirectory, options.follow());
        Replication replication = new Replication(store, options);
        try {
            HttpServer http = HttpServer.create(new InetSocketAddress(host, port), 0);
            ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
            VectorDbServer server = new VectorDbServer(store, options, replication, http, executor);
            http.setExecutor(executor);
            http.createContext("/", server::handle);
            http.start();
            replication.start();
            return server;
        } catch (IOException | RuntimeException e) {
            replication.close();
            store.close();
            throw e;
        }
    }

    public int port() {
        return http.getAddress().getPort();
    }

    /** This node's role and term, for logging. */
    public String describeRole() {
        NodeState node = store.node();
        return node.role() == NodeState.Role.LEADER
                ? "leader in term " + node.term()
                : "follower of " + node.leader() + " in term " + node.term();
    }

    /**
     * Stops following, stops accepting requests, lets running ones finish, snapshots and closes
     * every collection.
     */
    @Override
    public void close() throws IOException {
        replication.close();
        http.stop(1);
        executor.shutdown();
        store.close();
    }

    private void handle(HttpExchange exchange) throws IOException {
        int status;
        Object body;
        try {
            body = route(exchange);
            if (body == RESPONDED) {
                return;
            }
            status = "PUT".equals(exchange.getRequestMethod()) ? 201 : 200;
        } catch (ApiException e) {
            status = e.status;
            body = Map.of("error", e.getMessage());
        } catch (IllegalArgumentException e) {
            status = 400;
            body = Map.of("error", String.valueOf(e.getMessage()));
        } catch (IllegalStateException e) {
            // The collection was dropped, or the server is shutting down, while this request ran.
            status = 409;
            body = Map.of("error", String.valueOf(e.getMessage()));
        } catch (IOException | RuntimeException e) {
            // Details go to the log, not to the client.
            System.err.println(exchange.getRequestMethod() + " " + exchange.getRequestURI() + " failed: " + e);
            status = 500;
            body = Map.of("error", "internal error");
        }
        byte[] bytes = Json.write(body).getBytes(StandardCharsets.UTF_8);
        try (exchange) {
            exchange.getResponseHeaders().set("Content-Type", "application/json");
            exchange.sendResponseHeaders(status, bytes.length);
            exchange.getResponseBody().write(bytes);
        }
    }

    private Object route(HttpExchange exchange) throws IOException {
        String method = exchange.getRequestMethod();
        String path = exchange.getRequestURI().getRawPath();
        String[] parts = path.split("/", -1); // "/collections/x" -> ["", "collections", "x"]
        if (parts.length == 2 && parts[1].equals("health")) {
            requireMethod(method, "GET");
            return Map.of("status", "ok");
        }
        if (parts.length >= 2 && parts[1].equals("replication")) {
            return replication.replicationRequest(exchange, parts);
        }
        if (parts.length == 3 && parts[1].equals("admin")) {
            requireMethod(method, "POST");
            return replication.adminRequest(parts[2], readObject(exchange));
        }
        if (parts.length < 2 || !parts[1].equals("collections") || parts.length > 5) {
            throw new ApiException(404, "no such path: " + path);
        }
        if (parts.length == 2) {
            requireMethod(method, "GET");
            List<Object> all = new ArrayList<>();
            for (Collection collection : store.list()) {
                all.add(describe(collection));
            }
            return Map.of("collections", all);
        }
        String name = parts[2];
        if (!CollectionStore.NAME.matcher(name).matches()) {
            throw new ApiException(400, "a collection name is 1 to 64 letters, digits, '-' or '_'");
        }
        if (parts.length == 3) {
            return switch (method) {
                case "PUT" -> {
                    replication.checkWritable();
                    Object created = create(name, readObject(exchange));
                    replication.catalogChanged();
                    yield created;
                }
                case "GET" -> describe(existing(name));
                case "DELETE" -> {
                    replication.checkWritable();
                    if (!store.drop(name)) {
                        throw noCollection(name);
                    }
                    replication.catalogChanged();
                    yield Map.of("dropped", true);
                }
                default -> throw methodNotAllowed(method);
            };
        }
        String resource = parts[3];
        if (parts.length == 4) {
            requireMethod(method, "POST");
            return switch (resource) {
                case "vectors" -> {
                    replication.checkWritable();
                    yield upsert(existing(name), readObject(exchange));
                }
                case "search" -> search(existing(name), readObject(exchange));
                case "snapshot" -> {
                    existing(name).index().snapshot();
                    yield Map.of("snapshot", true);
                }
                default -> throw new ApiException(404, "no such path: " + path);
            };
        }
        if (!resource.equals("vectors")) {
            throw new ApiException(404, "no such path: " + path);
        }
        long id;
        try {
            id = Long.parseLong(parts[4]);
        } catch (NumberFormatException e) {
            throw new ApiException(400, "a vector id is a 64-bit integer");
        }
        return switch (method) {
            case "GET" -> {
                float[] vector = existing(name).index().get(id);
                if (vector == null) {
                    throw new ApiException(404, "no vector with id " + id);
                }
                yield ordered("id", id, "vector", vector);
            }
            case "DELETE" -> {
                replication.checkWritable();
                yield remove(existing(name), id);
            }
            default -> throw methodNotAllowed(method);
        };
    }

    private Object create(String name, Map<String, Object> request) throws IOException {
        Settings settings = new Settings(
                intField(request, "dimension", null),
                distanceField(request),
                intField(request, "m", 16),
                intField(request, "efConstruction", 200),
                intField(request, "efSearch", 50));
        Collection created = store.create(name, settings);
        if (created == null) {
            throw new ApiException(409, "collection " + name + " already exists");
        }
        return describe(created);
    }

    private Object upsert(Collection collection, Map<String, Object> request) throws IOException {
        if (!(request.get("vectors") instanceof List<?> items) || items.isEmpty()) {
            throw new ApiException(400, "\"vectors\" must be a non-empty array");
        }
        // Check the whole request before changing anything, so a bad entry rejects all of it.
        int dimension = collection.settings().dimension();
        long[] ids = new long[items.size()];
        float[][] vectors = new float[items.size()][];
        for (int i = 0; i < ids.length; i++) {
            if (!(items.get(i) instanceof Map<?, ?> item)) {
                throw new ApiException(400, "vectors[" + i + "] must be an object");
            }
            if (!(item.get("id") instanceof Long id)) {
                throw new ApiException(400, "vectors[" + i + "].id must be a 64-bit integer");
            }
            ids[i] = id;
            vectors[i] = vector(item.get("vector"), dimension, "vectors[" + i + "].vector");
        }
        DurableIndex index = collection.index();
        int replaced = 0;
        for (int i = 0; i < ids.length; i++) {
            if (index.upsert(ids[i], vectors[i])) {
                replaced++;
            }
        }
        finishWrite(index);
        replication.awaitReplicas(collection, index.lastPosition().sequence());
        return ordered("upserted", ids.length, "replaced", replaced);
    }

    private Object remove(Collection collection, long id) throws IOException {
        DurableIndex index = collection.index();
        if (!index.remove(id)) {
            throw new ApiException(404, "no vector with id " + id);
        }
        finishWrite(index);
        replication.awaitReplicas(collection, index.lastPosition().sequence());
        return Map.of("removed", true);
    }

    /** Makes the request durable before it is answered, and keeps the log from growing without bound. */
    private void finishWrite(DurableIndex index) throws IOException {
        if (options.sync() == SyncMode.REQUEST) {
            index.sync();
        }
        index.snapshotIfLogExceeds(options.snapshotLogBytes());
    }

    private Object search(Collection collection, Map<String, Object> request) {
        Settings settings = collection.settings();
        float[] query = vector(request.get("vector"), settings.dimension(), "vector");
        int k = intField(request, "k", 10);
        int efSearch = intField(request, "efSearch", settings.efSearch());
        if (k < 1 || k > MAX_K) {
            throw new ApiException(400, "k must be between 1 and " + MAX_K);
        }
        if (efSearch < 1 || efSearch > Settings.MAX_EF) {
            throw new ApiException(400, "efSearch must be between 1 and " + Settings.MAX_EF);
        }
        List<Object> results = new ArrayList<>(k);
        for (SearchResult result : collection.index().search(query, k, efSearch)) {
            results.add(ordered("id", result.id(), "distance", result.distance()));
        }
        return Map.of("results", results);
    }

    private Collection existing(String name) {
        Collection collection = store.get(name);
        if (collection == null) {
            throw noCollection(name);
        }
        return collection;
    }

    private static Map<String, Object> describe(Collection collection) {
        Settings settings = collection.settings();
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("name", collection.name());
        out.put("dimension", settings.dimension());
        out.put("distance", settings.distance().name().toLowerCase(Locale.ROOT));
        out.put("m", settings.m());
        out.put("efConstruction", settings.efConstruction());
        out.put("efSearch", settings.efSearch());
        out.put("size", collection.index().size());
        return out;
    }

    /** Reads the request body as a JSON object, refusing bodies over the size limit. */
    private Map<String, Object> readObject(HttpExchange exchange) throws IOException {
        byte[] bytes;
        try (InputStream in = exchange.getRequestBody()) {
            bytes = in.readNBytes(options.maxBodyBytes() + 1);
        }
        if (bytes.length > options.maxBodyBytes()) {
            throw new ApiException(413, "request body is larger than " + options.maxBodyBytes() + " bytes");
        }
        Object parsed = Json.parse(new String(bytes, StandardCharsets.UTF_8));
        if (!(parsed instanceof Map<?, ?> map)) {
            throw new ApiException(400, "the request body must be a JSON object");
        }
        Map<String, Object> object = new LinkedHashMap<>();
        map.forEach((key, value) -> object.put((String) key, value));
        return object;
    }

    private static float[] vector(Object value, int dimension, String field) {
        if (!(value instanceof List<?> numbers)) {
            throw new ApiException(400, field + " must be an array of numbers");
        }
        if (numbers.size() != dimension) {
            throw new ApiException(400, field + " has " + numbers.size() + " numbers, expected " + dimension);
        }
        float[] vector = new float[dimension];
        for (int i = 0; i < dimension; i++) {
            if (!(numbers.get(i) instanceof Number number)) {
                throw new ApiException(400, field + " must be an array of numbers");
            }
            vector[i] = number.floatValue();
            if (!Float.isFinite(vector[i])) {
                throw new ApiException(400, field + "[" + i + "] does not fit in a 32-bit float");
            }
        }
        return vector;
    }

    /** An integer field of the request, or {@code fallback} if it is absent. A null fallback makes it required. */
    private static int intField(Map<String, Object> request, String field, Integer fallback) {
        Object value = request.get(field);
        if (value == null && fallback != null) {
            return fallback;
        }
        if (!(value instanceof Long number) || number < Integer.MIN_VALUE || number > Integer.MAX_VALUE) {
            throw new ApiException(400, "\"" + field + "\" must be an integer");
        }
        return number.intValue();
    }

    private static Distance distanceField(Map<String, Object> request) {
        Object value = request.getOrDefault("distance", "l2");
        if (value instanceof String name) {
            for (Distance distance : Distance.values()) {
                if (distance.name().equalsIgnoreCase(name)) {
                    return distance;
                }
            }
        }
        throw new ApiException(400, "\"distance\" must be \"l2\", \"cosine\" or \"dot\"");
    }

    private static Map<String, Object> ordered(String key1, Object value1, String key2, Object value2) {
        Map<String, Object> map = new LinkedHashMap<>();
        map.put(key1, value1);
        map.put(key2, value2);
        return map;
    }

    private static void requireMethod(String method, String allowed) {
        if (!method.equals(allowed)) {
            throw methodNotAllowed(method);
        }
    }

    private static ApiException methodNotAllowed(String method) {
        return new ApiException(405, "method " + method + " is not allowed here");
    }

    private static ApiException noCollection(String name) {
        return new ApiException(404, "no collection called " + name);
    }
}
