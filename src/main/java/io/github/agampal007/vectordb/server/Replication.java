package io.github.agampal007.vectordb.server;

import com.sun.net.httpserver.HttpExchange;
import io.github.agampal007.vectordb.DurableIndex;
import io.github.agampal007.vectordb.DurableIndex.Position;
import io.github.agampal007.vectordb.server.CollectionStore.Catalog;
import io.github.agampal007.vectordb.server.CollectionStore.Collection;
import io.github.agampal007.vectordb.server.CollectionStore.Settings;
import io.github.agampal007.vectordb.server.NodeState.Role;
import io.github.agampal007.vectordb.server.ReplicaClient.PeerStatus;
import io.github.agampal007.vectordb.server.VectorDbServer.Options;
import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.net.URLDecoder;
import java.net.http.HttpClient;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Leader–follower replication on the server: the endpoints, the waiting that makes a write on the
 * leader wait for followers, and the switches between roles.
 *
 * <pre>
 * GET  /replication/status                        role, term, and the position in each collection
 * GET  /replication/catalog                       the collection list; followers long-poll it
 * GET  /replication/collections/{name}/log        log records after a position; followers long-poll it
 * GET  /replication/collections/{name}/snapshot   a snapshot to start again from
 * POST /admin/promote  {"peers": ["http://..."]}  catch up from the peers, then lead in a new term
 * POST /admin/follow   {"leader": "http://..."}   stop leading or following, and follow this leader
 * </pre>
 *
 * <p>A follower's requests carry {@code follower=<node id>}. Only the leader answers them, and
 * each one acknowledges the position it asks from. A node being promoted sends
 * {@code peer=true} instead: any node answers, without waiting and without counting it.
 *
 * <p>Every request carries the sender's term. A follower that hears of a higher term adopts it and
 * from then on refuses answers from any node in a lower one. A leader that hears of a higher term
 * knows another node has been promoted, and stops accepting writes.
 */
final class Replication implements Closeable {

    private static final long MAX_WAIT_MILLIS = 10_000;
    private static final int MAX_BATCH_BYTES = 16 << 20;

    private final CollectionStore store;
    private final NodeState node;
    private final Options options;
    private final ReplicaTracker tracker = new ReplicaTracker();
    private final HttpClient http;
    /** Lets a follower's request for the collection list wait until it changes. */
    private final ReentrantLock catalogLock = new ReentrantLock();
    private final Condition catalogChanged = catalogLock.newCondition();
    /** Serialises promote, follow and close. */
    private final ReentrantLock roleLock = new ReentrantLock();
    private Follower follower;
    /** Set when a leader learns that a node has been promoted in a later term. */
    private volatile boolean deposed;

    Replication(CollectionStore store, Options options) {
        this.store = store;
        this.node = store.node();
        this.options = options;
        this.http = HttpClient.newBuilder()
                .version(HttpClient.Version.HTTP_1_1)
                .connectTimeout(Duration.ofSeconds(2))
                .build();
    }

    /** Starts following, if this node is a follower. */
    void start() {
        roleLock.lock();
        try {
            if (node.role() == Role.FOLLOWER) {
                startFollowing(node.leader());
            }
        } finally {
            roleLock.unlock();
        }
    }

    @Override
    public void close() {
        roleLock.lock();
        try {
            stopFollowing();
        } finally {
            roleLock.unlock();
        }
        // Abort rather than wait: a follower's long poll could otherwise hold this up.
        http.shutdownNow();
        try {
            http.awaitTermination(Duration.ofSeconds(5));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    // ---- The write path -------------------------------------------------------------------------

    /** Refuses a write unless this node is the leader and has not heard of a later one. */
    void checkWritable() {
        if (node.role() != Role.LEADER) {
            throw new ApiException(409, "this node follows " + node.leader() + "; send writes to the leader");
        }
        if (deposed) {
            throw new ApiException(409, "this node led term " + node.term()
                    + ", but another node has since been promoted; it no longer accepts writes");
        }
    }

    /** Holds a write's answer until enough followers hold the collection's log up to {@code sequence}. */
    void awaitReplicas(Collection collection, long sequence) {
        int replicas = options.minReplicas();
        if (replicas == 0) {
            return;
        }
        try {
            if (!tracker.awaitLog(collection.incarnation(), sequence, replicas,
                    options.replicationTimeoutMillis(), TimeUnit.MILLISECONDS)) {
                throw notReplicated(replicas);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw notReplicated(replicas);
        }
    }

    /** Wakes followers waiting for the collection list, then holds the answer until enough of them hold it. */
    void catalogChanged() {
        Catalog catalog = store.catalog();
        catalogLock.lock();
        try {
            catalogChanged.signalAll();
        } finally {
            catalogLock.unlock();
        }
        int replicas = options.minReplicas();
        if (replicas == 0) {
            return;
        }
        try {
            if (!tracker.awaitCatalog(catalog.term(), catalog.version(), replicas,
                    options.replicationTimeoutMillis(), TimeUnit.MILLISECONDS)) {
                throw notReplicated(replicas);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw notReplicated(replicas);
        }
    }

    private ApiException notReplicated(int replicas) {
        return new ApiException(503, "written on this node, but fewer than " + replicas
                + " follower(s) confirmed it within " + options.replicationTimeoutMillis()
                + " ms; it may or may not survive a failover");
    }

    // ---- Endpoints ------------------------------------------------------------------------------

    /** Answers a GET under /replication. Returns a JSON body, or {@link VectorDbServer#RESPONDED}. */
    Object replicationRequest(HttpExchange exchange, String[] parts) throws IOException {
        if (!exchange.getRequestMethod().equals("GET")) {
            throw new ApiException(405, "method " + exchange.getRequestMethod() + " is not allowed here");
        }
        Map<String, String> query = query(exchange);
        if (parts.length == 3 && parts[2].equals("status")) {
            return status(query);
        }
        if (parts.length == 3 && parts[2].equals("catalog")) {
            return catalog(query);
        }
        if (parts.length == 5 && parts[2].equals("collections")) {
            switch (parts[4]) {
                case "log":
                    return log(exchange, parts[3], query);
                case "snapshot":
                    return snapshot(exchange, parts[3], query);
                default:
                    break;
            }
        }
        throw new ApiException(404, "no such path: " + exchange.getRequestURI().getRawPath());
    }

    /** Answers a POST under /admin. */
    Object adminRequest(String action, Map<String, Object> body) throws IOException {
        return switch (action) {
            case "promote" -> promote(body);
            case "follow" -> follow(body);
            default -> throw new ApiException(404, "no such path: /admin/" + action);
        };
    }

    private Map<String, Object> status(Map<String, String> query) throws IOException {
        observeTerm(number(query, "term", -1));
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("id", node.id());
        out.put("role", node.role().name().toLowerCase(Locale.ROOT));
        out.put("term", node.term());
        out.put("deposed", deposed);
        out.put("leader", node.leader());
        out.put("catalogTerm", node.catalogTerm());
        out.put("catalogVersion", node.catalogVersion());
        List<Object> collections = new ArrayList<>();
        Map<String, String> names = new HashMap<>();
        for (Collection listed : store.list()) {
            Map<String, Object> entry = describePosition(listed);
            if (entry != null) {
                collections.add(entry);
                names.put((String) entry.get("incarnation"), listed.name());
            }
        }
        out.put("collections", collections);
        if (node.role() == Role.LEADER) {
            out.put("followers", tracker.describe(names));
        }
        Follower current = follower;
        if (current != null) {
            out.put("replication", current.describe());
        }
        return out;
    }

    /**
     * A collection's size and position, read from the store again if its index is replaced while
     * this runs (a follower installing a snapshot closes the old one). Null if it was dropped.
     */
    private Map<String, Object> describePosition(Collection listed) {
        Collection collection = listed;
        for (int attempt = 0; collection != null; attempt++) {
            try {
                Position position = collection.index().lastPosition();
                Map<String, Object> entry = new LinkedHashMap<>();
                entry.put("name", collection.name());
                entry.put("incarnation", collection.incarnation());
                entry.put("size", collection.index().size());
                entry.put("sequence", position.sequence());
                entry.put("term", position.term());
                return entry;
            } catch (IllegalStateException e) {
                if (attempt == 100) {
                    throw e;
                }
                Thread.onSpinWait();
                collection = store.get(listed.name());
            }
        }
        return null;
    }

    private Map<String, Object> catalog(Map<String, String> query) throws IOException {
        String follower = sender(query);
        long theirTerm = number(query, "catalogTerm", -1);
        long theirVersion = number(query, "catalogVersion", -1);
        if (follower != null) {
            tracker.ackCatalog(follower, theirTerm, theirVersion);
        }
        Catalog catalog = store.catalog();
        long wait = follower == null ? 0 : Math.min(number(query, "wait", 0), MAX_WAIT_MILLIS);
        if (wait > 0) {
            long nanos = TimeUnit.MILLISECONDS.toNanos(wait);
            catalogLock.lock();
            try {
                // Read again under the lock: a change after this is signalled to this thread.
                catalog = store.catalog();
                while (catalog.term() == theirTerm && catalog.version() == theirVersion && nanos > 0) {
                    nanos = catalogChanged.awaitNanos(nanos);
                    catalog = store.catalog();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } finally {
                catalogLock.unlock();
            }
        }
        List<Object> collections = new ArrayList<>();
        for (Collection collection : catalog.collections()) {
            Settings settings = collection.settings();
            Map<String, Object> entry = new LinkedHashMap<>();
            entry.put("name", collection.name());
            entry.put("incarnation", collection.incarnation());
            entry.put("dimension", settings.dimension());
            entry.put("distance", settings.distance().name().toLowerCase(Locale.ROOT));
            entry.put("m", settings.m());
            entry.put("efConstruction", settings.efConstruction());
            entry.put("efSearch", settings.efSearch());
            collections.add(entry);
        }
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("term", node.term());
        out.put("catalogTerm", catalog.term());
        out.put("catalogVersion", catalog.version());
        out.put("collections", collections);
        return out;
    }

    private Object log(HttpExchange exchange, String name, Map<String, String> query) throws IOException {
        String follower = sender(query);
        Collection collection = replicaOf(name, query);
        Position after = new Position(number(query, "after", null), number(query, "afterTerm", null));
        int maxBytes = (int) Math.max(1, Math.min(number(query, "maxBytes", MAX_BATCH_BYTES), MAX_BATCH_BYTES));
        DurableIndex index = collection.index();
        byte[] records = index.readLogAfter(after, maxBytes);
        if (records != null && follower != null) {
            // The follower holds everything up to its position on its own device: that is the acknowledgement.
            tracker.ackLog(collection.incarnation(), follower, after.sequence());
            long wait = Math.min(number(query, "wait", 0), MAX_WAIT_MILLIS);
            if (records.length == 0 && wait > 0) {
                try {
                    index.awaitSyncedAfter(after.sequence(), wait, TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                records = index.readLogAfter(after, maxBytes);
            }
        }
        if (records == null) {
            throw new ApiException(410, "this log does not continue from sequence " + after.sequence()
                    + " in term " + after.term() + "; start again from a snapshot");
        }
        exchange.getResponseHeaders().set("Content-Type", "application/octet-stream");
        exchange.getResponseHeaders().set(ReplicaClient.TERM_HEADER, Long.toString(node.term()));
        try (exchange) {
            exchange.sendResponseHeaders(200, records.length == 0 ? -1 : records.length);
            exchange.getResponseBody().write(records);
        } catch (IOException e) {
            // The follower went away; it will ask again.
        }
        return VectorDbServer.RESPONDED;
    }

    private Object snapshot(HttpExchange exchange, String name, Map<String, String> query) throws IOException {
        sender(query);
        Collection collection = replicaOf(name, query);
        DurableIndex.Transfer transfer = collection.index().writeTransferSnapshot();
        try (exchange) {
            exchange.getResponseHeaders().set("Content-Type", "application/octet-stream");
            exchange.getResponseHeaders().set(ReplicaClient.TERM_HEADER, Long.toString(node.term()));
            exchange.sendResponseHeaders(200, Files.size(transfer.file()));
            try (OutputStream out = exchange.getResponseBody()) {
                Files.copy(transfer.file(), out);
            }
        } catch (IOException e) {
            // The follower went away; it will ask again.
        } finally {
            Files.deleteIfExists(transfer.file());
        }
        return VectorDbServer.RESPONDED;
    }

    // ---- Roles ----------------------------------------------------------------------------------

    /**
     * Makes this node the leader. First it learns the terms of the peers it is given and announces
     * a term above all of them, which they adopt, so they refuse anything more from the old leader.
     * Then it copies, from whichever peer is furthest ahead, whatever it is missing: a write the
     * old leader acknowledged after one follower confirmed it is then not lost, whichever follower
     * is promoted, as long as that one follower is among the peers. Only then does it lead.
     */
    private Map<String, Object> promote(Map<String, Object> request) throws IOException {
        List<String> peers = urls(request.get("peers"), "peers");
        roleLock.lock();
        try {
            if (node.role() == Role.LEADER && !deposed) {
                throw new ApiException(409, "this node is already the leader, in term " + node.term());
            }
            stopFollowing();
            ReplicaClient client = new ReplicaClient(http, store, node, options.snapshotLogBytes(), false);
            Map<String, PeerStatus> reachable = new LinkedHashMap<>();
            List<String> unreachable = new ArrayList<>();
            long term = node.term();
            for (String peer : peers) {
                try {
                    PeerStatus status = client.status(peer, -1);
                    reachable.put(peer, status);
                    term = Math.max(term, status.term());
                } catch (IOException e) {
                    unreachable.add(peer);
                }
            }
            long newTerm = term + 1;
            node.raiseTerm(newTerm);
            for (String peer : reachable.keySet()) {
                try {
                    client.status(peer, newTerm);
                } catch (IOException e) {
                    // It answered a moment ago; if it is gone now, catching up from it fails below.
                }
            }
            List<Object> caughtUp;
            try {
                caughtUp = client.catchUp(reachable);
            } catch (IOException e) {
                throw new ApiException(502, "catching up from the peers failed, so this node did not take over: "
                        + e.getMessage() + ". Promote again, or follow a leader.");
            }
            store.setTerm(newTerm);
            node.becomeLeader();
            deposed = false;
            tracker.clear();
            System.out.println("promoted: leading in term " + newTerm);
            Map<String, Object> out = new LinkedHashMap<>();
            out.put("term", newTerm);
            out.put("caughtUp", caughtUp);
            out.put("unreachable", unreachable);
            return out;
        } finally {
            roleLock.unlock();
        }
    }

    private Map<String, Object> follow(Map<String, Object> request) throws IOException {
        if (!(request.get("leader") instanceof String text)) {
            throw new ApiException(400, "\"leader\" must be a URL like http://host:8080");
        }
        String leader = baseUrl(text, "leader");
        roleLock.lock();
        try {
            stopFollowing();
            node.becomeFollower(leader);
            deposed = false;
            tracker.clear();
            startFollowing(leader);
            System.out.println("following " + leader);
        } finally {
            roleLock.unlock();
        }
        return Map.of("following", leader);
    }

    private void startFollowing(String leader) {
        follower = new Follower(store, leader, http, options.snapshotLogBytes());
        follower.start();
    }

    private void stopFollowing() {
        if (follower != null) {
            follower.stop();
            follower = null;
        }
    }

    // ---- Helpers --------------------------------------------------------------------------------

    /**
     * Reacts to a sender's term, then says who sent the request: a follower's id, or null for a
     * peer. Only an active leader answers followers.
     */
    private String sender(Map<String, String> query) throws IOException {
        observeTerm(number(query, "term", -1));
        String follower = query.get("follower");
        if (follower == null) {
            if (!"true".equals(query.get("peer"))) {
                throw new ApiException(400, "say which follower is asking (follower=<id>), or that a peer is (peer=true)");
            }
            return null;
        }
        if (node.role() != Role.LEADER) {
            throw new ApiException(409, "this node is a follower of " + node.leader() + ", not the leader");
        }
        if (deposed) {
            throw new ApiException(409, "this node is no longer the leader: a node has been promoted in a later term");
        }
        return follower;
    }

    /** A follower adopts a higher term. A leader that sees one has been replaced. */
    private void observeTerm(long theirTerm) throws IOException {
        if (theirTerm <= node.term()) {
            return;
        }
        if (node.role() == Role.LEADER) {
            if (!deposed) {
                deposed = true;
                System.out.println("deposed: a node is in term " + theirTerm + ", later than this leader's term "
                        + node.term() + "; no longer accepting writes");
            }
        } else {
            node.raiseTerm(theirTerm);
        }
    }

    private Collection replicaOf(String name, Map<String, String> query) {
        Collection collection = store.get(name);
        if (collection == null || !collection.incarnation().equals(query.get("incarnation"))) {
            throw new ApiException(404, "no collection " + name + " with incarnation " + query.get("incarnation"));
        }
        return collection;
    }

    private static Map<String, String> query(HttpExchange exchange) {
        Map<String, String> out = new HashMap<>();
        String raw = exchange.getRequestURI().getRawQuery();
        if (raw == null || raw.isEmpty()) {
            return out;
        }
        for (String pair : raw.split("&")) {
            int equals = pair.indexOf('=');
            String key = URLDecoder.decode(equals < 0 ? pair : pair.substring(0, equals), StandardCharsets.UTF_8);
            String value = equals < 0 ? "" : URLDecoder.decode(pair.substring(equals + 1), StandardCharsets.UTF_8);
            out.put(key, value);
        }
        return out;
    }

    /** A whole-number query parameter, or {@code fallback} if it is absent. A null fallback makes it required. */
    private static long number(Map<String, String> query, String name, Integer fallback) {
        String value = query.get(name);
        if (value == null) {
            if (fallback == null) {
                throw new ApiException(400, "the " + name + " parameter is required");
            }
            return fallback;
        }
        try {
            return Long.parseLong(value);
        } catch (NumberFormatException e) {
            throw new ApiException(400, "the " + name + " parameter must be a whole number");
        }
    }

    /** Base URLs of other nodes, like {@code http://node2:8080}, without a trailing slash. */
    private static List<String> urls(Object value, String field) {
        if (value == null) {
            return List.of();
        }
        if (!(value instanceof List<?> items)) {
            throw new ApiException(400, "\"" + field + "\" must be a list of URLs");
        }
        List<String> out = new ArrayList<>();
        for (Object item : items) {
            if (!(item instanceof String text)) {
                throw new ApiException(400, "\"" + field + "\" must hold URLs like http://host:8080");
            }
            out.add(baseUrl(text, field));
        }
        return out;
    }

    /**
     * Checks a node's base URL, like {@code http://node2:8080}, and drops a trailing slash.
     *
     * @throws IllegalArgumentException if it is anything else
     */
    static String baseUrl(String text, String field) {
        String trimmed = text.endsWith("/") ? text.substring(0, text.length() - 1) : text;
        boolean valid;
        try {
            URI uri = URI.create(trimmed);
            valid = "http".equals(uri.getScheme())
                    && uri.getHost() != null
                    && (uri.getRawPath() == null || uri.getRawPath().isEmpty())
                    && uri.getRawQuery() == null;
        } catch (IllegalArgumentException e) {
            valid = false;
        }
        if (!valid) {
            throw new IllegalArgumentException(field + " must be a URL like http://host:8080, not " + text);
        }
        return trimmed;
    }
}
