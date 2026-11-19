package io.github.agampal007.vectordb.server;

import io.github.agampal007.vectordb.Distance;
import io.github.agampal007.vectordb.DurableIndex;
import io.github.agampal007.vectordb.DurableIndex.Position;
import io.github.agampal007.vectordb.server.CollectionStore.Collection;
import io.github.agampal007.vectordb.server.CollectionStore.Settings;
import io.github.agampal007.vectordb.wal.WriteAheadLog;
import io.github.agampal007.vectordb.wal.WriteAheadLog.Entry;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandler;
import java.net.http.HttpResponse.BodyHandlers;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;

/**
 * The client half of replication: asks another node for its state, its collection list, its log
 * and its snapshots, and applies what comes back to this node.
 *
 * <p>A follower uses it against its leader, sending its node id so that its requests count as
 * acknowledgements and may wait for news. A node being promoted uses it against its peers, as a
 * peer: no waiting, nothing counted.
 *
 * <p>{@link #cancel()} abandons requests in flight instead of interrupting the threads that sent
 * them. An interrupt that landed inside a file operation would close the log's channel and leave
 * the collection unusable.
 */
final class ReplicaClient {

    /** What one pull did. */
    enum Pulled {
        APPLIED,
        UP_TO_DATE,
        RESTARTED_FROM_SNAPSHOT,
        /** The other node has no such collection, or a different incarnation of it. */
        GONE
    }

    record Remote(String name, String incarnation, Settings settings) {}

    record RemoteCatalog(long term, long catalogTerm, long catalogVersion, List<Remote> collections) {}

    /** Another node's state, as its status endpoint reports it. */
    record PeerStatus(String url, long term, long catalogTerm, long catalogVersion, Map<String, Position> positions) {}

    static final String TERM_HEADER = "X-Vectordb-Term";
    private static final int BATCH_BYTES = 4 << 20;
    private static final Duration SNAPSHOT_TIMEOUT = Duration.ofMinutes(30);

    private final HttpClient http;
    private final CollectionStore store;
    private final NodeState node;
    private final long snapshotLogBytes;
    /** True for a follower: requests carry its id and may wait; responses from an older term are refused. */
    private final boolean asFollower;
    private final Set<CompletableFuture<?>> inFlight = ConcurrentHashMap.newKeySet();
    private volatile boolean cancelled;

    ReplicaClient(HttpClient http, CollectionStore store, NodeState node, long snapshotLogBytes, boolean asFollower) {
        this.http = http;
        this.store = store;
        this.node = node;
        this.snapshotLogBytes = snapshotLogBytes;
        this.asFollower = asFollower;
    }

    /** Abandons requests in flight; later ones fail at once with {@link CancellationException}. */
    void cancel() {
        cancelled = true;
        for (CompletableFuture<?> request : inFlight) {
            request.cancel(true);
        }
    }

    /**
     * Another node's role, term and positions.
     *
     * @param announceTerm if not negative, a term the other node should adopt if it is higher
     */
    PeerStatus status(String base, long announceTerm) throws IOException {
        String query = announceTerm >= 0 ? "term=" + announceTerm : "";
        HttpResponse<String> response = send(get(base, "/replication/status", query, Duration.ofSeconds(10)), BodyHandlers.ofString());
        Map<?, ?> body = jsonObject(base, response);
        Map<String, Position> positions = new HashMap<>();
        for (Object item : (List<?>) body.get("collections")) {
            Map<?, ?> collection = (Map<?, ?>) item;
            positions.put(
                    (String) collection.get("incarnation"),
                    new Position((Long) collection.get("sequence"), (Long) collection.get("term")));
        }
        return new PeerStatus(
                base,
                (Long) body.get("term"),
                (Long) body.get("catalogTerm"),
                (Long) body.get("catalogVersion"),
                positions);
    }

    /** The other node's collection list. A follower's request waits up to {@code waitMillis} for a change. */
    RemoteCatalog catalog(String base, long waitMillis) throws IOException {
        String query = "catalogTerm=" + node.catalogTerm() + "&catalogVersion=" + node.catalogVersion() + "&wait=" + waitMillis;
        HttpResponse<String> response =
                send(get(base, "/replication/catalog", query, requestTimeout(waitMillis)), BodyHandlers.ofString());
        Map<?, ?> body = jsonObject(base, response);
        checkTerm(base, (Long) body.get("term"));
        List<Remote> collections = new ArrayList<>();
        for (Object item : (List<?>) body.get("collections")) {
            Map<?, ?> collection = (Map<?, ?>) item;
            Settings settings = new Settings(
                    ((Long) collection.get("dimension")).intValue(),
                    Distance.valueOf(((String) collection.get("distance")).toUpperCase(Locale.ROOT)),
                    ((Long) collection.get("m")).intValue(),
                    ((Long) collection.get("efConstruction")).intValue(),
                    ((Long) collection.get("efSearch")).intValue());
            collections.add(new Remote((String) collection.get("name"), (String) collection.get("incarnation"), settings));
        }
        return new RemoteCatalog(
                (Long) body.get("term"), (Long) body.get("catalogTerm"), (Long) body.get("catalogVersion"), collections);
    }

    /**
     * Makes this node's collections match {@code remote}: drops the ones it lacks or holds as a
     * different incarnation, creates the missing ones empty, and records the list's version.
     *
     * @param beforeDrop called with each collection about to be dropped
     */
    void reconcile(RemoteCatalog remote, Consumer<Collection> beforeDrop) throws IOException {
        Map<String, Remote> wanted = new LinkedHashMap<>();
        for (Remote collection : remote.collections()) {
            wanted.put(collection.name(), collection);
        }
        for (Collection local : store.list()) {
            Remote match = wanted.get(local.name());
            if (match == null || !match.incarnation().equals(local.incarnation())) {
                beforeDrop.accept(local);
                store.dropReplica(local.name());
            }
        }
        for (Remote collection : remote.collections()) {
            if (store.get(collection.name()) == null) {
                store.createReplica(collection.name(), collection.settings(), collection.incarnation());
            }
        }
        node.setCatalog(remote.catalogTerm(), remote.catalogVersion());
    }

    /**
     * Asks for the records after this collection's last operation and applies them. If the other
     * node's log does not continue from there, downloads a snapshot and starts again from it.
     */
    Pulled pull(String base, Collection collection, long waitMillis) throws IOException {
        Position after = collection.index().lastPosition();
        String query = "incarnation=" + collection.incarnation()
                + "&after=" + after.sequence()
                + "&afterTerm=" + after.term()
                + "&maxBytes=" + BATCH_BYTES
                + "&wait=" + waitMillis;
        HttpRequest request = get(base, "/replication/collections/" + collection.name() + "/log", query, requestTimeout(waitMillis));
        HttpResponse<byte[]> response = send(request, BodyHandlers.ofByteArray());
        switch (response.statusCode()) {
            case 200 -> {
                checkTerm(base, termHeader(response));
                List<Entry> entries = WriteAheadLog.decodeRecords(response.body());
                if (entries.isEmpty()) {
                    return Pulled.UP_TO_DATE;
                }
                DurableIndex index = collection.index();
                index.applyReplicated(entries);
                index.snapshotIfLogExceeds(snapshotLogBytes);
                return Pulled.APPLIED;
            }
            case 410 -> {
                restartFromSnapshot(base, collection);
                return Pulled.RESTARTED_FROM_SNAPSHOT;
            }
            case 404 -> {
                return Pulled.GONE;
            }
            default -> throw failure(base, response.statusCode(), new String(response.body(), StandardCharsets.UTF_8));
        }
    }

    private void restartFromSnapshot(String base, Collection collection) throws IOException {
        Path download = Files.createTempFile(collection.index().directory(), DurableIndex.TRANSFER_PREFIX, ".download");
        try {
            HttpRequest request = get(
                    base,
                    "/replication/collections/" + collection.name() + "/snapshot",
                    "incarnation=" + collection.incarnation(),
                    SNAPSHOT_TIMEOUT);
            HttpResponse<Path> response = send(request, BodyHandlers.ofFile(download));
            if (response.statusCode() != 200) {
                throw failure(base, response.statusCode(), Files.readString(download));
            }
            checkTerm(base, termHeader(response));
            Collection replaced = store.installSnapshot(collection.name(), collection.incarnation(), download);
            System.out.println("collection " + collection.name() + " started again from a snapshot of " + base
                    + " at " + replaced.index().lastPosition());
        } finally {
            Files.deleteIfExists(download);
        }
    }

    /**
     * Brings this node up to the furthest of its peers before it is promoted: the collection list
     * from whichever peer holds the newest one, then each collection from whichever peer is
     * furthest ahead in it, as long as that is ahead of this node.
     *
     * @return what was copied from where
     */
    List<Object> catchUp(Map<String, PeerStatus> peers) throws IOException {
        List<Object> report = new ArrayList<>();
        PeerStatus newest = null;
        for (PeerStatus peer : peers.values()) {
            boolean ahead = newest == null
                    ? node.catalogIsBehind(peer.catalogTerm(), peer.catalogVersion())
                    : peer.catalogTerm() != newest.catalogTerm()
                            ? peer.catalogTerm() > newest.catalogTerm()
                            : peer.catalogVersion() > newest.catalogVersion();
            if (ahead) {
                newest = peer;
            }
        }
        if (newest != null) {
            reconcile(catalog(newest.url(), 0), collection -> {});
            report.add(Map.of("collections", "list from " + newest.url()));
        }
        for (Collection collection : store.list()) {
            Position mine = collection.index().lastPosition();
            PeerStatus furthest = null;
            Position furthestPosition = mine;
            for (PeerStatus peer : peers.values()) {
                Position theirs = peer.positions().get(collection.incarnation());
                if (theirs != null && theirs.isAheadOf(furthestPosition)) {
                    furthest = peer;
                    furthestPosition = theirs;
                }
            }
            if (furthest == null) {
                continue;
            }
            Collection current = collection;
            for (int round = 0; round < 100_000; round++) {
                Pulled pulled = pull(furthest.url(), current, 0);
                current = store.get(collection.name()); // a snapshot replaces the index
                if (pulled == Pulled.UP_TO_DATE || pulled == Pulled.GONE || current == null) {
                    break;
                }
            }
            Map<String, Object> entry = new LinkedHashMap<>();
            entry.put("collection", collection.name());
            entry.put("from", furthest.url());
            entry.put("before", mine.sequence());
            entry.put("after", current == null ? null : current.index().lastPosition().sequence());
            report.add(entry);
        }
        return report;
    }

    /**
     * For a follower, refuses an answer from a node in an older term, which cannot be the current
     * leader, and adopts a newer term.
     */
    private void checkTerm(String base, long theirTerm) throws IOException {
        if (!asFollower) {
            return;
        }
        if (theirTerm < node.term()) {
            throw new IOException(base + " is in term " + theirTerm + ", behind this node's term " + node.term()
                    + ", so it is not the current leader");
        }
        node.raiseTerm(theirTerm);
    }

    private static long termHeader(HttpResponse<?> response) throws IOException {
        return Long.parseLong(response.headers()
                .firstValue(TERM_HEADER)
                .orElseThrow(() -> new IOException("the response has no " + TERM_HEADER + " header")));
    }

    private HttpRequest get(String base, String path, String query, Duration timeout) {
        String sender = asFollower ? "follower=" + node.id() : "peer=true";
        String all = sender + "&term=" + node.term() + (query.isEmpty() ? "" : "&" + query);
        return HttpRequest.newBuilder(URI.create(base + path + "?" + all)).timeout(timeout).GET().build();
    }

    private static Duration requestTimeout(long waitMillis) {
        return Duration.ofMillis(waitMillis + 10_000);
    }

    private <T> HttpResponse<T> send(HttpRequest request, BodyHandler<T> handler) throws IOException {
        if (cancelled) {
            throw new CancellationException();
        }
        CompletableFuture<HttpResponse<T>> response = http.sendAsync(request, handler);
        inFlight.add(response);
        try {
            if (cancelled) {
                response.cancel(true);
            }
            return response.get();
        } catch (ExecutionException e) {
            if (e.getCause() instanceof IOException io) {
                throw io;
            }
            if (e.getCause() instanceof CancellationException cancelled) {
                throw cancelled;
            }
            throw new IOException(request.uri() + ": " + e.getCause(), e.getCause());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            response.cancel(true);
            throw new CancellationException("interrupted");
        } finally {
            inFlight.remove(response);
        }
    }

    private static Map<?, ?> jsonObject(String base, HttpResponse<String> response) throws IOException {
        if (response.statusCode() != 200) {
            throw failure(base, response.statusCode(), response.body());
        }
        try {
            return (Map<?, ?>) Json.parse(response.body());
        } catch (RuntimeException e) {
            throw new IOException(base + " sent something that is not a JSON object", e);
        }
    }

    private static IOException failure(String base, int status, String body) {
        String text = body.length() > 300 ? body.substring(0, 300) : body;
        return new IOException(base + " answered " + status + ": " + text);
    }
}
