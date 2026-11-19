package io.github.agampal007.vectordb.server;

import io.github.agampal007.vectordb.server.CollectionStore.Collection;
import java.net.http.HttpClient;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * Keeps this node's collections equal to a leader's.
 *
 * <p>One thread long-polls the leader's collection list and creates or drops collections to match.
 * One thread per collection long-polls that collection's log, applies what comes back and asks
 * again from its new position, which tells the leader how far it has got. All of them are virtual
 * threads, and all their waiting happens in the leader, so an idle follower costs a few open
 * requests.
 *
 * <p>Stopping never interrupts a thread: requests in flight are abandoned, and a thread that is
 * applying a batch finishes it first.
 */
final class Follower {

    /** How long the leader may hold a request open before answering that nothing has changed. */
    static final long WAIT_MILLIS = 2_000;
    private static final long JOIN_MILLIS = 30_000;

    private final CollectionStore store;
    private final String leader;
    private final ReplicaClient client;
    private volatile boolean running = true;
    /** The thread pulling each collection, by incarnation; a thread stops when it is no longer here. */
    private final Map<String, Thread> pullers = new ConcurrentHashMap<>();
    private Thread catalogThread;
    private volatile String lastError;
    private volatile long lastContactNanos;

    Follower(CollectionStore store, String leader, HttpClient http, long snapshotLogBytes) {
        this.store = store;
        this.leader = leader;
        this.client = new ReplicaClient(http, store, store.node(), snapshotLogBytes, true);
    }

    void start() {
        catalogThread = Thread.ofVirtual().name("follow-catalog").start(this::followCatalog);
    }

    /** Stops every thread and waits for them, each finishing the batch it is applying. */
    void stop() {
        running = false;
        client.cancel();
        join(catalogThread);
        for (Thread puller : pullers.values()) {
            join(puller);
        }
    }

    Map<String, Object> describe() {
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("leader", leader);
        out.put("lastContactMillisAgo", lastContactNanos == 0
                ? null
                : TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - lastContactNanos));
        out.put("lastError", lastError);
        return out;
    }

    private void followCatalog() {
        Backoff backoff = new Backoff();
        boolean first = true;
        while (running) {
            try {
                // The first request does not wait for a change, so the collections start pulling at once.
                client.reconcile(client.catalog(leader, first ? 0 : WAIT_MILLIS), this::stopPulling);
                first = false;
                for (Collection collection : store.list()) {
                    startPulling(collection);
                }
                contact();
                backoff.reset();
            } catch (CancellationException e) {
                return;
            } catch (Exception e) {
                problem(e);
                backoff.pause();
            }
        }
    }

    private void startPulling(Collection collection) {
        Thread existing = pullers.get(collection.incarnation());
        if (existing != null && existing.isAlive()) {
            return;
        }
        // Registered before it starts, because a puller stops as soon as it is not registered.
        Thread puller = Thread.ofVirtual()
                .name("follow-" + collection.name())
                .unstarted(() -> pull(collection.name(), collection.incarnation()));
        pullers.put(collection.incarnation(), puller);
        puller.start();
    }

    private void stopPulling(Collection collection) {
        Thread puller = pullers.remove(collection.incarnation());
        join(puller);
    }

    private void pull(String name, String incarnation) {
        Backoff backoff = new Backoff();
        while (running && pullers.get(incarnation) == Thread.currentThread()) {
            Collection collection = store.get(name);
            if (collection == null || !collection.incarnation().equals(incarnation)) {
                return;
            }
            try {
                if (client.pull(leader, collection, WAIT_MILLIS) == ReplicaClient.Pulled.GONE) {
                    backoff.pause(); // the leader dropped it; the catalog thread will drop it here
                } else {
                    contact();
                    backoff.reset();
                }
            } catch (CancellationException e) {
                return;
            } catch (Exception e) {
                problem(e);
                backoff.pause();
            }
        }
    }

    private void contact() {
        lastContactNanos = System.nanoTime();
        if (lastError != null) {
            System.out.println("replication from " + leader + " is working again");
            lastError = null;
        }
    }

    /** Records a failure, and prints it only when it differs from the last, so retries do not flood the log. */
    private void problem(Exception e) {
        String message = e.getMessage() == null ? e.toString() : e.getMessage();
        if (!message.equals(lastError)) {
            System.err.println("replication from " + leader + ": " + message);
        }
        lastError = message;
    }

    private static void join(Thread thread) {
        if (thread == null) {
            return;
        }
        try {
            thread.join(JOIN_MILLIS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /** Waits longer after each failure in a row, up to two seconds, and stops waiting when stopped. */
    private final class Backoff {
        private long millis = 50;

        void reset() {
            millis = 50;
        }

        void pause() {
            long until = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(millis);
            millis = Math.min(millis * 2, 2_000);
            while (running && System.nanoTime() < until) {
                try {
                    Thread.sleep(20);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        }
    }
}
