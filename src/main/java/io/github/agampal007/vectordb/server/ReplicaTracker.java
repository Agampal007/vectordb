package io.github.agampal007.vectordb.server;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * The leader's record of how far each follower has got, and the waiting that holds a write's
 * answer until enough followers have it.
 *
 * <p>A follower reports a position by asking for what comes after it, and it only asks once
 * everything up to that position is on its own storage device. So a report is a durable
 * acknowledgement. Only the latest report from each follower counts: a follower that starts again
 * from an older snapshot moves back.
 *
 * <p>Waiting uses a {@link Condition}, not {@code Object.wait}, so a waiting virtual thread does
 * not hold on to its carrier thread.
 */
final class ReplicaTracker {

    private final ReentrantLock lock = new ReentrantLock();
    private final Condition changed = lock.newCondition();
    /** Collection incarnation, then follower id, to the last sequence number that follower holds. */
    private final Map<String, Map<String, Long>> logs = new HashMap<>();
    /** Follower id to the version of the collection list it holds: {term, version}. */
    private final Map<String, long[]> catalogs = new HashMap<>();
    private final Map<String, Long> lastSeen = new HashMap<>();

    void ackLog(String incarnation, String follower, long sequence) {
        lock.lock();
        try {
            logs.computeIfAbsent(incarnation, k -> new HashMap<>()).put(follower, sequence);
            lastSeen.put(follower, System.nanoTime());
            changed.signalAll();
        } finally {
            lock.unlock();
        }
    }

    void ackCatalog(String follower, long term, long version) {
        lock.lock();
        try {
            catalogs.put(follower, new long[] {term, version});
            lastSeen.put(follower, System.nanoTime());
            changed.signalAll();
        } finally {
            lock.unlock();
        }
    }

    /** Waits until {@code replicas} followers hold the collection's log up to {@code sequence}. */
    boolean awaitLog(String incarnation, long sequence, int replicas, long timeout, TimeUnit unit)
            throws InterruptedException {
        long nanos = unit.toNanos(timeout);
        lock.lock();
        try {
            while (countLog(incarnation, sequence) < replicas) {
                if (nanos <= 0) {
                    return false;
                }
                nanos = changed.awaitNanos(nanos);
            }
            return true;
        } finally {
            lock.unlock();
        }
    }

    /** Waits until {@code replicas} followers hold the collection list at ({@code term}, {@code version}) or later. */
    boolean awaitCatalog(long term, long version, int replicas, long timeout, TimeUnit unit)
            throws InterruptedException {
        long nanos = unit.toNanos(timeout);
        lock.lock();
        try {
            while (countCatalog(term, version) < replicas) {
                if (nanos <= 0) {
                    return false;
                }
                nanos = changed.awaitNanos(nanos);
            }
            return true;
        } finally {
            lock.unlock();
        }
    }

    /** Forgets every follower, as when this node starts or stops leading. */
    void clear() {
        lock.lock();
        try {
            logs.clear();
            catalogs.clear();
            lastSeen.clear();
            changed.signalAll();
        } finally {
            lock.unlock();
        }
    }

    /** Each follower: when it was last heard from, and the position it holds in each collection. */
    Map<String, Object> describe(Map<String, String> namesByIncarnation) {
        lock.lock();
        try {
            Map<String, Object> out = new LinkedHashMap<>();
            long now = System.nanoTime();
            for (Map.Entry<String, Long> seen : lastSeen.entrySet()) {
                String follower = seen.getKey();
                Map<String, Object> positions = new LinkedHashMap<>();
                for (Map.Entry<String, Map<String, Long>> log : logs.entrySet()) {
                    Long sequence = log.getValue().get(follower);
                    String name = namesByIncarnation.get(log.getKey());
                    if (sequence != null && name != null) {
                        positions.put(name, sequence);
                    }
                }
                Map<String, Object> entry = new LinkedHashMap<>();
                entry.put("lastSeenMillisAgo", TimeUnit.NANOSECONDS.toMillis(now - seen.getValue()));
                long[] catalog = catalogs.get(follower);
                if (catalog != null) {
                    entry.put("catalogTerm", catalog[0]);
                    entry.put("catalogVersion", catalog[1]);
                }
                entry.put("collections", positions);
                out.put(follower, entry);
            }
            return out;
        } finally {
            lock.unlock();
        }
    }

    private int countLog(String incarnation, long sequence) {
        Map<String, Long> followers = logs.get(incarnation);
        if (followers == null) {
            return 0;
        }
        int count = 0;
        for (long held : followers.values()) {
            if (held >= sequence) {
                count++;
            }
        }
        return count;
    }

    private int countCatalog(long term, long version) {
        int count = 0;
        for (long[] held : catalogs.values()) {
            if (held[0] > term || (held[0] == term && held[1] >= version)) {
                count++;
            }
        }
        return count;
    }
}
