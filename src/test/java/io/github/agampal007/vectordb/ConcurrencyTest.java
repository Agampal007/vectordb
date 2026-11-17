package io.github.agampal007.vectordb;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.agampal007.vectordb.wal.WriteAheadLog.SyncPolicy;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * Many threads on one index. The checks are exact, not statistical: results must equal what a
 * single thread gets, and the graph must equal the one a single thread builds.
 */
@Timeout(value = 120, unit = TimeUnit.SECONDS)
class ConcurrencyTest {

    private static final int DIM = 16;
    private static final int READERS = 6;
    private static final Supplier<HnswIndex> EMPTY = () -> new HnswIndex(DIM, Distance.L2, 8, 40, 40, 3L);

    @TempDir Path dir;

    @Test
    void concurrentSearchesGiveTheSameResultsAsOneThread() throws Exception {
        float[][] vectors = RandomVectors.generate(3_000, DIM, 11L);
        float[][] queries = RandomVectors.generate(200, DIM, 12L);
        HnswIndex index = EMPTY.get();
        for (int i = 0; i < vectors.length; i++) {
            index.add(i, vectors[i]);
        }
        List<List<SearchResult>> expected = new ArrayList<>();
        for (float[] query : queries) {
            expected.add(index.search(query, 10));
        }

        runAll(8, thread -> () -> {
            for (int round = 0; round < 20; round++) {
                for (int q = 0; q < queries.length; q++) {
                    assertEquals(expected.get(q), index.search(queries[q], 10), "query " + q);
                }
            }
            return null;
        });
    }

    /**
     * One writer adds vectors and removes every fifth one while readers search and another thread
     * takes snapshots. A removal that had returned before a search started must never show up in
     * that search, and at the end the graph must be the one a single thread builds from the same
     * operations, both in memory and after reopening from disk.
     */
    @Test
    void searchesStayCorrectWhileAWriterAddsRemovesAndSnapshots() throws Exception {
        int total = 3_000;
        float[][] vectors = RandomVectors.generate(total, DIM, 21L);
        float[][] queries = RandomVectors.generate(64, DIM, 22L);
        AtomicLong removedUpTo = new AtomicLong(-1); // every multiple of 5 up to this id is gone
        AtomicBoolean writing = new AtomicBoolean(true);
        AtomicLong searches = new AtomicLong();

        HnswIndex live;
        try (DurableIndex index = DurableIndex.open(dir, EMPTY, SyncPolicy.MANUAL)) {
            for (int i = 0; i < 200; i++) {
                index.add(i, vectors[i]);
            }
            runAll(READERS + 2, thread -> {
                if (thread == 0) {
                    return () -> {
                        try {
                            for (int id = 200; id < total; id++) {
                                index.add(id, vectors[id]);
                                long doomed = id - 200;
                                if (doomed % 5 == 0) {
                                    assertTrue(index.remove(doomed));
                                    removedUpTo.set(doomed);
                                }
                            }
                        } finally {
                            writing.set(false);
                        }
                        return null;
                    };
                }
                if (thread == 1) {
                    return () -> {
                        while (writing.get()) {
                            index.snapshot();
                            Thread.sleep(5);
                        }
                        return null;
                    };
                }
                return () -> {
                    int q = thread;
                    while (writing.get()) {
                        long removedBefore = removedUpTo.get();
                        List<SearchResult> results = index.search(queries[q++ % queries.length], 10);
                        assertValid(results, total, removedBefore);
                        searches.incrementAndGet();
                    }
                    return null;
                };
            });
            assertTrue(searches.get() > 0, "no search ran while the writer was running");

            HnswIndex oneThread = EMPTY.get();
            for (int i = 0; i < 200; i++) {
                oneThread.add(i, vectors[i]);
            }
            for (int id = 200; id < total; id++) {
                oneThread.add(id, vectors[id]);
                if ((id - 200) % 5 == 0) {
                    oneThread.remove(id - 200);
                }
            }
            live = index.index();
            TestGraphs.assertSameGraph(oneThread, live);
        }
        try (DurableIndex reopened = DurableIndex.open(dir, EMPTY, SyncPolicy.MANUAL)) {
            TestGraphs.assertSameGraph(live, reopened.index());
        }
    }

    /**
     * Several writers at once. Their operations interleave in some order; whatever it is, the log
     * must hold that same order, or replaying it would build a different graph.
     */
    @Test
    void concurrentWritersAreLoggedInTheOrderTheyAreApplied() throws Exception {
        int writers = 4;
        int perWriter = 400;
        float[][] vectors = RandomVectors.generate(writers * perWriter, DIM, 31L);

        HnswIndex live;
        try (DurableIndex index = DurableIndex.open(dir, EMPTY, SyncPolicy.MANUAL)) {
            runAll(writers, thread -> () -> {
                for (int i = 0; i < perWriter; i++) {
                    int id = thread * perWriter + i;
                    if (i % 2 == 0) {
                        index.add(id, vectors[id]);
                    } else {
                        assertFalse(index.upsert(id, vectors[id]));
                    }
                    if (i % 10 == 9) {
                        assertTrue(index.remove(id - 3));
                        assertTrue(index.upsert(id - 1, vectors[id - 2])); // replace a vector
                    }
                }
                return null;
            });
            assertEquals(writers * perWriter * 9 / 10, index.size());
            live = index.index();
        }
        try (DurableIndex reopened = DurableIndex.open(dir, EMPTY, SyncPolicy.MANUAL)) {
            assertFalse(reopened.recovery().snapshotLoaded());
            TestGraphs.assertSameGraph(live, reopened.index());
        }
    }

    private static void assertValid(List<SearchResult> results, int total, long removedBefore) {
        assertFalse(results.isEmpty());
        assertTrue(results.size() <= 10);
        Set<Long> seen = new HashSet<>();
        float previous = Float.NEGATIVE_INFINITY;
        for (SearchResult result : results) {
            assertTrue(result.id() >= 0 && result.id() < total, "unknown id " + result.id());
            assertTrue(seen.add(result.id()), "id " + result.id() + " returned twice");
            assertTrue(result.distance() >= previous, "results are not sorted by distance");
            previous = result.distance();
            assertFalse(
                    result.id() % 5 == 0 && result.id() <= removedBefore,
                    "id " + result.id() + " was removed before the search started");
        }
    }

    private interface Task {
        Callable<Void> forThread(int thread);
    }

    /** Runs one task per thread and rethrows the first failure, assertion errors included. */
    private static void runAll(int threads, Task task) throws Exception {
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        try {
            List<Future<Void>> futures = new ArrayList<>();
            for (int thread = 0; thread < threads; thread++) {
                futures.add(pool.submit(task.forThread(thread)));
            }
            for (Future<Void> future : futures) {
                try {
                    future.get();
                } catch (ExecutionException e) {
                    if (e.getCause() instanceof Error error) {
                        throw error;
                    }
                    if (e.getCause() instanceof Exception exception) {
                        throw exception;
                    }
                    throw e;
                }
            }
        } finally {
            pool.shutdownNow();
        }
    }
}
