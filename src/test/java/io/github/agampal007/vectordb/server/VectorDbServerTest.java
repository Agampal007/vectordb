package io.github.agampal007.vectordb.server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.agampal007.vectordb.Distance;
import io.github.agampal007.vectordb.HnswIndex;
import io.github.agampal007.vectordb.IndexLockedException;
import io.github.agampal007.vectordb.SearchResult;
import io.github.agampal007.vectordb.server.VectorDbServer.Options;
import io.github.agampal007.vectordb.server.VectorDbServer.SyncMode;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpRequest.BodyPublishers;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/** Drives a real server over a real socket with the JDK's HTTP client. */
@Timeout(value = 120, unit = TimeUnit.SECONDS)
class VectorDbServerTest {

    private record Reply(int status, Object body) {

        @SuppressWarnings("unchecked")
        Map<String, Object> object() {
            return (Map<String, Object>) body;
        }

        Object get(String field) {
            return object().get(field);
        }
    }

    private static final int DIM = 8;

    @TempDir Path data;

    private final HttpClient client = HttpClient.newHttpClient();
    private VectorDbServer server;

    private void start(Options options) throws IOException {
        server = VectorDbServer.start(data, "127.0.0.1", 0, options);
    }

    private void start() throws IOException {
        start(Options.defaults());
    }

    @AfterEach
    void stop() throws IOException {
        if (server != null) {
            server.close();
        }
        client.close();
    }

    private Reply send(String method, String path, String body) throws IOException, InterruptedException {
        HttpRequest request = HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + server.port() + path))
                .method(method, body == null ? BodyPublishers.noBody() : BodyPublishers.ofString(body))
                .build();
        HttpResponse<String> response = client.send(request, BodyHandlers.ofString());
        assertEquals("application/json", response.headers().firstValue("Content-Type").orElse(null));
        return new Reply(response.statusCode(), Json.parse(response.body()));
    }

    private static float[][] vectors(int count, long seed) {
        Random random = new Random(seed);
        float[][] vectors = new float[count][DIM];
        for (float[] vector : vectors) {
            for (int i = 0; i < DIM; i++) {
                vector[i] = random.nextFloat() * 2f - 1f;
            }
        }
        return vectors;
    }

    private static String upsertBody(float[][] vectors, int from, int to) {
        List<Object> items = new ArrayList<>();
        for (int id = from; id < to; id++) {
            Map<String, Object> item = new LinkedHashMap<>();
            item.put("id", (long) id);
            item.put("vector", vectors[id]);
            items.add(item);
        }
        return Json.write(Map.of("vectors", items));
    }

    private static String searchBody(float[] query, int k) {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("vector", query);
        body.put("k", (long) k);
        return Json.write(body);
    }

    private void createCollection(String name) throws IOException, InterruptedException {
        Reply reply = send("PUT", "/collections/" + name, "{\"dimension\":" + DIM + ",\"m\":8,\"efConstruction\":40}");
        assertEquals(201, reply.status());
    }

    @Test
    void collectionsAreCreatedDescribedListedAndDropped() throws Exception {
        start();
        assertEquals(new Reply(200, Map.of("status", "ok")), send("GET", "/health", null));
        assertEquals(new Reply(200, Map.of("collections", List.of())), send("GET", "/collections", null));

        Reply created = send("PUT", "/collections/docs", "{\"dimension\":3,\"distance\":\"cosine\"}");
        assertEquals(201, created.status());
        Map<String, Object> description = new LinkedHashMap<>();
        description.put("name", "docs");
        description.put("dimension", 3L);
        description.put("distance", "cosine");
        description.put("m", 16L);
        description.put("efConstruction", 200L);
        description.put("efSearch", 50L);
        description.put("size", 0L);
        assertEquals(description, created.body());
        assertEquals(new Reply(200, description), send("GET", "/collections/docs", null));
        assertEquals(409, send("PUT", "/collections/docs", "{\"dimension\":3}").status());

        createCollection("another");
        Reply listed = send("GET", "/collections", null);
        assertEquals(2, ((List<?>) listed.get("collections")).size());
        assertEquals(description, ((List<?>) listed.get("collections")).get(1)); // sorted by name

        assertTrue(Files.isDirectory(data.resolve("docs")));
        assertEquals(new Reply(200, Map.of("dropped", true)), send("DELETE", "/collections/docs", null));
        assertFalse(Files.exists(data.resolve("docs")));
        assertEquals(404, send("GET", "/collections/docs", null).status());
        assertEquals(404, send("DELETE", "/collections/docs", null).status());
        // The name can be used again, with different settings.
        assertEquals(201, send("PUT", "/collections/docs", "{\"dimension\":5}").status());
    }

    @Test
    void searchOverHttpReturnsWhatTheIndexReturns() throws Exception {
        start();
        createCollection("c");
        float[][] vectors = vectors(500, 1L);
        HnswIndex direct = new HnswIndex(DIM, Distance.L2, 8, 40, 50, 42L);
        for (int from = 0; from < vectors.length; from += 100) {
            Reply reply = send("POST", "/collections/c/vectors", upsertBody(vectors, from, from + 100));
            assertEquals(new Reply(200, Map.of("upserted", 100L, "replaced", 0L)), reply);
        }
        for (int id = 0; id < vectors.length; id++) {
            direct.add(id, vectors[id]);
        }
        assertEquals(500L, send("GET", "/collections/c", null).get("size"));

        for (float[] query : vectors(25, 2L)) {
            List<SearchResult> expected = direct.search(query, 5);
            List<?> results = (List<?>) send("POST", "/collections/c/search", searchBody(query, 5)).get("results");
            assertEquals(expected.size(), results.size());
            for (int i = 0; i < expected.size(); i++) {
                Map<?, ?> result = (Map<?, ?>) results.get(i);
                assertEquals(expected.get(i).id(), result.get("id"));
                assertEquals(expected.get(i).distance(), ((Number) result.get("distance")).floatValue());
            }
        }
        // efSearch can be set per query.
        Reply wide = send("POST", "/collections/c/search",
                "{\"vector\":" + Json.write(vectors[3]) + ",\"k\":1,\"efSearch\":200}");
        assertEquals(List.of(Map.of("id", 3L, "distance", 0.0)), wide.get("results"));
    }

    @Test
    void vectorsAreReadReplacedAndDeleted() throws Exception {
        start();
        createCollection("c");
        float[][] vectors = vectors(50, 3L);
        send("POST", "/collections/c/vectors", upsertBody(vectors, 0, 40));

        Reply fetched = send("GET", "/collections/c/vectors/7", null);
        assertEquals(200, fetched.status());
        assertEquals(7L, fetched.get("id"));
        List<?> numbers = (List<?>) fetched.get("vector");
        for (int i = 0; i < DIM; i++) {
            assertEquals(vectors[7][i], ((Number) numbers.get(i)).floatValue());
        }

        // Replace id 7 with another vector: searching for the new one finds it, at distance zero.
        String replace = "{\"vectors\":[{\"id\":7,\"vector\":" + Json.write(vectors[45]) + "}]}";
        assertEquals(new Reply(200, Map.of("upserted", 1L, "replaced", 1L)),
                send("POST", "/collections/c/vectors", replace));
        assertEquals(List.of(Map.of("id", 7L, "distance", 0.0)),
                send("POST", "/collections/c/search", searchBody(vectors[45], 1)).get("results"));
        assertEquals(40L, send("GET", "/collections/c", null).get("size"));

        assertEquals(new Reply(200, Map.of("removed", true)), send("DELETE", "/collections/c/vectors/7", null));
        assertEquals(404, send("DELETE", "/collections/c/vectors/7", null).status());
        assertEquals(404, send("GET", "/collections/c/vectors/7", null).status());
        List<?> afterDelete =
                (List<?>) send("POST", "/collections/c/search", searchBody(vectors[45], 40)).get("results");
        assertEquals(39, afterDelete.size());
        assertTrue(afterDelete.stream().noneMatch(result -> ((Map<?, ?>) result).get("id").equals(7L)));
    }

    @Test
    void idsUseAll64Bits() throws Exception {
        start();
        createCollection("c");
        long big = Long.MAX_VALUE - 1; // not representable as a double
        float[] vector = vectors(1, 4L)[0];
        String body = "{\"vectors\":[{\"id\":" + big + ",\"vector\":" + Json.write(vector)
                + "},{\"id\":-5,\"vector\":" + Json.write(vector) + "}]}";
        assertEquals(200, send("POST", "/collections/c/vectors", body).status());
        assertEquals(big, send("GET", "/collections/c/vectors/" + big, null).get("id"));
        assertEquals(-5L, send("GET", "/collections/c/vectors/-5", null).get("id"));
        List<?> results = (List<?>) send("POST", "/collections/c/search", searchBody(vector, 2)).get("results");
        assertEquals(List.of(Map.of("id", -5L, "distance", 0.0), Map.of("id", big, "distance", 0.0)), results);
    }

    @Test
    void badRequestsAreRefusedWithoutChangingAnything() throws Exception {
        start();
        createCollection("c");
        float[][] vectors = vectors(4, 5L);
        String good = Json.write(vectors[0]);

        String[][] cases = {
            // method, path, body, expected status
            {"POST", "/collections/c/vectors", "{not json", "400"},
            {"POST", "/collections/c/vectors", "[1,2,3]", "400"},
            {"POST", "/collections/c/vectors", "{}", "400"},
            {"POST", "/collections/c/vectors", "{\"vectors\":[]}", "400"},
            {"POST", "/collections/c/vectors", "{\"vectors\":[{\"id\":1,\"vector\":[1,2,3]}]}", "400"},
            {"POST", "/collections/c/vectors", "{\"vectors\":[{\"id\":1.5,\"vector\":" + good + "}]}", "400"},
            {"POST", "/collections/c/vectors", "{\"vectors\":[{\"id\":\"1\",\"vector\":" + good + "}]}", "400"},
            {"POST", "/collections/c/vectors", "{\"vectors\":[{\"id\":1,\"vector\":[1,2,3,4,5,6,7,\"x\"]}]}", "400"},
            {"POST", "/collections/c/vectors", "{\"vectors\":[{\"id\":1,\"vector\":[1,2,3,4,5,6,7,1e60]}]}", "400"},
            // One bad entry rejects the whole request, including the good entry before it.
            {"POST", "/collections/c/vectors",
                "{\"vectors\":[{\"id\":1,\"vector\":" + good + "},{\"id\":2,\"vector\":[1]}]}", "400"},
            {"POST", "/collections/c/search", "{\"vector\":[1,2]}", "400"},
            {"POST", "/collections/c/search", "{\"vector\":" + good + ",\"k\":0}", "400"},
            {"POST", "/collections/c/search", "{\"vector\":" + good + ",\"k\":1000000}", "400"},
            {"POST", "/collections/c/search", "{\"vector\":" + good + ",\"efSearch\":0}", "400"},
            {"POST", "/collections/c/search", "[".repeat(100_000), "400"},
            {"GET", "/collections/c/vectors/abc", null, "400"},
            {"GET", "/collections/c/vectors/99999999999999999999", null, "400"},
            {"PUT", "/collections/new", "{}", "400"},
            {"PUT", "/collections/new", "{\"dimension\":0}", "400"},
            {"PUT", "/collections/new", "{\"dimension\":8,\"distance\":\"manhattan\"}", "400"},
            {"PUT", "/collections/new", "{\"dimension\":8,\"m\":1}", "400"},
            {"PUT", "/collections/a.b", "{\"dimension\":8}", "400"},
            {"PUT", "/collections/a%2Fb", "{\"dimension\":8}", "400"},
            {"PUT", "/collections/%2E%2E", "{\"dimension\":8}", "400"},
            {"PUT", "/collections/" + "x".repeat(65), "{\"dimension\":8}", "400"},
            {"POST", "/collections/missing/search", "{\"vector\":" + good + "}", "404"},
            {"GET", "/collections/missing", null, "404"},
            {"GET", "/nothing", null, "404"},
            {"GET", "/collections/c/unknown", null, "405"},
            {"POST", "/collections/c/unknown", "{}", "404"},
            {"GET", "/collections/c/vectors/1/extra", null, "404"},
            {"POST", "/health", "{}", "405"},
            {"POST", "/collections", "{}", "405"},
            {"GET", "/collections/c/search", null, "405"},
            {"PATCH", "/collections/c", "{}", "405"},
            {"PUT", "/collections/c/vectors/1", "{}", "405"},
        };
        for (String[] c : cases) {
            Reply reply = send(c[0], c[1], c[2]);
            String label = c[0] + " " + c[1] + " " + (c[2] != null && c[2].length() > 60 ? c[2].substring(0, 60) : c[2]);
            assertEquals(Integer.parseInt(c[3]), reply.status(), label);
            assertTrue(reply.get("error") instanceof String, label);
        }
        assertEquals(0L, send("GET", "/collections/c", null).get("size"));
        assertEquals(1, ((List<?>) send("GET", "/collections", null).get("collections")).size());
        try (var children = Files.list(data)) {
            assertEquals(List.of("LOCK", "c"), children.map(path -> path.getFileName().toString()).sorted().toList());
        }
    }

    @Test
    void anOversizedBodyIsRefused() throws Exception {
        start(new Options(SyncMode.REQUEST, 64L << 20, 1024));
        createCollection("c");
        String body = "{\"vectors\":[" + " ".repeat(4096) + "]}";
        assertEquals(413, send("POST", "/collections/c/vectors", body).status());
    }

    @Test
    void dataSurvivesARestartAndTheDirectoryCannotBeSharedMeanwhile() throws Exception {
        float[][] vectors = vectors(300, 6L);
        start(new Options(SyncMode.REQUEST, 16 << 10, 32 << 20)); // snapshot after every 16 KB of log
        createCollection("kept");
        createCollection("emptied");
        for (int from = 0; from < vectors.length; from += 50) {
            send("POST", "/collections/kept/vectors", upsertBody(vectors, from, from + 50));
        }
        send("DELETE", "/collections/kept/vectors/11", null);
        assertTrue(Files.exists(data.resolve("kept").resolve("snapshot.bin")), "the log limit took no snapshot");
        Object before = send("POST", "/collections/kept/search", searchBody(vectors[11], 10)).body();

        assertThrows(IndexLockedException.class,
                () -> VectorDbServer.start(data, "127.0.0.1", 0, Options.defaults()));
        assertEquals(299L, send("GET", "/collections/kept", null).get("size"));

        server.close();
        start();
        assertEquals(299L, send("GET", "/collections/kept", null).get("size"));
        assertEquals(0L, send("GET", "/collections/emptied", null).get("size"));
        assertEquals(before, send("POST", "/collections/kept/search", searchBody(vectors[11], 10)).body());
        assertEquals(404, send("GET", "/collections/kept/vectors/11", null).status());
    }

    @Test
    void manyClientsAtOnce() throws Exception {
        start(new Options(SyncMode.NONE, 64L << 20, 32 << 20));
        createCollection("c");
        int writers = 4;
        int perWriter = 150;
        float[][] vectors = vectors(writers * perWriter, 7L);
        send("POST", "/collections/c/vectors", upsertBody(vectors, 0, 10));

        ExecutorService pool = Executors.newFixedThreadPool(writers * 2);
        try {
            List<Future<Integer>> futures = new ArrayList<>();
            for (int w = 0; w < writers; w++) {
                int writer = w;
                futures.add(pool.submit(() -> {
                    for (int i = writer == 0 ? 10 : 0; i < perWriter; i += 5) {
                        int from = writer * perWriter + i;
                        assertEquals(200, send("POST", "/collections/c/vectors", upsertBody(vectors, from, from + 5))
                                .status());
                    }
                    return 0;
                }));
                Callable<Integer> reader = () -> {
                    int searches = 0;
                    for (int q = 0; q < 150; q++) {
                        Reply reply = send("POST", "/collections/c/search", searchBody(vectors[q], 3));
                        assertEquals(200, reply.status());
                        assertFalse(((List<?>) reply.get("results")).isEmpty());
                        searches++;
                    }
                    return searches;
                };
                futures.add(pool.submit(reader));
            }
            for (Future<Integer> future : futures) {
                future.get();
            }
        } finally {
            pool.shutdownNow();
        }
        assertEquals((long) writers * perWriter, send("GET", "/collections/c", null).get("size"));
        for (int id = 0; id < vectors.length; id += 37) {
            String wide = "{\"vector\":" + Json.write(vectors[id]) + ",\"k\":1,\"efSearch\":200}";
            assertEquals(List.of(Map.of("id", (long) id, "distance", 0.0)),
                    send("POST", "/collections/c/search", wide).get("results"));
        }
    }
}
