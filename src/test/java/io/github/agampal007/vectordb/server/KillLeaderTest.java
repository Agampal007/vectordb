package io.github.agampal007.vectordb.server;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.BufferedReader;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpRequest.BodyPublishers;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * The replication milestone's failure test, with real processes.
 *
 * <p>Three servers run in their own JVMs, each with {@code --min-replicas 1}. In each round, writers
 * send to the leader without pause. At a random moment one follower is killed without warning
 * ({@code SIGKILL} on Linux, {@code TerminateProcess} on Windows), so the writes that follow are
 * confirmed by the other follower alone. At a second random moment the leader is killed too.
 *
 * <p>The follower that was killed first is restarted and promoted, with the other as its peer. It
 * is the worse choice on purpose: it lacks every write answered after it died, and promotion has
 * to fetch them from the peer. Every write the dead leader answered must then be on the new
 * leader, with its exact vector. The dead leader comes back, still believing it leads, is pointed
 * at the new leader, and must end with the same snapshot bytes as the other two. Then the next
 * round kills the new leader.
 */
class KillLeaderTest {

    private static final int ROUNDS = 3;
    private static final int WRITERS = 3;
    private static final int DIM = 16;
    private static final Pattern LISTENING = Pattern.compile("vectordb listening on (http://127\\.0\\.0\\.1:\\d+),");

    @TempDir Path dir;

    private final HttpClient client = HttpClient.newBuilder()
            .version(HttpClient.Version.HTTP_1_1)
            .connectTimeout(Duration.ofSeconds(2))
            .build();
    private final List<Server> servers = new ArrayList<>();

    /** One node: a data directory, and a server process while it is running. */
    private final class Server {
        final String name;
        final Path data;
        Process process;
        String url;
        final StringBuffer output = new StringBuffer();

        Server(String name) {
            this.name = name;
            this.data = dir.resolve(name);
            servers.add(this);
        }

        Server start(String... extra) throws Exception {
            List<String> command = new ArrayList<>(List.of(
                    Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                    "-cp", System.getProperty("vectordb.test.classpath", System.getProperty("java.class.path")),
                    Main.class.getName(),
                    "--data", data.toString(),
                    "--port", "0",
                    "--min-replicas", "1",
                    "--replication-timeout-ms", "5000"));
            command.addAll(List.of(extra));
            process = new ProcessBuilder(command).redirectErrorStream(true).start();
            CompletableFuture<String> listening = new CompletableFuture<>();
            Thread.ofVirtual().start(() -> {
                try (BufferedReader lines = process.inputReader()) {
                    String line;
                    while ((line = lines.readLine()) != null) {
                        output.append(name).append(": ").append(line).append('\n');
                        Matcher matcher = LISTENING.matcher(line);
                        if (matcher.find()) {
                            listening.complete(matcher.group(1));
                        }
                    }
                } catch (IOException e) {
                    // The process was killed.
                }
                listening.completeExceptionally(new IllegalStateException(name + " exited before listening"));
            });
            url = listening.get(30, TimeUnit.SECONDS);
            return this;
        }

        void kill() throws InterruptedException {
            process.destroyForcibly();
            assertTrue(process.waitFor(30, TimeUnit.SECONDS));
        }
    }

    @AfterEach
    void stopAll() throws InterruptedException {
        for (Server server : servers) {
            if (server.process != null && server.process.isAlive()) {
                server.kill();
            }
            System.out.print(server.output); // kept in the test report
        }
        client.close();
    }

    @Test
    @Timeout(value = 300, unit = TimeUnit.SECONDS)
    void everyAnsweredWriteSurvivesKillingTheLeader() throws Exception {
        Random random = new Random(2026);
        Server a = new Server("a").start();
        Server b = new Server("b").start("--follow", a.url);
        Server c = new Server("c").start("--follow", a.url);
        assertEquals(201, send("PUT", a.url + "/collections/docs", "{\"dimension\":" + DIM + "}").status);

        Set<Long> answered = ConcurrentHashMap.newKeySet();
        Set<Long> attempted = ConcurrentHashMap.newKeySet();
        Server leader = a;
        List<Server> followers = new ArrayList<>(List.of(b, c));
        for (int round = 0; round < ROUNDS; round++) {
            // Write without pause, then kill the leader at a random moment.
            List<Thread> writers = new ArrayList<>();
            for (int w = 0; w < WRITERS; w++) {
                long firstId = (round * WRITERS + w) * 1_000_000L;
                String target = leader.url;
                writers.add(Thread.ofVirtual().start(() -> write(target, firstId, answered, attempted)));
            }
            Server promoted = followers.get(random.nextInt(2));
            Server other = followers.get(0) == promoted ? followers.get(1) : followers.get(0);
            Thread.sleep(200 + random.nextInt(400));
            promoted.kill(); // from here on, only the other follower confirms writes
            int answeredBefore = answered.size();
            Thread.sleep(200 + random.nextInt(400));
            leader.kill();
            for (Thread writer : writers) {
                writer.join();
            }
            assertTrue(answered.size() > answeredBefore, "no write was answered after the first follower died");

            // Promote the follower that is behind, with the other as its peer.
            promoted.start(); // its data directory says it follows the dead leader
            Reply promotion = send("POST", promoted.url + "/admin/promote",
                    "{\"peers\":[\"" + other.url + "\",\"" + leader.url + "\"]}");
            assertEquals(200, promotion.status, promotion.body);

            // Nothing that was answered may be missing.
            int extra = 0;
            for (long id : attempted) {
                Reply reply = send("GET", promoted.url + "/collections/docs/vectors/" + id, null);
                if (answered.contains(id)) {
                    assertEquals(200, reply.status, "round " + round + ": answered write " + id + " is missing");
                    assertEquals(Json.write(vectorFor(id)), Json.write(vectorOf(reply)), "round " + round + ", id " + id);
                } else if (reply.status == 200) {
                    extra++; // reached a follower but the answer never got back: allowed
                }
            }
            // And they came from the peer: the promoted node did not have them.
            List<?> caughtUp = (List<?>) ((Map<?, ?>) Json.parse(promotion.body)).get("caughtUp");
            assertEquals(1, caughtUp.size(), promotion.body);
            Map<?, ?> fromPeer = (Map<?, ?>) caughtUp.get(0);
            assertEquals(other.url, fromPeer.get("from"), promotion.body);
            assertTrue((Long) fromPeer.get("after") > (Long) fromPeer.get("before"), promotion.body);
            System.out.printf("round %d: %s and then %s killed, %s promoted with %s as peer; %d writes answered"
                            + " so far, %d of them after %s died; %d unanswered writes present anyway; promotion: %s%n",
                    round, promoted.name, leader.name, promoted.name, other.name, answered.size(),
                    answered.size() - answeredBefore, promoted.name, extra, promotion.body);

            // The rest of the cluster follows the new leader, including the node that was killed.
            assertEquals(200, send("POST", other.url + "/admin/follow", "{\"leader\":\"" + promoted.url + "\"}").status);
            Server killed = leader.start();
            assertEquals(200, send("POST", killed.url + "/admin/follow", "{\"leader\":\"" + promoted.url + "\"}").status);
            long marker = 900_000_000L + round;
            assertEquals(200, send("POST", promoted.url + "/collections/docs/vectors",
                    "{\"vectors\":[{\"id\":" + marker + ",\"vector\":" + Json.write(vectorFor(marker)) + "}]}").status);
            answered.add(marker);
            attempted.add(marker);
            awaitSamePosition(promoted, List.of(other, killed));
            byte[] expected = snapshot(promoted);
            assertArrayEquals(expected, snapshot(other), "round " + round + ": " + other.name);
            assertArrayEquals(expected, snapshot(killed), "round " + round + ": " + killed.name);

            leader = promoted;
            followers = List.of(other, killed);
        }
        assertTrue(answered.size() > ROUNDS * WRITERS * 5, "too few writes were answered: " + answered.size());
    }

    /** Upserts batches of one to four new vectors until a request fails, recording what was answered. */
    private void write(String url, long firstId, Set<Long> answered, Set<Long> attempted) {
        Random random = new Random(firstId);
        long next = firstId;
        while (true) {
            int count = 1 + random.nextInt(4);
            List<Object> items = new ArrayList<>();
            List<Long> ids = new ArrayList<>();
            for (int i = 0; i < count; i++) {
                long id = next++;
                ids.add(id);
                Map<String, Object> item = new LinkedHashMap<>();
                item.put("id", id);
                item.put("vector", vectorFor(id));
                items.add(item);
            }
            attempted.addAll(ids);
            try {
                if (send("POST", url + "/collections/docs/vectors", Json.write(Map.of("vectors", items))).status != 200) {
                    return;
                }
            } catch (Exception e) {
                return; // the leader is gone
            }
            answered.addAll(ids);
        }
    }

    private void awaitSamePosition(Server leader, List<Server> followers) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
        while (true) {
            Object target = position(leader);
            boolean all = true;
            for (Server follower : followers) {
                all &= target.equals(position(follower));
            }
            if (all) {
                return;
            }
            if (System.nanoTime() > deadline) {
                throw new AssertionError("the followers did not catch up with " + leader.name);
            }
            Thread.sleep(20);
        }
    }

    /** The node's sequence number and term in the collection, or null while it does not have it. */
    private Object position(Server server) throws Exception {
        Reply reply = send("GET", server.url + "/replication/status", null);
        if (reply.status != 200) {
            return null;
        }
        for (Object item : (List<?>) ((Map<?, ?>) Json.parse(reply.body)).get("collections")) {
            Map<?, ?> collection = (Map<?, ?>) item;
            if ("docs".equals(collection.get("name"))) {
                return List.of(collection.get("sequence"), collection.get("term"));
            }
        }
        return null;
    }

    private byte[] snapshot(Server server) throws Exception {
        String incarnation = null;
        Reply status = send("GET", server.url + "/replication/status", null);
        for (Object item : (List<?>) ((Map<?, ?>) Json.parse(status.body)).get("collections")) {
            incarnation = (String) ((Map<?, ?>) item).get("incarnation");
        }
        HttpRequest request = HttpRequest.newBuilder(URI.create(server.url
                        + "/replication/collections/docs/snapshot?peer=true&incarnation=" + incarnation))
                .build();
        HttpResponse<byte[]> response = client.send(request, BodyHandlers.ofByteArray());
        assertEquals(200, response.statusCode());
        return response.body();
    }

    private record Reply(int status, String body) {}

    private Reply send(String method, String url, String body) throws IOException, InterruptedException {
        HttpRequest request = HttpRequest.newBuilder(URI.create(url))
                .timeout(Duration.ofSeconds(30))
                .method(method, body == null ? BodyPublishers.noBody() : BodyPublishers.ofString(body))
                .build();
        HttpResponse<String> response = client.send(request, BodyHandlers.ofString());
        return new Reply(response.statusCode(), response.body());
    }

    private static float[] vectorOf(Reply reply) {
        List<?> numbers = (List<?>) ((Map<?, ?>) Json.parse(reply.body)).get("vector");
        float[] vector = new float[numbers.size()];
        for (int i = 0; i < vector.length; i++) {
            vector[i] = ((Number) numbers.get(i)).floatValue();
        }
        return vector;
    }

    private static float[] vectorFor(long id) {
        Random random = new Random(id);
        float[] vector = new float[DIM];
        for (int i = 0; i < DIM; i++) {
            vector[i] = random.nextFloat() * 2f - 1f;
        }
        return vector;
    }
}
