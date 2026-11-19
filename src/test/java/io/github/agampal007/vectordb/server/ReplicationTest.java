package io.github.agampal007.vectordb.server;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.agampal007.vectordb.server.VectorDbServer.Options;
import io.github.agampal007.vectordb.server.VectorDbServer.SyncMode;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpRequest.BodyPublishers;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * Leaders and followers as real servers on real sockets, in this JVM. Stopping a node here is a
 * clean shutdown; {@link KillLeaderTest} kills processes instead.
 */
@Timeout(value = 120, unit = TimeUnit.SECONDS)
class ReplicationTest {

    private record Reply(int status, Object body) {

        Object get(String field) {
            return ((Map<?, ?>) body).get(field);
        }
    }

    private static final int DIM = 8;
    private static final float[][] VECTORS = vectors(2_000, 3L);

    @TempDir Path dir;

    private final HttpClient client = HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1).build();
    private final List<Node> nodes = new ArrayList<>();

    /** One node: a data directory, and a server while it is running. Each start gets a new port. */
    private final class Node {
        final Path data;
        VectorDbServer server;
        String url;

        Node(String name) {
            data = dir.resolve(name);
            nodes.add(this);
        }

        Node start(Options options) throws IOException {
            server = VectorDbServer.start(data, "127.0.0.1", 0, options);
            url = "http://127.0.0.1:" + server.port();
            return this;
        }

        void stop() throws IOException {
            if (server != null) {
                server.close();
                server = null;
            }
        }
    }

    @AfterEach
    void stopAll() throws IOException {
        for (Node node : nodes) {
            node.stop();
        }
        client.close();
    }

    private static Options leading(int minReplicas, long timeoutMillis) {
        return Options.defaults().withReplication(minReplicas, timeoutMillis, null);
    }

    private static Options following(Node leader) {
        return Options.defaults().withReplication(1, 2_000, leader.url);
    }

    private Reply send(String method, String url, String body) throws IOException, InterruptedException {
        HttpRequest request = HttpRequest.newBuilder(URI.create(url))
                .method(method, body == null ? BodyPublishers.noBody() : BodyPublishers.ofString(body))
                .build();
        HttpResponse<String> response = client.send(request, BodyHandlers.ofString());
        return new Reply(response.statusCode(), Json.parse(response.body()));
    }

    private Reply createCollection(Node node, String name, int dimension) throws Exception {
        return send("PUT", node.url + "/collections/" + name, "{\"dimension\":" + dimension + ",\"m\":8,\"efConstruction\":40}");
    }

    /** Upserts {@code VECTORS[from..to)} under the same ids, in one request. */
    private Reply upsert(Node node, String collection, int from, int to) throws Exception {
        List<Object> items = new ArrayList<>();
        for (int id = from; id < to; id++) {
            Map<String, Object> item = new LinkedHashMap<>();
            item.put("id", (long) id);
            item.put("vector", VECTORS[id]);
            items.add(item);
        }
        return send("POST", node.url + "/collections/" + collection + "/vectors", Json.write(Map.of("vectors", items)));
    }

    private Map<?, ?> status(Node node) throws Exception {
        Reply reply = send("GET", node.url + "/replication/status", null);
        assertEquals(200, reply.status());
        return (Map<?, ?>) reply.body();
    }

    /** The node's {sequence, term} in a collection, or null if it does not have it. */
    private long[] position(Node node, String collection) throws Exception {
        for (Object item : (List<?>) status(node).get("collections")) {
            Map<?, ?> entry = (Map<?, ?>) item;
            if (entry.get("name").equals(collection)) {
                return new long[] {(Long) entry.get("sequence"), (Long) entry.get("term")};
            }
        }
        return null;
    }

    private String incarnation(Node node, String collection) throws Exception {
        for (Object item : (List<?>) status(node).get("collections")) {
            Map<?, ?> entry = (Map<?, ?>) item;
            if (entry.get("name").equals(collection)) {
                return (String) entry.get("incarnation");
            }
        }
        throw new AssertionError(node.url + " has no collection " + collection);
    }

    private void awaitCaughtUp(Node follower, Node leader, String collection) throws Exception {
        long[] target = position(leader, collection);
        await(() -> {
            try {
                long[] position = position(follower, collection);
                return position != null && position[0] == target[0] && position[1] == target[1];
            } catch (Exception | AssertionError e) {
                return false; // not there yet, or busy installing a snapshot
            }
        }, follower.url + " did not catch up with " + leader.url);
    }

    private static void await(BooleanSupplier condition, String message) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError(message);
            }
            Thread.sleep(20);
        }
    }

    /**
     * A snapshot of the collection as the node holds it now, byte for byte. Two nodes at the same
     * position with the same graph give identical bytes: the format has no timestamps and no
     * unordered maps.
     */
    private byte[] snapshotBytes(Node node, String collection) throws Exception {
        String url = node.url + "/replication/collections/" + collection + "/snapshot?peer=true&incarnation="
                + incarnation(node, collection);
        HttpResponse<byte[]> response = client.send(HttpRequest.newBuilder(URI.create(url)).build(), BodyHandlers.ofByteArray());
        assertEquals(200, response.statusCode());
        return response.body();
    }

    private void assertHolds(Node node, String collection, int from, int to) throws Exception {
        for (int id = from; id < to; id++) {
            Reply reply = send("GET", node.url + "/collections/" + collection + "/vectors/" + id, null);
            assertEquals(200, reply.status(), node.url + " is missing id " + id);
            List<?> numbers = (List<?>) reply.get("vector");
            for (int i = 0; i < DIM; i++) {
                assertEquals(VECTORS[id][i], ((Number) numbers.get(i)).floatValue(), "id " + id);
            }
        }
    }

    @Test
    void anAnsweredWriteIsAlreadyOnTheFollowerWithTheLeadersExactGraph() throws Exception {
        Node leader = new Node("leader").start(leading(1, 5_000));
        Node follower = new Node("follower").start(following(leader));

        assertEquals(201, createCollection(leader, "docs", DIM).status());
        for (int from = 0; from < 300; from += 50) {
            assertEquals(200, upsert(leader, "docs", from, from + 50).status());
        }
        assertEquals(200, send("DELETE", leader.url + "/collections/docs/vectors/7", null).status());
        assertEquals(200, upsert(leader, "docs", 1_000, 1_010).status());

        // No waiting: with one follower and minReplicas 1, every answered write is already there.
        assertArrayEquals(position(leader, "docs"), position(follower, "docs"));
        assertHolds(follower, "docs", 0, 7);
        assertHolds(follower, "docs", 8, 300);
        assertEquals(404, send("GET", follower.url + "/collections/docs/vectors/7", null).status());
        assertArrayEquals(snapshotBytes(leader, "docs"), snapshotBytes(follower, "docs"));
        String search = "{\"vector\":" + Json.write(VECTORS[42]) + ",\"k\":10}";
        assertEquals(send("POST", leader.url + "/collections/docs/search", search),
                send("POST", follower.url + "/collections/docs/search", search));

        Map<?, ?> leaderStatus = status(leader);
        assertEquals("leader", leaderStatus.get("role"));
        assertEquals(1, ((Map<?, ?>) leaderStatus.get("followers")).size());
        Map<?, ?> followerStatus = status(follower);
        assertEquals("follower", followerStatus.get("role"));
        assertEquals(leader.url, followerStatus.get("leader"));
    }

    @Test
    void aFollowerRefusesWritesAndNamesItsLeader() throws Exception {
        Node leader = new Node("leader").start(leading(1, 5_000));
        Node follower = new Node("follower").start(following(leader));
        assertEquals(201, createCollection(leader, "docs", DIM).status());

        List<Reply> refused = List.of(
                createCollection(follower, "other", DIM),
                upsert(follower, "docs", 0, 1),
                send("DELETE", follower.url + "/collections/docs/vectors/1", null),
                send("DELETE", follower.url + "/collections/docs", null));
        for (Reply reply : refused) {
            assertEquals(409, reply.status());
            assertTrue(((String) reply.get("error")).contains(leader.url), reply.toString());
        }
        assertEquals(200, send("GET", follower.url + "/collections/docs", null).status());
    }

    @Test
    void aLeaderWithoutEnoughFollowersAnswers503ButKeepsTheWrite() throws Exception {
        Node leader = new Node("leader").start(leading(1, 300));
        Reply created = createCollection(leader, "docs", DIM);
        assertEquals(503, created.status());
        assertTrue(((String) created.get("error")).contains("may or may not survive"));
        assertEquals(503, upsert(leader, "docs", 0, 10).status());
        assertEquals(10L, send("GET", leader.url + "/collections/docs", null).get("size"));

        // A follower that arrives later copies all of it, and writes are confirmed again.
        Node follower = new Node("follower").start(following(leader));
        awaitCaughtUp(follower, leader, "docs");
        assertEquals(200, upsert(leader, "docs", 10, 20).status());
        assertHolds(follower, "docs", 0, 20);
    }

    @Test
    void aFollowerThatFellBehindACompactedLogStartsAgainFromASnapshot() throws Exception {
        Options compactsOften = new Options(SyncMode.REQUEST, 8 << 10, 32 << 20);
        Node leader = new Node("leader").start(compactsOften);
        Node follower = new Node("follower").start(following(leader));
        assertEquals(201, createCollection(leader, "docs", DIM).status());
        upsert(leader, "docs", 0, 100);
        awaitCaughtUp(follower, leader, "docs");

        follower.stop();
        for (int from = 100; from < 900; from += 100) {
            assertEquals(200, upsert(leader, "docs", from, from + 100).status()); // the log is compacted several times
        }
        follower.start(following(leader)); // the data directory remembers it follows
        awaitCaughtUp(follower, leader, "docs");
        assertHolds(follower, "docs", 0, 900);
        assertArrayEquals(snapshotBytes(leader, "docs"), snapshotBytes(follower, "docs"));
    }

    @Test
    void droppingAndRecreatingACollectionReachesTheFollower() throws Exception {
        Node leader = new Node("leader").start(leading(1, 5_000));
        Node follower = new Node("follower").start(following(leader));
        assertEquals(201, createCollection(leader, "c", DIM).status());
        upsert(leader, "c", 0, 20);
        String first = incarnation(follower, "c");

        assertEquals(200, send("DELETE", leader.url + "/collections/c", null).status());
        assertEquals(404, send("GET", follower.url + "/collections/c", null).status(), "the drop was confirmed");

        assertEquals(201, createCollection(leader, "c", 4).status());
        assertEquals(4L, send("GET", follower.url + "/collections/c", null).get("dimension"));
        assertEquals(0L, send("GET", follower.url + "/collections/c", null).get("size"));
        assertFalse(first.equals(incarnation(follower, "c")));
        String body = "{\"vectors\":[{\"id\":5,\"vector\":[1,2,3,4]}]}";
        assertEquals(200, send("POST", leader.url + "/collections/c/vectors", body).status());
        assertEquals(1L, send("GET", follower.url + "/collections/c", null).get("size"));
    }

    /**
     * The whole failover: the leader stops after a write that only one follower confirmed, the
     * other follower is promoted, catches up from the first, and leads. The old leader comes back
     * thinking it still leads, is fenced off, and then follows, throwing away its own write.
     */
    @Test
    void promotionCatchesUpFromAPeerAndTheOldLeaderIsFencedOff() throws Exception {
        Node oldLeader = new Node("a").start(leading(1, 1_000));
        Node promoted = new Node("b").start(following(oldLeader));
        Node other = new Node("c").start(following(oldLeader));
        assertEquals(201, createCollection(oldLeader, "docs", DIM).status());
        assertEquals(200, upsert(oldLeader, "docs", 0, 100).status());
        awaitCaughtUp(promoted, oldLeader, "docs");
        awaitCaughtUp(other, oldLeader, "docs");

        // b falls behind; these writes are confirmed by c alone.
        promoted.stop();
        assertEquals(200, upsert(oldLeader, "docs", 100, 200).status());
        String oldLeaderUrl = oldLeader.url;
        oldLeader.stop();

        promoted.start(following(oldLeader)); // still tries to follow a's old address
        Reply promotion = send("POST", promoted.url + "/admin/promote",
                Json.write(Map.of("peers", List.of(other.url, oldLeaderUrl))));
        assertEquals(200, promotion.status(), promotion.toString());
        assertEquals(1L, promotion.get("term"));
        assertEquals(List.of(oldLeaderUrl), promotion.get("unreachable"));
        assertHolds(promoted, "docs", 0, 200);
        assertEquals(1L, status(other).get("term"), "the peer adopted the new term during promotion");

        assertEquals(200, send("POST", other.url + "/admin/follow", "{\"leader\":\"" + promoted.url + "\"}").status());
        assertEquals(200, upsert(promoted, "docs", 200, 300).status());
        assertHolds(other, "docs", 200, 300);

        // a comes back. Its data directory says it leads, in term 0, and it has no followers.
        oldLeader.start(leading(1, 1_000));
        assertEquals("leader", status(oldLeader).get("role"));
        assertEquals(503, upsert(oldLeader, "docs", 1_500, 1_501).status(), "nobody confirms it");

        // A follower that knows term 1 deposes it on first contact, and its writes stop.
        assertEquals(200, send("POST", other.url + "/admin/follow", "{\"leader\":\"" + oldLeader.url + "\"}").status());
        await(() -> {
            try {
                return Boolean.TRUE.equals(status(oldLeader).get("deposed"));
            } catch (Exception | AssertionError e) {
                return false;
            }
        }, "the old leader was not deposed");
        Reply fenced = upsert(oldLeader, "docs", 1_501, 1_502);
        assertEquals(409, fenced.status());
        assertTrue(((String) fenced.get("error")).contains("promoted"));
        assertEquals(200, send("POST", other.url + "/admin/follow", "{\"leader\":\"" + promoted.url + "\"}").status());

        // a follows b. The logs disagree at sequence 201: a's unconfirmed write there is in term 0,
        // b's write there is in term 1. So a starts again from b's snapshot.
        assertEquals(200, send("POST", oldLeader.url + "/admin/follow", "{\"leader\":\"" + promoted.url + "\"}").status());
        assertEquals(200, upsert(promoted, "docs", 300, 310).status());
        awaitCaughtUp(oldLeader, promoted, "docs");
        awaitCaughtUp(other, promoted, "docs");
        assertEquals(404, send("GET", oldLeader.url + "/collections/docs/vectors/1500", null).status());
        byte[] expected = snapshotBytes(promoted, "docs");
        assertArrayEquals(expected, snapshotBytes(other, "docs"));
        assertArrayEquals(expected, snapshotBytes(oldLeader, "docs"));

        // Roles survive a restart.
        promoted.stop();
        promoted.start(leading(1, 1_000));
        assertEquals("leader", status(promoted).get("role"));
        assertEquals(1L, status(promoted).get("term"));
    }

    @Test
    void misdirectedReplicationRequestsAreRefused() throws Exception {
        Node leader = new Node("leader").start(leading(0, 1_000));
        Node follower = new Node("follower").start(following(leader));
        assertEquals(201, createCollection(leader, "docs", DIM).status());
        String incarnation = incarnation(leader, "docs");

        assertEquals(409, send("POST", leader.url + "/admin/promote", "{}").status(), "already the leader");
        assertEquals(400, send("POST", follower.url + "/admin/follow", "{\"leader\":\"ftp://x\"}").status());
        assertEquals(400, send("POST", follower.url + "/admin/follow", "{\"leader\":\"http://x:1/path\"}").status());
        assertEquals(400, send("POST", follower.url + "/admin/promote", "{\"peers\":\"http://x:1\"}").status());
        String log = "/replication/collections/docs/log?after=0&afterTerm=0&incarnation=" + incarnation;
        assertEquals(400, send("GET", leader.url + log, null).status(), "neither follower nor peer");
        assertEquals(409, send("GET", follower.url + log + "&follower=someone", null).status(), "a follower is not a leader");
        assertEquals(404, send("GET", leader.url + log.replace(incarnation, "other") + "&peer=true", null).status());
        assertEquals(400, send("GET", leader.url + "/replication/collections/docs/log?peer=true&incarnation=" + incarnation, null).status());
        assertEquals(405, send("POST", leader.url + "/replication/status", "{}").status());
        assertEquals(404, send("GET", leader.url + "/replication/nothing", null).status());
        assertEquals(404, send("POST", leader.url + "/admin/nothing", "{}").status());
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
}
