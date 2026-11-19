package io.github.agampal007.vectordb.server;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.locks.ReentrantLock;

/**
 * What this node knows about its place in a cluster, kept in {@code node.properties} in the data
 * directory so that it survives restarts: its id, the highest term it has taken part in, whether
 * it leads or follows and whom, and the version of the collection list it holds.
 *
 * <p>The role given on the command line is used only for a new data directory. After that the
 * role changes only through promotion and {@code follow}, so a node that was promoted is still the
 * leader after a restart, whatever its command line says.
 *
 * <p>The collection list's version is a pair: the term of the leader that made the latest change,
 * and a count of changes. Pairs compare by term first, so a list from a newer leader always counts
 * as newer.
 */
final class NodeState {

    enum Role {
        LEADER,
        FOLLOWER
    }

    static final String FILE = "node.properties";

    private final Path file;
    private final String id;
    /** Taken for every change, which is saved before the lock is released. */
    private final ReentrantLock lock = new ReentrantLock();
    private volatile long term;
    private volatile Role role;
    private volatile String leader;
    private volatile long catalogTerm;
    private volatile long catalogVersion;

    private NodeState(Path file, String id) {
        this.file = file;
        this.id = id;
    }

    /**
     * Reads the node's state, or creates it for a new data directory.
     *
     * @param initialLeader for a new data directory, the leader to follow, or null to lead
     */
    static NodeState load(Path root, String initialLeader) throws IOException {
        Path file = root.resolve(FILE);
        if (!Files.exists(file)) {
            NodeState state = new NodeState(file, UUID.randomUUID().toString());
            state.role = initialLeader == null ? Role.LEADER : Role.FOLLOWER;
            state.leader = initialLeader;
            state.save();
            return state;
        }
        Properties properties = PropertiesFile.read(file);
        try {
            NodeState state = new NodeState(file, properties.getProperty("id").trim());
            state.term = Long.parseLong(properties.getProperty("term"));
            state.role = Role.valueOf(properties.getProperty("role"));
            state.leader = properties.getProperty("leader");
            state.catalogTerm = Long.parseLong(properties.getProperty("catalogTerm"));
            state.catalogVersion = Long.parseLong(properties.getProperty("catalogVersion"));
            if (state.id.isEmpty() || (state.role == Role.FOLLOWER) != (state.leader != null)) {
                throw new IllegalArgumentException("a follower names its leader, and only a follower does");
            }
            return state;
        } catch (RuntimeException e) {
            throw new IOException(file + " is not a valid node state file", e);
        }
    }

    String id() {
        return id;
    }

    long term() {
        return term;
    }

    Role role() {
        return role;
    }

    /** The leader this node follows, or null if it leads. */
    String leader() {
        return leader;
    }

    long catalogTerm() {
        return catalogTerm;
    }

    long catalogVersion() {
        return catalogVersion;
    }

    /** Whether the collection list at ({@code term}, {@code version}) is newer than this node's. */
    boolean catalogIsBehind(long term, long version) {
        return term != catalogTerm ? term > catalogTerm : version > catalogVersion;
    }

    /** Raises this node's term to {@code newTerm} if that is higher. Returns whether it did. */
    boolean raiseTerm(long newTerm) throws IOException {
        lock.lock();
        try {
            if (newTerm <= term) {
                return false;
            }
            term = newTerm;
            save();
            return true;
        } finally {
            lock.unlock();
        }
    }

    void becomeLeader() throws IOException {
        lock.lock();
        try {
            role = Role.LEADER;
            leader = null;
            save();
        } finally {
            lock.unlock();
        }
    }

    void becomeFollower(String newLeader) throws IOException {
        lock.lock();
        try {
            role = Role.FOLLOWER;
            leader = newLeader;
            save();
        } finally {
            lock.unlock();
        }
    }

    /** Records that this node now holds the collection list at the given version. */
    void setCatalog(long term, long version) throws IOException {
        lock.lock();
        try {
            if (term == catalogTerm && version == catalogVersion) {
                return;
            }
            catalogTerm = term;
            catalogVersion = version;
            save();
        } finally {
            lock.unlock();
        }
    }

    /** Records one more change to the collection list, made here as leader in this node's term. */
    void advanceCatalog() throws IOException {
        lock.lock();
        try {
            catalogTerm = term;
            catalogVersion++;
            save();
        } finally {
            lock.unlock();
        }
    }

    private void save() throws IOException {
        Properties properties = new Properties();
        properties.setProperty("id", id);
        properties.setProperty("term", Long.toString(term));
        properties.setProperty("role", role.name());
        if (leader != null) {
            properties.setProperty("leader", leader);
        }
        properties.setProperty("catalogTerm", Long.toString(catalogTerm));
        properties.setProperty("catalogVersion", Long.toString(catalogVersion));
        PropertiesFile.write(file, properties, "vectordb node");
    }
}
