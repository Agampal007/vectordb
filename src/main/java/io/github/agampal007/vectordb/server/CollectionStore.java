package io.github.agampal007.vectordb.server;

import io.github.agampal007.vectordb.DirectoryLock;
import io.github.agampal007.vectordb.Distance;
import io.github.agampal007.vectordb.DurableIndex;
import io.github.agampal007.vectordb.HnswIndex;
import io.github.agampal007.vectordb.wal.WriteAheadLog.SyncPolicy;
import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/**
 * The named collections under one data directory. Each collection is a subdirectory holding a
 * small settings file and a {@link DurableIndex}. The store also owns the node's
 * {@link NodeState}, kept beside the collections.
 *
 * <p>A collection exists once its settings file does. The file is written last when creating and
 * deleted first when dropping, so a crash part-way through either leaves a directory without one,
 * which is ignored at start-up and cleared if the name is used again.
 *
 * <p>Each collection has an incarnation: a random id given when it is created and copied to every
 * replica. A collection dropped and created again under the same name is a different incarnation,
 * so a follower can tell that its copy belongs to the old one.
 */
public final class CollectionStore implements Closeable {

    /** The fixed parameters of a collection, chosen when it is created. */
    public record Settings(int dimension, Distance distance, int m, int efConstruction, int efSearch) {

        static final int MAX_DIMENSION = 65_536;
        static final int MAX_M = 512;
        static final int MAX_EF = 10_000;

        public Settings {
            if (dimension < 1 || dimension > MAX_DIMENSION) {
                throw new IllegalArgumentException("dimension must be between 1 and " + MAX_DIMENSION);
            }
            if (distance == null) {
                throw new IllegalArgumentException("distance is required");
            }
            if (m < 2 || m > MAX_M) {
                throw new IllegalArgumentException("m must be between 2 and " + MAX_M);
            }
            if (efConstruction < m || efConstruction > MAX_EF) {
                throw new IllegalArgumentException("efConstruction must be between m and " + MAX_EF);
            }
            if (efSearch < 1 || efSearch > MAX_EF) {
                throw new IllegalArgumentException("efSearch must be between 1 and " + MAX_EF);
            }
        }

        HnswIndex emptyIndex() {
            return new HnswIndex(dimension, distance, m, efConstruction, efSearch, SEED);
        }
    }

    /** One open collection. */
    public record Collection(String name, Settings settings, String incarnation, DurableIndex index) {}

    /** The collection list and the version of it, read together. */
    record Catalog(long term, long version, List<Collection> collections) {}

    /** Letters, digits, '-' and '_' only: a name is used as a directory name, so nothing else is allowed. */
    static final Pattern NAME = Pattern.compile("[A-Za-z0-9_-]{1,64}");

    static final String SETTINGS_FILE = "collection.properties";
    private static final long SEED = 42L;

    private final Path root;
    private final DirectoryLock rootLock;
    private final NodeState node;
    private final ConcurrentMap<String, Collection> collections = new ConcurrentHashMap<>();
    /** Serialises create, drop, replacing a collection's index and close. Lookups do not take it. */
    private final Object changes = new Object();
    private boolean closed;

    private CollectionStore(Path root, DirectoryLock rootLock, NodeState node) {
        this.root = root;
        this.rootLock = rootLock;
        this.node = node;
    }

    /** Opens a store whose node, if the directory is new, leads. */
    public static CollectionStore open(Path root) throws IOException {
        return open(root, null);
    }

    /**
     * Opens every collection found under {@code root}, replaying their logs.
     *
     * @param initialLeader for a new data directory, the leader to follow, or null to lead
     * @throws io.github.agampal007.vectordb.IndexLockedException if another process has the directory
     * @throws IOException if a collection cannot be read. Start-up stops instead of serving without it.
     */
    public static CollectionStore open(Path root, String initialLeader) throws IOException {
        DirectoryLock rootLock = DirectoryLock.acquire(root);
        CollectionStore store;
        try {
            store = new CollectionStore(root, rootLock, NodeState.load(root, initialLeader));
        } catch (IOException | RuntimeException e) {
            rootLock.close();
            throw e;
        }
        try {
            List<Path> directories;
            try (Stream<Path> children = Files.list(root)) {
                directories = children.filter(Files::isDirectory).sorted().toList();
            }
            for (Path directory : directories) {
                String name = directory.getFileName().toString();
                if (!NAME.matcher(name).matches() || !Files.exists(directory.resolve(SETTINGS_FILE))) {
                    System.err.println("ignoring " + directory + ": not a complete collection");
                    continue;
                }
                Properties properties = PropertiesFile.read(directory.resolve(SETTINGS_FILE));
                Settings settings = settings(directory.resolve(SETTINGS_FILE), properties);
                String incarnation = properties.getProperty("incarnation");
                if (incarnation == null) {
                    // Written before collections had incarnations.
                    incarnation = UUID.randomUUID().toString();
                    writeSettings(directory, settings, incarnation);
                }
                DurableIndex index = DurableIndex.open(directory, settings::emptyIndex, SyncPolicy.MANUAL);
                store.collections.put(name, new Collection(name, settings, incarnation, index));
                // The node's term is saved before any operation is logged in it, so this only
                // raises it if node.properties was lost or replaced.
                store.node.raiseTerm(index.lastPosition().term());
                DurableIndex.Recovery recovery = index.recovery();
                System.out.println("opened " + name + ": " + index.size() + " vectors, "
                        + (recovery.snapshotLoaded() ? "snapshot loaded, " : "no snapshot, ")
                        + recovery.replayedEntries() + " logged operations replayed");
            }
            for (Collection collection : store.collections.values()) {
                collection.index().setTerm(store.node.term());
            }
            return store;
        } catch (IOException | RuntimeException e) {
            store.close();
            throw e;
        }
    }

    NodeState node() {
        return node;
    }

    /** The collection called {@code name}, or null. */
    public Collection get(String name) {
        return collections.get(name);
    }

    /** Every collection, by name. */
    public List<Collection> list() {
        List<Collection> all = new ArrayList<>(collections.values());
        all.sort(Comparator.comparing(Collection::name));
        return all;
    }

    /** The collection list with its version, as one consistent reading. */
    Catalog catalog() {
        synchronized (changes) {
            return new Catalog(node.catalogTerm(), node.catalogVersion(), list());
        }
    }

    /**
     * Creates an empty collection, as a change made by this node as leader: the collection list
     * moves on a version.
     *
     * @return the new collection, or null if the name is taken
     * @throws IllegalArgumentException if the name is not allowed
     */
    public Collection create(String name, Settings settings) throws IOException {
        return create(name, settings, UUID.randomUUID().toString(), true);
    }

    /** Creates a follower's copy of a leader's collection. The collection list's version is not changed. */
    Collection createReplica(String name, Settings settings, String incarnation) throws IOException {
        return create(name, settings, incarnation, false);
    }

    private Collection create(String name, Settings settings, String incarnation, boolean leading)
            throws IOException {
        checkName(name);
        synchronized (changes) {
            ensureOpen();
            if (collections.containsKey(name)) {
                return null;
            }
            Path directory = root.resolve(name);
            deleteRecursively(directory); // what a crash during an earlier create or drop left behind
            DurableIndex index = DurableIndex.open(directory, settings::emptyIndex, SyncPolicy.MANUAL);
            try {
                writeSettings(directory, settings, incarnation);
                index.setTerm(node.term());
            } catch (IOException | RuntimeException e) {
                index.close();
                throw e;
            }
            Collection collection = new Collection(name, settings, incarnation, index);
            collections.put(name, collection);
            if (leading) {
                node.advanceCatalog();
            }
            return collection;
        }
    }

    /**
     * Closes the collection and deletes its files, as a change made by this node as leader.
     *
     * @return false if there is no such collection
     */
    public boolean drop(String name) throws IOException {
        return drop(name, true);
    }

    /** Drops a follower's copy of a collection the leader no longer has. */
    boolean dropReplica(String name) throws IOException {
        return drop(name, false);
    }

    private boolean drop(String name, boolean leading) throws IOException {
        checkName(name);
        synchronized (changes) {
            ensureOpen();
            Collection collection = collections.remove(name);
            if (collection == null) {
                return false;
            }
            collection.index().close(); // waits for requests still using it
            Path directory = root.resolve(name);
            Files.deleteIfExists(directory.resolve(SETTINGS_FILE));
            deleteRecursively(directory);
            if (leading) {
                node.advanceCatalog();
            }
            return true;
        }
    }

    /**
     * Replaces a collection's contents with a downloaded snapshot, which must be in the
     * collection's directory. Requests using the old index while it is closed fail with
     * {@link IllegalStateException}; later ones get the new one.
     *
     * @return the collection with its new index
     */
    Collection installSnapshot(String name, String incarnation, Path downloaded) throws IOException {
        synchronized (changes) {
            ensureOpen();
            Collection old = collections.get(name);
            if (old == null || !old.incarnation().equals(incarnation)) {
                throw new IllegalStateException("collection " + name + " was dropped or replaced");
            }
            old.index().close();
            Path directory = root.resolve(name);
            DurableIndex index;
            try {
                DurableIndex.installSnapshot(directory, downloaded);
            } finally {
                // Reopen whatever is there now, so the collection stays usable even if the
                // install failed.
                index = DurableIndex.open(directory, old.settings()::emptyIndex, SyncPolicy.MANUAL);
                index.setTerm(Math.max(node.term(), index.lastPosition().term()));
                collections.put(name, new Collection(name, old.settings(), incarnation, index));
            }
            return collections.get(name);
        }
    }

    /** Sets the term for operations written from now on, in every collection. */
    void setTerm(long term) {
        synchronized (changes) {
            for (Collection collection : collections.values()) {
                collection.index().setTerm(term);
            }
        }
    }

    /**
     * Snapshots every collection that has logged operations since its last snapshot, so the next
     * start does not have to replay them, then closes everything.
     */
    @Override
    public void close() throws IOException {
        synchronized (changes) {
            if (closed) {
                return;
            }
            closed = true;
            IOException failure = null;
            for (Collection collection : collections.values()) {
                try (DurableIndex index = collection.index()) {
                    if (index.hasLoggedOperations()) {
                        index.snapshot();
                    }
                } catch (IOException e) {
                    if (failure == null) {
                        failure = e;
                    } else {
                        failure.addSuppressed(e);
                    }
                }
            }
            collections.clear();
            rootLock.close();
            if (failure != null) {
                throw failure;
            }
        }
    }

    private void ensureOpen() {
        if (closed) {
            throw new IllegalStateException("the store is closed");
        }
    }

    private static void checkName(String name) {
        if (name == null || !NAME.matcher(name).matches()) {
            throw new IllegalArgumentException(
                    "a collection name is 1 to 64 letters, digits, '-' or '_'");
        }
    }

    private static Settings settings(Path file, Properties properties) throws IOException {
        try {
            return new Settings(
                    Integer.parseInt(properties.getProperty("dimension")),
                    Distance.valueOf(properties.getProperty("distance")),
                    Integer.parseInt(properties.getProperty("m")),
                    Integer.parseInt(properties.getProperty("efConstruction")),
                    Integer.parseInt(properties.getProperty("efSearch")));
        } catch (RuntimeException e) {
            throw new IOException(file + " is not a valid collection settings file", e);
        }
    }

    private static void writeSettings(Path directory, Settings settings, String incarnation) throws IOException {
        Properties properties = new Properties();
        properties.setProperty("dimension", Integer.toString(settings.dimension()));
        properties.setProperty("distance", settings.distance().name());
        properties.setProperty("m", Integer.toString(settings.m()));
        properties.setProperty("efConstruction", Integer.toString(settings.efConstruction()));
        properties.setProperty("efSearch", Integer.toString(settings.efSearch()));
        properties.setProperty("incarnation", incarnation);
        PropertiesFile.write(directory.resolve(SETTINGS_FILE), properties, "vectordb collection");
    }

    private static void deleteRecursively(Path directory) throws IOException {
        if (!Files.exists(directory)) {
            return;
        }
        List<Path> paths;
        try (Stream<Path> walk = Files.walk(directory)) {
            paths = walk.sorted(Comparator.reverseOrder()).toList();
        }
        for (Path path : paths) {
            Files.deleteIfExists(path);
        }
    }
}
