package io.github.agampal007.vectordb.server;

import io.github.agampal007.vectordb.DirectoryLock;
import io.github.agampal007.vectordb.Distance;
import io.github.agampal007.vectordb.DurableIndex;
import io.github.agampal007.vectordb.HnswIndex;
import io.github.agampal007.vectordb.wal.WriteAheadLog.SyncPolicy;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/**
 * The named collections under one data directory. Each collection is a subdirectory holding a
 * small settings file and a {@link DurableIndex}.
 *
 * <p>A collection exists once its settings file does. The file is written last when creating and
 * deleted first when dropping, so a crash part-way through either leaves a directory without one,
 * which is ignored at start-up and cleared if the name is used again.
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
    public record Collection(String name, Settings settings, DurableIndex index) {}

    /** Letters, digits, '-' and '_' only: a name is used as a directory name, so nothing else is allowed. */
    static final Pattern NAME = Pattern.compile("[A-Za-z0-9_-]{1,64}");

    static final String SETTINGS_FILE = "collection.properties";
    private static final String SETTINGS_TEMPORARY = "collection.properties.tmp";
    private static final long SEED = 42L;

    private final Path root;
    private final DirectoryLock rootLock;
    private final ConcurrentMap<String, Collection> collections = new ConcurrentHashMap<>();
    /** Serialises create, drop and close. Lookups do not take it. */
    private final Object changes = new Object();
    private boolean closed;

    private CollectionStore(Path root, DirectoryLock rootLock) {
        this.root = root;
        this.rootLock = rootLock;
    }

    /**
     * Opens every collection found under {@code root}, replaying their logs.
     *
     * @throws io.github.agampal007.vectordb.IndexLockedException if another process has the directory
     * @throws IOException if a collection cannot be read. Start-up stops instead of serving without it.
     */
    public static CollectionStore open(Path root) throws IOException {
        DirectoryLock rootLock = DirectoryLock.acquire(root);
        CollectionStore store = new CollectionStore(root, rootLock);
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
                Settings settings = readSettings(directory.resolve(SETTINGS_FILE));
                DurableIndex index = DurableIndex.open(directory, settings::emptyIndex, SyncPolicy.MANUAL);
                store.collections.put(name, new Collection(name, settings, index));
            }
            return store;
        } catch (IOException | RuntimeException e) {
            store.close();
            throw e;
        }
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

    /**
     * Creates an empty collection.
     *
     * @return the new collection, or null if the name is taken
     * @throws IllegalArgumentException if the name is not allowed
     */
    public Collection create(String name, Settings settings) throws IOException {
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
                writeSettings(directory, settings);
            } catch (IOException | RuntimeException e) {
                index.close();
                throw e;
            }
            Collection collection = new Collection(name, settings, index);
            collections.put(name, collection);
            return collection;
        }
    }

    /**
     * Closes the collection and deletes its files.
     *
     * @return false if there is no such collection
     */
    public boolean drop(String name) throws IOException {
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
            return true;
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
                    if (index.logSizeBytes() > EMPTY_LOG_BYTES) {
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

    /** A log with no records is just its 8-byte header. */
    private static final long EMPTY_LOG_BYTES = 8;

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

    private static Settings readSettings(Path file) throws IOException {
        Properties properties = new Properties();
        try (InputStream in = Files.newInputStream(file)) {
            properties.load(in);
        }
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

    /** Writes to a temporary file, forces it to disk and renames it, as snapshots are. */
    private static void writeSettings(Path directory, Settings settings) throws IOException {
        Properties properties = new Properties();
        properties.setProperty("dimension", Integer.toString(settings.dimension()));
        properties.setProperty("distance", settings.distance().name());
        properties.setProperty("m", Integer.toString(settings.m()));
        properties.setProperty("efConstruction", Integer.toString(settings.efConstruction()));
        properties.setProperty("efSearch", Integer.toString(settings.efSearch()));
        Path temporary = directory.resolve(SETTINGS_TEMPORARY);
        try (OutputStream out = Files.newOutputStream(temporary)) {
            properties.store(out, "vectordb collection");
        }
        try (FileChannel channel = FileChannel.open(temporary, StandardOpenOption.WRITE)) {
            channel.force(true);
        }
        Files.move(
                temporary,
                directory.resolve(SETTINGS_FILE),
                StandardCopyOption.ATOMIC_MOVE,
                StandardCopyOption.REPLACE_EXISTING);
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
