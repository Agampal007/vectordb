package io.github.agampal007.vectordb.wal;

import java.io.IOException;
import java.nio.file.Path;

/**
 * The log is damaged somewhere other than its tail, so opening it would silently drop writes that
 * were acknowledged. Recovery stops and leaves the file untouched for inspection.
 */
public final class WalCorruptedException extends IOException {

    private static final long serialVersionUID = 1L;

    public WalCorruptedException(Path path, long offset, String reason) {
        super(path + ": " + reason + " at byte " + offset);
    }
}
