package io.github.agampal007.vectordb;

import java.io.IOException;
import java.nio.file.Path;

/** The snapshot file fails its checksum or does not parse. It is left untouched for inspection. */
public final class CorruptSnapshotException extends IOException {

    private static final long serialVersionUID = 1L;

    public CorruptSnapshotException(Path path, String reason, Throwable cause) {
        super(path + ": " + reason, cause);
    }
}
