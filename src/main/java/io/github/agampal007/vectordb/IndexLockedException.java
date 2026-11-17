package io.github.agampal007.vectordb;

import java.io.IOException;
import java.nio.file.Path;

/** Another process, or another object in this process, already has the directory open. */
public final class IndexLockedException extends IOException {

    private static final long serialVersionUID = 1L;

    public IndexLockedException(Path directory) {
        super(directory + " is already open, here or in another process");
    }
}
