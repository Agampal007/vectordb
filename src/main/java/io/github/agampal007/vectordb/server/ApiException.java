package io.github.agampal007.vectordb.server;

/** A request that cannot be served, with the HTTP status to answer it with. */
final class ApiException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    final int status;

    ApiException(int status, String message) {
        super(message);
        this.status = status;
    }
}
