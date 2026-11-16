package io.github.agampal007.vectordb.server;

import io.github.agampal007.vectordb.server.VectorDbServer.Options;
import io.github.agampal007.vectordb.server.VectorDbServer.SyncMode;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Locale;

/**
 * Starts the server.
 *
 * <pre>
 * --data DIR            where collections are stored (default ./vectordb-data)
 * --host HOST           address to listen on (default 127.0.0.1)
 * --port PORT           default 8080
 * --sync request|none   fsync once per write request, or leave it to the operating system
 * --snapshot-log-mb N   snapshot a collection when its log passes N MB (default 64)
 * --max-body-mb N       largest request body accepted (default 32)
 * </pre>
 */
public final class Main {

    private Main() {}

    public static void main(String[] args) {
        Path data = Path.of("vectordb-data");
        String host = "127.0.0.1";
        int port = 8080;
        Options defaults = Options.defaults();
        SyncMode sync = defaults.sync();
        long snapshotLogBytes = defaults.snapshotLogBytes();
        int maxBodyBytes = defaults.maxBodyBytes();
        try {
            if (args.length % 2 != 0) {
                throw new IllegalArgumentException("every option takes a value");
            }
            for (int i = 0; i < args.length; i += 2) {
                String value = args[i + 1];
                switch (args[i]) {
                    case "--data" -> data = Path.of(value);
                    case "--host" -> host = value;
                    case "--port" -> port = Integer.parseInt(value);
                    case "--sync" -> sync = SyncMode.valueOf(value.toUpperCase(Locale.ROOT));
                    case "--snapshot-log-mb" -> snapshotLogBytes = megabytes(value);
                    case "--max-body-mb" -> maxBodyBytes = Math.toIntExact(megabytes(value));
                    default -> throw new IllegalArgumentException("unknown option " + args[i]);
                }
            }
        } catch (IllegalArgumentException | ArithmeticException e) {
            System.err.println("vectordb: " + e.getMessage());
            System.err.println("options: --data DIR --host HOST --port PORT --sync request|none"
                    + " --snapshot-log-mb N --max-body-mb N");
            System.exit(2);
            return;
        }

        VectorDbServer server;
        try {
            server = VectorDbServer.start(data, host, port, new Options(sync, snapshotLogBytes, maxBodyBytes));
        } catch (IOException e) {
            // A locked directory, a corrupt log, a port in use: say which, without a stack trace.
            System.err.println("vectordb: cannot start: " + e.getMessage());
            System.exit(1);
            return;
        }
        // SIGTERM (docker stop, Ctrl-C) snapshots every collection so the next start is fast.
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                server.close();
            } catch (IOException e) {
                System.err.println("vectordb: shutdown failed: " + e);
            }
        }, "vectordb-shutdown"));
        System.out.println("vectordb listening on http://" + host + ":" + server.port()
                + ", data in " + data.toAbsolutePath() + ", sync=" + sync.name().toLowerCase(Locale.ROOT));
    }

    private static long megabytes(String value) {
        long megabytes = Long.parseLong(value);
        if (megabytes < 1 || megabytes > 1024) {
            throw new IllegalArgumentException("a size in MB must be between 1 and 1024: " + value);
        }
        return megabytes << 20;
    }
}
