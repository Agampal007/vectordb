# vectordb

A vector database written from scratch in Java: an HNSW approximate nearest-neighbour index,
a write-ahead log for durability, an HTTP service, and leader–follower replication, measured
against exact search on standard ANN benchmarks.

It exists to answer, with numbers, the questions a production vector store has to answer:
how much recall do you give up for how much speed, what survives a crash, and what happens to
writes when a node dies.

> **Status:** all 6 milestones done. On SIFT-1M (1,000,000 vectors) the HNSW index reaches
> recall@10 of 0.975 at 169x the speed of exact search, and 0.993 at 92x, on a single thread.
> Writes are logged before they are applied and survive killing the process; recovery from a
> snapshot is 368x faster than replaying the log. It runs as an HTTP service, in Docker, with
> searches on every core (51,000 to 71,000 a second on 24 threads over 100,000 vectors) that keep
> most of their speed while a writer runs. Followers copy the leader's log and can be promoted by
> hand: a test kills the leader three times over and loses none of the writes it answered.
> Benchmark numbers below come only from real runs.

## Build

Requires JDK 21 or newer. The Gradle wrapper downloads Gradle itself.

```bash
./gradlew build      # compile, run tests
./gradlew test       # tests only
./gradlew sweep      # recall and speed of HNSW against exact search
scripts/download_sift.sh   # SIFT-1M into data/sift (160 MB download)
./gradlew sift             # SIFT-1M benchmark, about 15 minutes; -Pn=100000 for a quick subset
./gradlew durability       # cost of logging and speed of recovery on 100,000 SIFT vectors
./gradlew concurrency      # search throughput by thread count, with a writer, and over HTTP
./gradlew replication      # cost of replication, and a new follower catching up
./gradlew run --args="--port 8080 --data vectordb-data"   # start the server
docker compose up -d --build && scripts/cluster_test.sh   # three-node cluster and a failover
```

## Design

| Piece | Choice | Why |
|---|---|---|
| Exact search | `FlatIndex`: one packed `float[]`, bounded max-heap of size k | Ground truth for recall; the baseline HNSW must beat on latency |
| ANN index | `HnswIndex` (Malkov & Yashunin, 2018) | State of the art for in-memory ANN; the same family used in Lucene, FAISS and pgvector |
| Metrics | `L2` (squared), `COSINE`, `DOT`, all "smaller is closer" | One ordering rule for every index |
| Determinism | Seeded level assignment with saveable generator state, ties broken by id | Reproducible tests and benchmarks; replaying the same operations rebuilds the same graph |
| Deletes | Tombstones: a removed node stays linked but is never returned | Removing a node from an HNSW graph would break paths that run through it |
| Durability | Append-only WAL with a CRC32 and sequence number per record; snapshots written to a temporary file and renamed | No acknowledged write is lost in a crash, and recovery does not have to rebuild the graph from scratch |
| Concurrency | One writer at a time; a read-write lock around the graph, taken exclusively only to link a new node in | Searches run on every core and never wait for a log write, an fsync, a snapshot, or the search an insert does for its neighbours |
| Service | HTTP and JSON on the JDK's built-in server, a virtual thread per request, no dependencies | Anything can call it with `curl`; nothing to audit but this repository |
| Replication | Followers pull the leader's log records byte for byte; each record carries the term it was written in; failover is a manual promotion | The graph is deterministic, so a follower that replays the same records has the leader's graph, link for link. Raft is the stretch goal |

## Milestones

| # | Weeks | Goal | Done when |
|---|---|---|---|
| 1 | 1 | **Exact baseline.** `Distance`, `FlatIndex`, `Recall`, CI on JDK 21 and 25 | ✅ tests green (CI runs on JDK 21 and 25) |
| 2 | 2–3 | **HNSW in memory.** Insert (Alg. 1), layer search (Alg. 2), neighbour heuristic (Alg. 4), k-NN search (Alg. 5) | ✅ recall@10 ≥ 0.90 test passing (0.993 at `efSearch` 100) |
| 3 | 4 | **Benchmarks.** Load SIFT-1M (`.fvecs`/`.ivecs`), recall-vs-QPS curve across `efSearch`, latency, build time and memory | ✅ SIFT-1M table and plot below. GloVe-100 and JMH distance kernels moved to the stretch goals |
| 4 | 5 | **Durability.** WAL (append, fsync policy, CRC), snapshot + replay, deletes via tombstones | ✅ `KillRecoveryTest` kills a writer process 16 times and recovers every acknowledged write. Power loss is not tested |
| 5 | 6 | **Service.** HTTP API (collections, upsert, search, delete), read–write locking, directory lock, Dockerfile | ✅ `docker run` and `curl` example below; CI builds the image, stops it, kills it and checks the data each time; `ConcurrencyTest` runs readers, a writer and snapshots together |
| 6 | 7–8 | **Replication.** Leader ships WAL to followers, follower catch-up, manual failover; Docker Compose cluster | ✅ `KillLeaderTest` kills the leader process three rounds running, promotes the follower that is behind, and finds every answered write on it. CI runs the same failover on a Docker Compose cluster |

Stretch: SIMD distance kernels with the Java Vector API (benchmarked with JMH), multi-threaded
builds, group commit for the log,
reclaiming removed vectors, GloVe-100 for angular distance,
product quantisation, Raft, and plugging this in as a retriever backend for
[rag-retrieval-bench](https://github.com/Agampal007/rag-retrieval-bench).

## Durability

`DurableIndex` wraps the HNSW index so that its contents survive a crash.

```java
try (DurableIndex index = DurableIndex.open(dir, () -> new HnswIndex(128, Distance.L2), SyncPolicy.EVERY_WRITE)) {
    index.add(42, vector);   // on the storage device before this returns
    index.upsert(42, other); // replaces it: one log record, so never half done
    index.remove(7);
    index.snapshot();        // optional: makes the next open fast
}
```

- **Write-ahead log.** Every add, upsert and remove is appended to `wal.log` before it is applied in
  memory. A record is its length, a CRC32, a sequence number, a term (see Replication) and the
  operation. With
  `EVERY_WRITE` each record is fsynced before the call returns; with `MANUAL` the caller decides
  when to sync.
- **Snapshots.** `snapshot()` writes the whole index to a temporary file, fsyncs it, renames it
  over the old snapshot (an atomic step) and then empties the log.
- **Recovery.** Opening a directory loads the snapshot, if there is one, and replays the log
  records with a higher sequence number.
- **Deterministic replay.** Replay runs the same code as the original calls, and the snapshot
  stores the state of the generator that assigns graph levels. A recovered index is therefore the
  same graph, link for link, as one that never stopped. The tests check this directly, and
  replication relies on it.

What recovery does in each failure case, and the test that covers it:

| What happened | What recovery does | Covered by |
| --- | --- | --- |
| The last record was cut short | Truncates it; it was never acknowledged | `WriteAheadLogTest` cuts the last record at every byte |
| The file was extended but the data never arrived (zeros at the end) | Truncates the zeros | `WriteAheadLogTest` |
| A checksum fails before the last record | Refuses to open and leaves the file untouched, because truncating there would drop acknowledged writes | `WriteAheadLogTest`, `DurableIndexTest` |
| The process died while writing a snapshot | Discards the half-written temporary file and uses the previous snapshot plus the log | `DurableIndexTest`; about a third of the kills in `KillRecoveryTest` land here (5 or 6 of 16 in the runs so far) |
| The process died after the snapshot was renamed but before the log was emptied | Uses sequence numbers to skip the records the snapshot already holds | `DurableIndexTest` |
| The snapshot fails its checksum or is cut short | Refuses to open | `DurableIndexTest` |
| The writer process was killed at a random moment | No acknowledged write is missing and the graph equals an uninterrupted build | `KillRecoveryTest`: 16 kills of a real JVM, each continuing from the last |

### What it costs

Output of `./gradlew durability` on the first 100,000 SIFT vectors (`results/durability-100k.txt`),
same laptop and settings as the SIFT-1M run below:

| Step | Time | Notes |
| --- | --- | --- |
| Build in memory | 32.7 s | no durability |
| Build with every write logged, one fsync at the end | 33.2 s | log is 51 MB |
| Recover by replaying the log | 33.3 s | rebuilds the graph insert by insert |
| Write a snapshot | 0.11 s | 58 MB, includes the fsync |
| Recover from the snapshot | 0.09 s | 368x faster than replay |

- **Logging is cheap; fsync is not.** Appending every write to the log added 0.5 s to a 33 s
  build. Forcing each record to the device is the real cost: the log managed 1,771 appends per
  second with an fsync per record against 257,526 without. That is about 0.56 ms per fsync, more
  than the 0.33 ms an insert takes at this size, so `EVERY_WRITE` would roughly triple the build
  time (an estimate from these two figures, not a measured run). Group commit, one fsync for many
  writers, is the usual fix and is a stretch goal.
- **Replay costs as much as the original build**, because it re-inserts every vector. A snapshot
  stores the finished graph, so loading it took 0.09 s, and searches on the loaded index returned
  exactly the results of the in-memory build.

### What is not covered

- **Power loss is not tested.** Killing a process does not test fsync, because the operating
  system still flushes its cache afterwards. Whether an fsync really reaches the disk also depends
  on the device.
- **A damaged final record is dropped silently.** It cannot be told apart from a torn write. The
  same goes for a record whose damaged length field makes it appear to run past the end of the file.
- **One process per directory.** A second opener is refused (see the service section). The lock
  is advisory: it stops this program, not another one writing to the same files.
- **Removed vectors keep their space**, in memory and in snapshots, until the index is rebuilt.
- **A snapshot rewrites the whole index.** Its cost grows with the index, not with what changed.
- **On Windows the snapshot rename is atomic but not fsynced**, because a directory cannot be
  opened for fsync there.

## Service

```bash
./gradlew run --args="--port 8080 --data vectordb-data"
```

or, in Docker, with the data in a named volume:

```bash
docker build -t vectordb .
docker run -p 8080:8080 -v vectordb-data:/data vectordb
```

Then, from another terminal:

```bash
curl -X PUT localhost:8080/collections/demo -d '{"dimension": 4, "distance": "l2"}'

curl -X POST localhost:8080/collections/demo/vectors -d '{"vectors": [
    {"id": 1, "vector": [1, 0, 0, 0]},
    {"id": 2, "vector": [0, 1, 0, 0]},
    {"id": 3, "vector": [0, 0, 1, 0]}]}'

curl -X POST localhost:8080/collections/demo/search -d '{"vector": [0.5, 1, 0, 0], "k": 2}'
# {"results":[{"id":2,"distance":0.25},{"id":1,"distance":1.25}]}
```

`scripts/smoke_test.sh` runs these requests and checks the answers.

| Request | Body | Answer |
| --- | --- | --- |
| `GET /health` | | `{"status":"ok"}` |
| `GET /collections` | | every collection with its settings and size |
| `PUT /collections/{name}` | `dimension`; optional `distance` (`l2`, `cosine`, `dot`), `m`, `efConstruction`, `efSearch` | 201 and the collection; 409 if the name is taken |
| `GET /collections/{name}` | | settings and size |
| `DELETE /collections/{name}` | | closes the collection and deletes its files |
| `POST /collections/{name}/vectors` | `vectors`: a list of `{"id", "vector"}` | `{"upserted": n, "replaced": r}` |
| `GET /collections/{name}/vectors/{id}` | | the stored vector, or 404 |
| `DELETE /collections/{name}/vectors/{id}` | | `{"removed": true}`, or 404 |
| `POST /collections/{name}/search` | `vector`; optional `k` (default 10) and `efSearch` | `results`: `{"id", "distance"}`, closest first |
| `POST /collections/{name}/snapshot` | | writes a snapshot now |

Errors are `{"error": "..."}` with status 400 (bad input), 404, 405, 409 or 413 (body over
`--max-body-mb`, 32 MB by default). Ids are 64-bit integers and are never passed through a
floating-point number.

- **A write is answered after it is durable.** The request's records go to the log, one fsync
  covers the whole request, and only then is the answer sent. `--sync none` skips the fsync: an
  answered write then survives a crash of the server but not of the machine.
- **A bad entry rejects the whole request.** Every vector in a request is checked before any of
  them is written.
- **Upsert is one log record.** Replacing a vector by removing and re-adding it would be two
  records, and a crash between them would leave the id with nothing. `UpsertTest` cuts the record
  short and checks that the old vector is still there.
- **Recovery time is bounded.** A collection is snapshotted when its log passes
  `--snapshot-log-mb` (64 MB by default), and on a clean shutdown, so a restart never replays more
  than that.
- **One process per data directory.** The server takes an operating-system lock on a `LOCK` file
  in the data directory and in each collection. A second server on the same directory is refused
  at start-up instead of corrupting the log. The operating system drops the lock when the process
  dies, so a crash never leaves the directory locked; `DirectoryLockTest` kills the holder to
  check that.
- **Collection names** are 1 to 64 letters, digits, `-` or `_`. A name becomes a directory name,
  so nothing else is accepted.

### Concurrency

`DurableIndex` is safe to call from any number of threads. It uses two locks:

- A **writer lock** admits one writer at a time and is held across the log append and the change
  in memory. Operations therefore reach the log in the order they were applied, which is what
  makes replay rebuild the same graph.
- A **graph lock**, a read-write lock around the in-memory index. Searches share it. A writer
  takes it exclusively only to link a new node in.

An insert spends nearly all its time finding the new vector's neighbours, and that only reads
the graph. So an insert has two halves. `prepareAdd` finds the neighbours and works out the link
lists they will have, holding only the writer lock, while searches carry on. `commit` then takes
the graph lock exclusively and applies the result, which is a handful of array copies. Nothing
can change the graph between the two halves, because only the holder of the writer lock writes.

A search therefore waits only for that linking step, and never for the disk: not for a log
append, not for an fsync, and not for a snapshot, which only reads the index. Each search also
borrows its own scratch space, where the single-threaded index shared one.

Splitting the insert did not change what it builds. On each layer the search looks only at that
layer's links, the new node cannot be reached until it is linked, and the neighbours chosen on a
layer are distinct, so no step depends on a change an earlier step would have made.
`GraphFingerprintTest` pins the SHA-256 of four graphs as the one-step insert built them. That
matters beyond tidiness: the log stores operations, so a log written by an older build must
replay into the graph that build had.

What the tests check:

| Guarantee | Test |
| --- | --- |
| Eight threads searching at once get exactly the results one thread gets | `ConcurrencyTest` |
| While a writer adds and removes and another thread snapshots, no search returns a vector whose removal had already returned, and the final graph equals a single-threaded build, in memory and after reopening | `ConcurrencyTest` |
| With four writers at once, replaying the log rebuilds the graph that was in memory | `ConcurrencyTest` |
| The insert builds, bit for bit, the graphs it built before it was split, for each metric, with removals, re-added ids and tied distances | `GraphFingerprintTest` |
| Preparing an insert changes nothing a search can see; a plan is refused once the graph has moved on | `PrepareCommitTest` |
| A second opener is refused, in the same process and from another one, and is admitted once the holder is killed | `DirectoryLockTest` |
| Search results over HTTP equal the results of calling the index directly | `VectorDbServerTest` |
| 36 malformed or misdirected requests are refused and change nothing on disk | `VectorDbServerTest` |
| Data survives a server restart; concurrent clients reading and writing all succeed | `VectorDbServerTest` |
| In Docker: after `docker stop` the snapshot is loaded and nothing is replayed; after `docker kill` the log is replayed; the data is intact both times | the `docker` job in CI |

The `ConcurrencyTest` checks were each seen to fail with the lock they cover removed: sharing
one scratch space between searches, returning removed vectors, dropping the writer lock, and
linking a new node in under the shared lock instead of the exclusive one.

### What it measures

Output of `./gradlew concurrency` on the first 100,000 SIFT vectors
(`results/concurrency-100k.txt`), `efSearch` 100, `k` 10, on the same laptop as the other
benchmarks (8 performance and 16 efficiency cores, no hyper-threading). Each row starts from a
copy of the same snapshot and runs for 4 seconds after 1 second of warm-up. The writer adds new
vectors one at a time and fsyncs each.

| Search threads | Searches/s, no writer | Per thread | Searches/s, one writer | Writes/s (fsync each) |
| --- | --- | --- | --- | --- |
| 1 | 4,358 | 4,358 | 4,091 | 842 |
| 2 | 9,074 | 4,537 | 8,278 | 806 |
| 4 | 18,308 | 4,577 | 12,734 | 580 |
| 8 | 30,364 | 3,796 | 31,884 | 532 |
| 16 | 52,821 | 3,301 | 45,364 | 479 |
| 24 | 64,141 | 2,673 | 60,908 | 90 |

- **Searches use every core.** Throughput went from 4,358 searches a second on one thread to
  64,141 on 24. Across eight runs of this benchmark the 24-thread figure ranged from 50,835 to
  70,884.
- **This laptop is a noisy place to measure, so no speed-up factor is quoted.** The one-thread
  figure came out near 2,100 in some runs and near 4,400 in others, with nothing changed between
  them. Two speeds are what a lone thread landing on an efficiency core or a performance core
  would give, but that was not verified. Compare columns within a row, which were measured one
  after the other, before comparing rows or runs.
- **At 24 search threads the writer is starved.** Every core is already searching, the writer is
  a 25th thread, and it managed 77 to 355 inserts a second across runs, against 400 to 840 with
  one search thread.

**What moving the neighbour search out of the exclusive lock bought.** Before the insert was
split, it held the graph exclusively for its whole 0.33 ms. The old and new code were run back to
back, twice each (`results/concurrency-100k-insert-lock.txt`). The table shows how much of their
no-writer throughput searches kept once a writer was running, in each of the two runs:

| Search threads | Kept before | Kept after |
| --- | --- | --- |
| 1 | 56%, 39% | 90%, 98% |
| 2 | 51%, 92% | 91%, 98% |
| 4 | 55%, 51% | 88%, 94% |
| 8 | 45%, 40% | 91%, 86% |
| 16 | 47%, 40% | 72%, 87% |
| 24 | 92%, 66% | 105%, 78% |

- **From 1 to 16 search threads, searches kept 39% to 56% of their throughput before and 72% to
  98% after.** The one exception before, 92% at 2 threads, is a slow run without the writer, not
  a fast one with it: two threads managed 5,295 searches a second where one thread had just
  managed 4,182.
- **The 24-thread row shows no clear change.** It was noisy in both versions, for the reason
  given above.
- **What is left.** The writer still needs the graph to itself to link the node in, so it waits
  for the searches in flight to finish, and searches that arrive meanwhile wait behind it.
  Per-node locks, as hnswlib uses, would remove that wait; they are not done.

The same searches through the HTTP API on localhost, with `GET /health`, which does no work, as
the baseline:

| Request | Clients | Requests/s | Median latency | 99th percentile |
| --- | --- | --- | --- | --- |
| `GET /health` | 1 | 747 | 1.29 ms | 2.43 ms |
| `POST search` | 1 | 531 | 1.79 ms | 3.86 ms |
| `GET /health` | 8 | 6,184 | 1.27 ms | 1.86 ms |
| `POST search` | 8 | 4,545 | 1.72 ms | 2.53 ms |
| `GET /health` | 24 | 6,593 | 3.56 ms | 5.81 ms |
| `POST search` | 24 | 6,201 | 3.77 ms | 6.08 ms |

- **The HTTP layer, not the index, sets the latency here.** A request that does nothing takes
  1.3 to 3.6 ms, and a search adds 0.2 to 0.5 ms to that. With 8 clients the service answered 4,545
  searches a second, where 8 threads calling the index directly managed 30,364.
- This was measured on Windows with the JDK's built-in server and client. It has not been measured
  on Linux or with another client, so it says little about the cost elsewhere.

### What the service does not do

- **No authentication and no TLS.** It listens on `127.0.0.1` unless `--host` says otherwise. The
  Docker image listens on all of the container's interfaces, so publish the port only where the
  network is trusted, or put a proxy in front of it.
- **A request is not atomic.** Each vector is its own log record. If the server dies part-way
  through a request, the vectors already logged may be kept, and the client, which got no answer,
  has to send the request again. Sending it again is safe, because upsert replaces.
- **The Docker image has only been run in CI**, on Linux. Docker is not installed on the machine
  the rest of this was developed on.
- **The request that crosses the log limit pays for the snapshot** (0.11 s at 100,000 vectors),
  and other writers wait for it. Searches do not.
- **Vectors only.** No metadata, no filtering, no paging through a collection.

## Replication

A cluster is one leader and any number of followers. Writes go to the leader; any node answers
searches. `docker-compose.yml` starts three nodes, `node1` leading:

```bash
docker compose up -d --build
curl -X PUT localhost:8081/collections/demo -d '{"dimension": 4}'
curl -X POST localhost:8081/collections/demo/vectors -d '{"vectors": [{"id": 1, "vector": [1, 0, 0, 0]}]}'
curl localhost:8082/collections/demo/vectors/1     # already on node2 or node3, see below
curl localhost:8082/replication/status             # role, term and position of each collection
```

A node started with `--follow http://leader:8080` follows that leader. With `--min-replicas N`,
the leader answers a write only once N followers hold it on their own storage device, and answers
503 if they do not within `--replication-timeout-ms` (5 seconds by default). The write is then on
the leader and maybe on some followers: it may or may not survive a failover.

### Failover, by hand

There is no election. When the leader dies, an operator promotes a follower, naming the others as
its peers, and points the rest at it:

```bash
curl -X POST localhost:8082/admin/promote -d '{"peers": ["http://node3:8080"]}'
curl -X POST localhost:8083/admin/follow  -d '{"leader": "http://node2:8080"}'
# when node1 comes back:
curl -X POST localhost:8081/admin/follow  -d '{"leader": "http://node2:8080"}'
```

`scripts/cluster_test.sh` does exactly this on the Compose cluster, and CI runs it.

### How it works

- **Followers pull.** Each follower keeps one request open to the leader for the collection list,
  and one per collection for its log. The leader holds a request open until it has something new
  (long polling), then sends the next log records byte for byte, as they are stored, with their
  checksums. The follower appends them to its own log, keeping their sequence numbers, applies them
  through the same code as recovery, forces them to its device, and asks for what comes next.
- **Asking is acknowledging.** A follower asks for the records after position *p* only once
  everything up to *p* is on its own device. So the leader learns from each request how far that
  follower has got, and a write waiting for `--min-replicas` is released by the followers' next
  requests. There is no separate acknowledgement message.
- **Only durable records are sent.** The leader sends a record only after it has forced it to its
  own device. Otherwise a follower could hold an operation that the leader loses in a power cut,
  and the two logs would hold different operations under the same number.
- **Terms.** Every log record carries the term it was written in, the term of the node that
  accepted the write. Each promotion starts a new, higher term. Before sending anything, the leader
  checks that it holds the follower's last record, with the same term. If the follower is ahead of
  it, holds a different term at that number, or is so far behind that the records it needs are now
  only in a snapshot, the leader answers 410, and the follower downloads a snapshot of the leader's
  collection and starts again from it. This is the check Raft calls log matching, with "start again
  from a snapshot" in place of truncating, since a node cannot be removed from an HNSW graph.
- **Identical graphs.** The graph is deterministic: the snapshot keeps the state of the generator
  that assigns graph levels, and replay runs the same code. A follower's collection is therefore
  the leader's, link for link, and two nodes at the same position produce byte-identical snapshots.
  The tests compare snapshots byte for byte instead of sampling search results.
- **Promotion catches up first.** A write answered with `--min-replicas 1` is on at least one
  follower, but not necessarily the one being promoted. So `promote` asks each peer for its
  position, copies whatever is missing from the peer furthest ahead in each collection, and only
  then accepts writes. Without this step, promoting the wrong follower loses answered writes;
  `KillLeaderTest` checks that it fails without it.
- **Fencing the old leader.** Promotion announces the new term to the peers, which adopt it and
  from then on refuse anything from a node in an older term. They also send their term with every
  request, so a restarted old leader that one of them contacts learns it has been replaced and
  stops accepting writes (409). With `--min-replicas 1` or more it cannot answer a write anyway,
  because no follower confirms it; its unconfirmed operations are discarded when it follows the new
  leader, because its log no longer matches.
- **Collections.** Creating and dropping a collection are replicated through the collection list,
  which has a version the followers acknowledge in the same way. Each collection has an
  incarnation id, so a collection dropped and created again under the same name is not mistaken for
  the old one.
- **Roles survive restarts.** A node's id, term, role and leader are kept in `node.properties` in
  its data directory. `--follow` applies only to a new data directory, so a promoted node still
  leads after a restart, whatever its command line says.

What the tests check:

| Guarantee | Test |
| --- | --- |
| A record copied to another log keeps its number and term; terms never go down; damaged or torn records are refused, header included | `WriteAheadLogTest` |
| A follower that applies the leader's records has the leader's graph, link for link, and so does its own log when replayed | `ReplicatedIndexTest` |
| The leader refuses a follower that is ahead, holds a different term at its last record, or needs records only in a snapshot; it sends only records forced to its device | `ReplicatedIndexTest` |
| A downloaded snapshot that fails its checksum changes nothing | `ReplicatedIndexTest` |
| With one follower and `--min-replicas 1`, every answered write is already on the follower, with byte-identical snapshots | `ReplicationTest` |
| A leader without enough followers answers 503 but keeps the write; a follower that joins later copies it | `ReplicationTest` |
| A follower behind a compacted log starts again from a snapshot and ends identical | `ReplicationTest` |
| Dropping and re-creating a collection under the same name reaches the follower | `ReplicationTest` |
| Promotion copies the writes only a peer had; the restarted old leader is fenced off and then follows, losing its unconfirmed write | `ReplicationTest` |
| **Kill the leader process, promote the follower that is behind: no answered write is lost**, and the old leader ends byte-identical. Three rounds, each killing the current leader | `KillLeaderTest` |
| The same failover on three Docker containers | the `cluster` job in CI, `scripts/cluster_test.sh` |

`KillLeaderTest` was also run against two broken versions to check that it notices: with the
leader not waiting for a follower, and with promotion not catching up from its peer. Both times it
reported an answered write missing from the new leader.

In each round of `KillLeaderTest`, one follower is killed first, so that the writes after it are
confirmed by the other follower alone, and then the leader. The follower that was killed first is
the one promoted: the worse choice, on purpose. In the three runs so far (nine rounds), between
168 and 584 writes per round were answered after it died, and promotion copied every one of them
from its peer.

### What it costs

Output of `./gradlew replication` (`results/replication-100k.txt`), SIFT vectors, all servers in
one JVM on the same laptop as the other benchmarks, talking over localhost. One run.

Single-vector writes sent one at a time, each forced to the device before it is answered:

| Setup | Writes/s | Median latency | 99th percentile |
| --- | --- | --- | --- |
| No followers | 252 | 3.56 ms | 8.17 ms |
| 1 follower, leader does not wait | 318 | 3.13 ms | 5.08 ms |
| 1 follower, leader waits for it | 119 | 7.66 ms | 16.63 ms |
| 2 followers, leader waits for 1 | 143 | 6.84 ms | 9.52 ms |
| 2 followers, leader waits for both | 129 | 7.42 ms | 12.70 ms |

- **Waiting for a follower roughly doubles the latency of a lone write**, from 3.56 ms to 7.66 ms
  at the median. The extra is one more round trip: the follower's open request returns the record,
  the follower appends it, forces it to its device and applies it, and its next request releases
  the write. An HTTP exchange through the JDK's client and server already costs 1.3 to 3.6 ms on
  this machine (see the concurrency section).
- **The differences between the other rows are within this laptop's noise.** A follower that is not
  waited for cannot make the leader faster, yet that row came out ahead of "no followers"; and
  waiting for one of two followers came out slightly faster than waiting for the only one. The
  earlier 10,000-vector run gave the same order, within 0.5 ms.
- These are writes of one vector, one at a time. A request of many vectors waits once.

A new follower copying a collection of 100,000 vectors:

| Step | Time | Notes |
| --- | --- | --- |
| Load the leader through the API | 74.6 s | 100 requests of 1,000 vectors |
| New follower, copying the leader's log | 73.6 s | replays 100,000 operations, building the graph again |
| New follower, after the leader's log was emptied by a snapshot | 0.79 s | downloads a 58 MB snapshot; 93x faster |

- **Copying the log costs as much as building the graph**, because the follower inserts every
  vector again. A snapshot carries the finished graph. Both followers ended with snapshots byte for
  byte the leader's.
- **Loading through the API took 74.6 s, against 32.7 s for building the same graph in memory**
  in the durability benchmark. HTTP and JSON account for some of that, and this laptop's
  single-thread speed varies by up to two times between runs; the difference was not investigated
  further.


### What replication does not do

- **No automatic failover.** Nothing notices that the leader is gone; an operator promotes a
  follower. There is no election and no quorum, so two nodes promoted at the same time would both
  lead. Raft is the stretch goal.
- **Promotion relies on its peers being reachable.** An answered write is on at least
  `--min-replicas` followers. If all of those are down when another follower is promoted, the write
  is not on the new leader. A peer that cannot be reached during promotion also does not learn the
  new term, so it could keep following a restarted old leader.
- **`--min-replicas 0` is asynchronous.** The leader answers before any follower has the write, and
  a write answered just before the leader dies can be lost on failover. A restarted old leader with
  no followers can also accept writes nobody else will see.
- **Reads on a follower can be behind** the leader by whatever has not reached it yet.
- **A follower that falls far behind downloads a whole snapshot.** Once the leader's log has been
  compacted past a follower's position, the leader writes a snapshot for it while holding its
  writer lock (0.11 s at 100,000 vectors), so writes wait for that. Searches do not.
- **Replication traffic is plain HTTP**, like the API: no authentication and no TLS between nodes.
- **Tested on one machine and in CI.** The processes in `KillLeaderTest` and the containers in CI
  share one host; network partitions and power loss are not tested.

## Benchmarks

### SIFT-1M

[SIFT-1M](http://corpus-texmex.irisa.fr/) is a standard ANN benchmark: 1,000,000 128-dimensional
image descriptors, 10,000 query vectors, and the exact 100 nearest neighbours of each query. Recall
is measured against those published neighbours. Exact search scores 1.000 against them, which also
checks the file reader and the distance code.

Run on a laptop (Intel Core Ultra 9 275HX, 64 GB RAM, JDK 25), single-threaded for both building
and querying, `M` = 16, `efConstruction` = 200, k = 10. Full output: `results/sift-1m.txt`.

- **Build:** 742 s (12.4 minutes) for 1,000,000 inserts.
- **Heap:** 1.52 GB with both the HNSW index and the exact index loaded (each holds its own copy of
  the 512 MB of vectors).

| Index | efSearch | Recall@10 | Queries/s | p50 latency (ms) | p99 latency (ms) |
| --- | --- | --- | --- | --- | --- |
| Exact (FlatIndex) | - | 1.000 | 16 | 56.9 | 94.4 |
| HNSW | 10 | 0.709 | 12,254 | 0.076 | 0.179 |
| HNSW | 20 | 0.839 | 8,232 | 0.116 | 0.226 |
| HNSW | 40 | 0.928 | 4,681 | 0.207 | 0.422 |
| HNSW | 80 | 0.975 | 2,697 | 0.363 | 0.644 |
| HNSW | 160 | 0.993 | 1,473 | 0.671 | 1.210 |
| HNSW | 320 | 0.998 | 806 | 1.235 | 2.194 |
| HNSW | 640 | 0.999 | 426 | 2.302 | 4.397 |

![Recall@10 against queries per second on SIFT-1M](docs/sift-1m-recall-qps.png)

What it shows:

- **At a million vectors the graph pays off.** At recall 0.975 HNSW answers 169 times as many
  queries per second as a full scan (2,697 against 16), and at 0.993 it is still 92 times faster.
  The p99 latency at `efSearch` 80 is 0.64 ms against 94 ms. At 10,000 vectors (below) the same
  index was only 1.7-3.2 times faster, because a small scan is cheap.
- **The last per cent of recall is the expensive part.** Going from 0.975 to 0.993 halves the
  throughput, and 0.998 to 0.999 halves it again.
- **This is a plain Java implementation.** Distances are scalar loops (no SIMD), and the build is
  single-threaded, so optimised C++ libraries are faster. The stretch goals target both.

Caveats: one run on one machine; timings from `System.nanoTime()` around each query after a
warm-up, not JMH. Exact-search speed is timed on the first 200 queries; HNSW on all 10,000.

### First check: 10,000 random vectors

Output of `./gradlew sweep`: 10,000 uniform random 32-dimensional vectors, 200 queries, k = 10,
L2, `M` = 16, `efConstruction` = 200. Single-threaded on a laptop with an Intel Core Ultra 9 275HX,
JDK 25. HNSW build took 1.9 s.

| Index | efSearch | Recall@10 | Queries/s |
| --- | --- | --- | --- |
| Exact (FlatIndex) | - | 1.000 | 6,591 |
| HNSW | 10 | 0.596 | 98,037 |
| HNSW | 20 | 0.786 | 50,512 |
| HNSW | 50 | 0.945 | 21,292 |
| HNSW | 100 | 0.993 | 11,354 |
| HNSW | 200 | 1.000 | 5,691 |

What it shows:

- **`efSearch` is the recall/speed dial.** Each doubling roughly halves throughput, and recall
  climbs from 0.596 to 1.000 across the range.
- **At this size HNSW wins only modestly.** At recall 0.945 it is 3.2x faster than a full scan;
  at 0.993, 1.7x. A scan of 10,000 vectors is cheap, so the gap grows with the dataset (see
  SIFT-1M above).
- **Random data is a hard case.** Uniform vectors in 32 dimensions have no clusters for the graph
  to exploit, so real embeddings usually reach the same recall at a lower `efSearch`.

Caveats: one dataset and size, one seed, timings from `System.nanoTime()` after a warm-up rather
than JMH, and the numbers vary by a few per cent between runs.

### Known limitation: many exact duplicates

When hundreds of vectors are identical, the early copies fill their link lists and ties keep the
older links, so later copies get no inbound links. In a test with 300 identical vectors, 34 were
reachable. Search still returns exact matches, but not every copy. Deduplicate upstream if this
matters.

## Layout

```
src/main/java/io/github/agampal007/vectordb/
  Distance.java      distance metrics
  VectorIndex.java   index interface
  SearchResult.java  (id, distance) hit
  FlatIndex.java     exact search
  HnswIndex.java     HNSW graph index
  eval/Recall.java   recall@k
  DurableIndex.java  HNSW index with a write-ahead log, snapshots, recovery and locking
  DirectoryLock.java one process per directory
  Snapshot.java      checksummed, atomically replaced copy of the index on disk
  Lcg.java           java.util.Random's generator with saveable state
  wal/               WriteAheadLog: checksummed records, torn-write recovery
  io/VecsFiles.java  .fvecs / .ivecs readers for ANN benchmark data
  server/            VectorDbServer (HTTP API), CollectionStore, Json, Main;
                     replication: Replication (endpoints, promotion), Follower, ReplicaClient,
                     ReplicaTracker (acknowledgements), NodeState (role and term on disk)
src/test/java/...    unit tests, KillRecoveryTest (kills a writer process), ConcurrencyTest,
                     ReplicationTest, KillLeaderTest (kills leader processes),
                     RandomDataSweep (./gradlew sweep), SiftBenchmark (./gradlew sift),
                     DurabilityBenchmark (./gradlew durability), ConcurrencyBenchmark
                     (./gradlew concurrency), ReplicationBenchmark (./gradlew replication)
Dockerfile           two-stage build; runs the server as an unprivileged user
docker-compose.yml   three-node cluster
scripts/             SIFT-1M download, recall/QPS plot, smoke_test.sh (curl against a server),
                     cluster_test.sh (failover on the Compose cluster)
results/, docs/      saved benchmark output and plots
```

## Licence

MIT
