# vectordb

A vector database written from scratch in Java: an HNSW approximate nearest-neighbour index,
a write-ahead log for durability, and leader–follower replication, measured against exact search
on standard ANN benchmarks.

It exists to answer, with numbers, the questions a production vector store has to answer:
how much recall do you give up for how much speed, what survives a crash, and what happens to
writes when a node dies.

> **Status:** milestone 1 of 6 done (exact baseline, recall harness, CI). HNSW construction and
> search are in progress. Benchmark numbers below are filled in only from real runs.

## Build

Requires JDK 21 or newer. The Gradle wrapper downloads Gradle itself.

```bash
./gradlew build      # compile, run tests
./gradlew test       # tests only
```

## Design

| Piece | Choice | Why |
|---|---|---|
| Exact search | `FlatIndex`: one packed `float[]`, bounded max-heap of size k | Ground truth for recall; the baseline HNSW must beat on latency |
| ANN index | `HnswIndex` (Malkov & Yashunin, 2018) | State of the art for in-memory ANN; the same family used in Lucene, FAISS and pgvector |
| Metrics | `L2` (squared), `COSINE`, `DOT`, all "smaller is closer" | One ordering rule for every index |
| Determinism | Seeded level assignment, ties broken by id | Reproducible tests and benchmarks |
| Durability (M4) | Append-only WAL with CRC32 per record, periodic snapshots | Crash recovery without rebuilding the graph from scratch |
| Replication (M6) | WAL shipping from leader to followers | Simple, testable; Raft is the stretch goal |

## Milestones

| # | Weeks | Goal | Done when |
|---|---|---|---|
| 1 | 1 | **Exact baseline.** `Distance`, `FlatIndex`, `Recall`, CI on JDK 21 and 25 | ✅ tests green (CI runs on JDK 21 and 25) |
| 2 | 2–3 | **HNSW in memory.** Insert (Alg. 1), layer search (Alg. 2), neighbour heuristic (Alg. 4), k-NN search (Alg. 5) | `recallAt10BeatsTargetOnRandomData` enabled and passing |
| 3 | 4 | **Benchmarks.** Load SIFT-1M / GloVe-100 (`.fvecs`/`.ivecs`), JMH for distance kernels, recall-vs-QPS curve across `efSearch`, build time and memory | Results table and plot in this README |
| 4 | 5 | **Durability.** WAL (append, fsync policy, CRC), snapshot + replay, deletes via tombstones | Test that kills the process mid-write and recovers every acknowledged write |
| 5 | 6 | **Service.** gRPC or HTTP API (collections, upsert, search, delete), read–write locking, Dockerfile | `docker run` + client example in README; concurrent read/write test |
| 6 | 7–8 | **Replication.** Leader ships WAL to followers, follower catch-up, manual failover; Docker Compose cluster | Failure test: kill the leader, promote a follower, no acknowledged write lost |

Stretch: SIMD distance kernels with the Java Vector API, product quantisation, Raft, and plugging
this in as a retriever backend for [rag-retrieval-bench](https://github.com/Agampal007/rag-retrieval-bench).

## Benchmarks

_Not run yet. Milestone 3 fills this with recall@10, queries per second, p99 latency, build time
and memory for FlatIndex and HnswIndex on SIFT-1M, including the hardware they ran on._

## Layout

```
src/main/java/io/github/agampal007/vectordb/
  Distance.java      distance metrics
  VectorIndex.java   index interface
  SearchResult.java  (id, distance) hit
  FlatIndex.java     exact search
  HnswIndex.java     HNSW graph index
  eval/Recall.java   recall@k
```

## Licence

MIT
