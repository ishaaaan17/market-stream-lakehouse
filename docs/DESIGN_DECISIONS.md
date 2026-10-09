# Architectural & Technology Design Decisions (ADRs)

## ADR-001: Redpanda / Kafka-Compatible Streaming Engine Over RabbitMQ / Redis

- **Context:** Market execution events require persistent, replayable, distributed streaming with consumer group offset tracking.
- **Decision:** Select Redpanda (Kafka-compatible protocol written in C++) as the distributed event log.
- **Why:** Unlike RabbitMQ (which deletes messages after consumption) or Redis Streams (which keeps messages in memory), Kafka/Redpanda provides an immutable disk-backed commit log with explicit offset commits. Redpanda avoids the JVM garbage-collection pauses and ZooKeeper/KRaft complexity of Apache Kafka while offering 100% wire-protocol compatibility.

---

## ADR-002: In-Memory Watermarking Cache with Sliding LRU Window

- **Context:** Deduplicating a streaming feed of millions of trades requires state tracking. Storing every historical `trade_id` in an external relational database causes millisecond query latencies per incoming tick.
- **Decision:** Implement an in-memory `OrderedDict` LRU cache bounded to `max_cache_size = 250,000` entries, combined with an event-time watermark cutoff boundary.
- **Why:** In-memory hash lookup is $O(1)$ and executes at over **750,000 events/second**. Because late-arriving events older than 30 seconds are rejected by the watermark rule, we only need to maintain `trade_id` hashes within the active watermark horizon. Evicting keys older than the watermark boundary keeps memory consumption flat at $O(1)$.

---

## ADR-003: Asynchronous Out-of-Band Compaction vs. Inline Append Compaction

- **Context:** Streaming writes create hundreds of small micro-batch files (the Small-File Problem). Compacting files inline during each append call would block incoming streaming ingestion.
- **Decision:** Decouple compaction into an asynchronous background daemon (`CompactionWorker`) that periodically sweeps fragmented partitions.
- **Why:** Writing small files at ingestion time preserves sub-second write latencies. The compaction worker runs out-of-band, merging small files into 128MB consolidated Parquet chunks without introducing ingestion backpressure.

---

## ADR-004: Medallion Architecture (Bronze / Silver / Gold) Over Single Mutable Table

- **Context:** We could write directly to an updated/upserted table in Postgres or an in-place DuckDB table.
- **Decision:** Enforce an immutable 3-tier Medallion architecture (Bronze raw logs, Silver curated facts, Gold dimensional marts).
- **Why:** Financial compliance and auditing require an immutable audit trail of raw exchange receipts (Bronze). If a bug in data cleansing or fee calculation is discovered months later, raw Bronze data can be replayed from scratch. Separating Bronze from Silver and Gold ensures raw history is never mutated or lost.

---

## ADR-005: DuckDB Zero-Copy Analytical Compute Over PySpark for Local Execution

- **Context:** Need an ultra-fast analytical SQL query engine to execute Kimball transformations and compute rolling VWAP curves over Parquet partitions.
- **Decision:** Use embedded **DuckDB** for local columnar execution, alongside PyArrow and Polars.
- **Why:** PySpark requires a JVM, Python-Java socket serialization, and significant worker cluster overhead. For local and single-node workloads, DuckDB executes columnar vector scans directly over Parquet files with zero serialization overhead, running in single-digit milliseconds. For distributed cluster scale, the same Parquet/Iceberg layout seamlessly scales to PySpark or Trino.

---

## ADR-006: Dedicated Dead-Letter Queue (DLQ) Quarantine Over Silent Dropping

- **Context:** Corrupted schemas, negative execution prices, and empty ticker symbols must not enter downstream analytics.
- **Decision:** Route malformed records to an isolated `data/dlq/` Parquet store tagged with explicit failure reasons (`dlq_reason`) and quarantine timestamps (`quarantined_at`).
- **Why:** Silently dropping malformed records creates unobservable data loss and makes regulatory reconciliation impossible. Quarantining rows to a DLQ allows data engineers and compliance teams to inspect anomalies, write alerts, and replay corrected records.
