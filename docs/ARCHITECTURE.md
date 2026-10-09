# System Architecture & Technical Specifications

## 1. Overview & Architectural Principles

The **MarketStream Lakehouse** is an institutional-grade financial event-streaming and Medallion Lakehouse platform designed to process high-throughput equity trades, quotes, and execution fills. The system bridges the gap between ultra-low-latency real-time ingestion and high-performance analytical querying by enforcing strict physical separation between ingestion, curation, and analytical marts.

```
+-----------------------------------------------------------------------------------+
|                              PRODUCER / INGESTION TIER                            |
|                                                                                   |
|  [MarketFeedGenerator] (81.2k EPS) ---> [StreamBuffer] (Thread-Safe Queue)        |
|            │                                      │                               |
|            ▼                                      ▼                               |
|  [AnomalyInjector] (Dups, Late Ticks)   [WatermarkedDeduplicator] (750.5k EPS)    |
+---------------------------------------------------┬-------------------------------+
                                                    │
                                                    ▼
+-----------------------------------------------------------------------------------+
|                              MEDALLION LAKEHOUSE STORAGE                          |
|                                                                                   |
|  +--------------------+       +--------------------+       +--------------------+ |
|  |    BRONZE TIER     |       |    SILVER TIER     |       |     GOLD TIER      | |
|  | Append-only raw    | ----> | Cleaned, conformed | ----> | Kimball Star       | |
|  | micro-batch Parquet|       | trade facts + DLQ  |       | Analytical Marts   | |
|  +---------┬----------+       +--------------------+       +--------------------+ |
|            │                                                                      |
|            ▼                                                                      |
|  [CompactionWorker] (Consolidates 40+ small files -> single 128MB Parquet chunk)  |
+-----------------------------------------------------------------------------------+
```

---

## 2. Ingestion & Streaming Layer

### 2.1 Multi-Threaded Ingestion Buffering (`StreamBuffer`)
The ingestion layer decouples network socket / message broker receipt from disk persistence:
- **Thread Safety:** Implements reentrant mutex locks around internal circular double-ended queues (`collections.deque`).
- **Backpressure & Lag Tracking:** Continuously monitors `queue_depth_lag` (number of unacknowledged events awaiting disk commit). If lag exceeds `maxsize`, backpressure signals the load generator to pace dispatch.

### 2.2 Event-Time Watermarking (`WatermarkedDeduplicator`)
Because market feeds arrive across distributed exchange cross-connects (NASDAQ, NYSE, BATS, IEX), arrival timestamps do not match event generation timestamps.
- **Monotonic High-Water Mark:** The system tracks the latest observed `event_timestamp`.
- **Allowed Lateness Boundary:** Defined as $\text{Watermark} - \Delta t_{\text{delay}}$ (configured to 30 seconds). Events with timestamps strictly older than this boundary are tagged `EXCEEDED_EVENT_TIME_WATERMARK` and quarantined.
- **Deterministic Deduplication:** Maintains an internal state hash table of active `trade_id` keys within the active watermark window. Incoming duplicate events matching an existing active key are dropped idempotently.

---

## 3. Storage Hierarchy (Medallion Architecture)

### 3.1 Bronze Tier: Immutable Ingestion Log
- **Layout:** `data/bronze/date=YYYY-MM-DD/hour=HH/bronze_batch_<uuid>.parquet`
- **Semantics:** Append-only raw audit log containing every accepted payload alongside `ingestion_timestamp`.
- **Compression:** Snappy compression for fast write-time encoding.

### 3.2 Silver Tier: Conformed Financial Facts & DLQ
- **Layout:** `data/silver/date=YYYY-MM-DD/silver_trades_<timestamp>.parquet`
- **Validation Pipeline:** Enforces five institutional data contracts:
  1. `trade_id` is non-null and alphanumeric.
  2. `ticker` belongs to valid trading universe.
  3. `execution_price > 0.0`.
  4. `quantity > 0` shares.
  5. `event_timestamp` is a valid ISO 8601 UTC timestamp.
- **Dead-Letter Queue (`data/dlq/`):** Malformed rows that violate contract rules are quarantined into DLQ Parquet files with explicit `dlq_reason` and `quarantined_at` metadata.

### 3.3 Gold Tier: Dimensional Modeling (Kimball Star Schema)
- **Layout:** `data/gold/<mart_name>.parquet`
- **Models:**
  - `agg_intraday_vwap`: 1-minute and 5-minute rolling Volume-Weighted Average Price curves.
  - `fct_execution_slippage`: Fill price variance vs. arrival mid quote benchmark.
  - `dim_instruments`: Instrument metadata, aggregate volume, min/max ranges.
  - `dim_venues`: Execution liquidity breakdown across market centers.

---

## 4. The Compaction Architecture (`CompactionWorker`)

The primary operational failure mode of streaming lakehouses is the **Small-File Problem**:
- Micro-batch commits write small 20KB–500KB Parquet files.
- Over time, partition directories accumulate thousands of files, causing filesystem inode saturation and quadratic metadata deserialization latency during analytical scans.

```
BEFORE COMPACTION: Fragmented Partition
Partition `date=2026-10-09/hour=14/`
 ├── bronze_batch_01.parquet (45 KB)   ──► File handle open + metadata parse
 ├── bronze_batch_02.parquet (52 KB)   ──► File handle open + metadata parse
 ├── bronze_batch_03.parquet (48 KB)   ──► File handle open + metadata parse
 └── ... (40 small files total)        ──► High IOPS overhead (13.98 ms scan)

AFTER COMPACTION: Consolidated Chunk
Partition `date=2026-10-09/hour=14/`
 └── compacted_1728499200_a1b2c3d4.parquet (1.8 MB) ──► Single scan (2.77 ms scan · 5.05x faster)
```

**Compaction Workflow:**
1. Background worker inspects partitions where `uncompacted_file_count >= min_file_count_for_compaction` (default: 4).
2. Reads small Parquet files in zero-copy chunks using PyArrow Arrow tables.
3. Concatenates tables into an optimal row-group chunk (`row_group_size = 100,000`).
4. Writes an optimized, dictionary-encoded consolidated file with Snappy compression.
5. Atomically unlinks original micro-batch files once the compacted file is flushed.
