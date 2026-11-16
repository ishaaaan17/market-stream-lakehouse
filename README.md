# MarketStream Lakehouse — High-Throughput Streaming Ingestion (Python) + Medallion Lakehouse (DuckDB / Parquet / dbt)

## System Overview

MarketStream is a streaming data platform for financial market executions: a high-throughput ingestion pipeline with event-time watermarking and idempotent deduplication, feeding a Bronze → Silver → Gold medallion lakehouse of Parquet files queried with DuckDB, with a dbt project modelling the same Gold marts in SQL.

The feed is **simulated** — a generator producing execution reports across **5 venues** (`XNAS`, `XNYS`, `BATS`, `ARCA`, `IEXG`) and 10 symbols, with an anomaly injector that deliberately corrupts it (duplicates, late arrivals, malformed rows). That is a deliberate choice, not a shortcut: the system under test is the *pipeline*, and a generator makes the three failure modes that break real lakehouses reproducible on demand:

1. **The small-file problem** — streaming micro-batches write many tiny Parquet files per partition, and analytical scans degrade as file counts grow.
2. **At-least-once duplicates** — broker retries and consumer crashes replay messages; a naive lakehouse ingests phantom trades and corrupts VWAP curves and ledgers.
3. **Out-of-order and late ticks** — events from different venues arrive with non-deterministic delays; without an event-time watermark, stateful processing either drops valid trades or accepts arbitrarily stale ones.

Every number in this README is measured by the scripts in `benchmarks/` and recorded in `docs/BENCHMARKS.md` — nothing is estimated or copied from another project's documentation.

## Results

| Pipeline dimension | Measured | What it means |
|---|---|---|
| Producer throughput | **81,196 events/sec** | 104,108 events generated and enqueued in 1.282 s |
| Consumer drain + dedup rate | **750,564 events/sec** | The same 104,108 events validated and state-checked in 0.139 s — **~9.2x faster than production**, so the consumer drains burst backpressure instead of falling behind |
| Compaction scan acceleration | **5.05x faster** | DuckDB scan over one partition: **13.98 ms → 2.77 ms** after consolidating 40 small files into 1 |
| Storage reduction from compaction | **18.94%** | Dictionary encoding + consolidated row groups vs. fragmented micro-batches |
| Worker-crash fault tolerance | **0 duplicates · 0 lost records** | Verified two independent ways — see *Exactly-Once, Proven Twice* below |

One full benchmark run, end to end (100,000 generated trades + injected anomalies = **104,108 events**):

| Outcome | Events |
|---|---|
| Duplicates blocked by the deduplicator | 3,990 |
| Late ticks quarantined (past the 30 s watermark) | 3,092 |
| Clean trades committed to the lakehouse | 97,026 |

The dbt project builds clean against the same data: `dbt build` → **PASS=17** (models + data tests).

Reproduce everything:

```bash
python benchmarks/benchmark_throughput.py   # EPS, lag, dedup/quarantine counts
python benchmarks/benchmark_compaction.py   # scan latency before vs. after compaction
pytest                                      # 14 tests, incl. the real SIGKILL recovery test
```

---

## 🚀 Core Architectural Features

### Simulated Feed + Anomaly Injection (`src/generator/`)

`MarketFeedGenerator` emits execution reports with arrival-benchmark pricing and execution slippage across the 5 venues and 10 symbols. `AnomalyInjector` then corrupts the stream at fixed, declared rates — **4% duplicate trade IDs, 3% late arrivals lagging 45–120 s, 0.5% malformed rows** (negative prices, empty tickers). Because the corruption rates are known, the pipeline's outputs are checkable exactly: the benchmark run above *must* block ≈4% as duplicates and quarantine the late arrivals that fall outside the watermark window, and it does (3,990 and 3,092).

### Watermarked Deduplicator (`src/ingestion/deduplicator.py`)

The deduplicator is where correctness lives. It maintains:

- A **monotonic event-time watermark**: the highest `event_timestamp` seen, minus a bounded delay of **30 seconds** (`watermark_delay_seconds` in `config/settings.py`). An event arriving with a timestamp behind the watermark is not silently dropped — it is **quarantined** and counted, so late data is visible rather than lost.
- An **in-memory set of seen `trade_id`s** (capped at 250,000 entries). A replayed event whose ID is already in the set is rejected idempotently, regardless of which batch or restart it arrives in.

Validation and dedup run at ~750k events/sec because the check is a hash lookup, not a storage round-trip — the consumer is deliberately ~9x faster than the producer so backpressure spikes drain.

### Exactly-Once, Proven Twice (`src/chaos/`)

"Exactly-once" is claimed here in one precise sense: *after a worker crash and a full replay of the stream, the committed set contains every unique trade exactly once — no duplicates, no losses* — and the reconciliation is **computed from the data, never hardcoded**. It is proven two ways:

1. **In-process simulation** (`chaos_engine.py`): the consumer processes 2,500 trades, is dropped mid-flight, and the broker re-delivers the last 1,000 unacknowledged events along with the rest of the stream. The test then checks, on the actual committed output, that there are zero duplicate trade IDs **and** that `committed ∪ quarantined == the original set of unique trade IDs` exactly — no phantom records, none missing.
2. **Real subprocess crash test** (`real_crash.py`, `tests/test_real_crash_recovery.py`): ingestion runs in a **separate process** whose deduplicator persists its state (seen IDs + watermark) to disk after every batch via `save_state()`/`load_state()`. The process is killed with a real **`SIGKILL`** — no cleanup, no graceful shutdown — then restarted, the full stream is replayed, and the same computed reconciliation must pass. This is the test that makes the exactly-once claim more than a simulation: the recovery path is the same state-restore code a real restart would use.

### Solving the Small-File Problem (`src/lakehouse/compaction_worker.py`)

Bronze writes land as micro-batch Parquet files in `date=YYYY-MM-DD/hour=HH` partitions. The `CompactionWorker` scans partitions and consolidates any that exceed the fragmentation threshold (`min_file_count_for_compaction = 4`, or files under the 5 MB small-file threshold in `config/settings.py`): it reads the small files, concatenates them into one dictionary-encoded Parquet file targeting **128 MB**, and atomically unlinks the originals. Measured effect on a 40-file partition: scan latency **13.98 ms → 2.77 ms (5.05x)** and **18.94%** less storage. Row preservation through compaction is covered by `tests/test_compaction.py` — consolidation must never change the data, only its layout.

### Silver Pipeline + Dead-Letter Queue (`src/transforms/silver_pipeline.py`)

Silver conforms Bronze trades into a clean fact table, enforcing the schema contract: non-null trade IDs, `price > 0`, `qty > 0`, unique `trade_id`. Rows that fail are not dropped and not forced in — they are routed to the **Dead-Letter Queue** (`data/dlq/`) with the failure reason attached, so corrupt data stays inspectable. Silver builds are incremental (covered by `tests/test_silver_incremental.py`).

### Gold Marts — Python and dbt (`src/transforms/gold_marts.py`, `dbt/`)

The Gold layer exists in two equivalent forms:

- **Python/DuckDB** (`gold_marts.py`) — the runtime path the dashboard and tests use. Builds `agg_intraday_vwap` (1-minute and 5-minute VWAP buckets), `fct_trade_executions` (one row per trade), `fct_execution_slippage` (fill price vs. arrival mid-quote, per venue), and the `dim_instruments` / `dim_venues` dimensions as Parquet tables.
- **dbt** (`dbt/models/`) — the same marts expressed as versioned SQL models (`staging/stg_market_trades` → `marts/agg_intraday_vwap`, `fct_trade_executions`, `fct_execution_slippage`), with data contracts and invariant tests in `schema.yml`. `dbt build` passes 17/17. The point of the duplication is reviewability: the transformations exist both as executable pipeline code and as declarative SQL with tests, and both are verified.

VWAP is computed the standard way throughout: `Σ(price × qty) / Σ(qty)`.

### Optional Kafka Ingestion (`src/ingestion/kafka_consumer.py`)

The core pipeline runs on an in-process `StreamBuffer`, so it needs no broker to run or test. For a broker-backed deployment, `kafka_consumer.py` adapts a Redpanda/Kafka topic into the same buffer — and **commits offsets only after a message has been processed**, never on receipt. That ordering is what keeps the broker path at-least-once at the transport and exactly-once at the lakehouse: a crash before commit replays the message, and the deduplicator absorbs the replay.

---

## 🛠️ System Architecture Diagram

```
┌──────────────────────────────────────────────────────────────────┐
│                      Market Feed Generator                       │
│ 5 venues · 10 symbols · arrival pricing + slippage               │
└──────────────────────────────────────────────────────────────────┘
                                 │
                                 ▼
┌──────────────────────────────────────────────────────────────────┐
│                         Anomaly Injector                         │
│ 4% duplicates · 3% late (45-120s lag) · 0.5% malformed           │
└──────────────────────────────────────────────────────────────────┘
                                 │
                                 ▼
┌──────────────────────────────────────────────────────────────────┐
│     StreamBuffer  (or Redpanda/Kafka via kafka_consumer.py)      │
│ micro-batches · consumer lag / backpressure metrics              │
└──────────────────────────────────────────────────────────────────┘
                                 │
                                 ▼
┌──────────────────────────────────────────────────────────────────┐
│                     Watermarked Deduplicator                     │
│ 30s event-time watermark · in-memory seen trade_id set           │
│ duplicates -> rejected · too-late -> quarantined (counted)       │
│ state persisted to disk: save_state() / load_state()             │
└──────────────────────────────────────────────────────────────────┘
                                 │
                                 │ clean, unique, in-window
                                 ▼
┌──────────────────────────────────────────────────────────────────┐
│            BRONZE — append-only Parquet micro-batches            │
│ partitions: date=YYYY-MM-DD / hour=HH                            │
└──────────────────────────────────────────────────────────────────┘
                                 │
                                 ▼
┌──────────────────────────────────────────────────────────────────┐
│                    Compaction Worker (async)                     │
│ >=4 files or <5 MB -> one ~128 MB dictionary-encoded file        │
│ measured: DuckDB scan 13.98 ms -> 2.77 ms (5.05x)                │
└──────────────────────────────────────────────────────────────────┘
                                 │
                                 ▼
┌──────────────────────────────────────────────────────────────────┐
│                  SILVER — conformed trade fact                   │
│ price > 0 · qty > 0 · unique trade_id · non-null IDs             │
│ failures ----------> Dead-Letter Queue (data/dlq/)               │
└──────────────────────────────────────────────────────────────────┘
                                 │
                                 ▼
┌──────────────────────────────────────────────────────────────────┐
│    GOLD — Kimball marts (Python/DuckDB and dbt, same models)     │
│ agg_intraday_vwap (1-min & 5-min) · fct_trade_executions         │
│ fct_execution_slippage · dim_instruments · dim_venues            │
└──────────────────────────────────────────────────────────────────┘
                                 │
                                 ▼
                 DuckDB SQL  ·  Streamlit Dashboard                 

Chaos tests (src/chaos/): an in-process crash simulation and a real
subprocess SIGKILL test both replay the full stream against the
deduplicator and verify, by computed reconciliation, that
committed + quarantined == the original unique trade IDs:
0 duplicates, 0 lost.
```

---

## 📦 Project Directory Structure

```
market-stream-lakehouse/
├── config/
│   └── settings.py                  # Storage paths, compaction thresholds, watermarks, venues
├── src/
│   ├── generator/
│   │   ├── market_feed.py           # Institutional market execution generator (5 venues, 10 symbols)
│   │   └── anomalies.py             # Duplicate / late / malformed event injector (4% / 3% / 0.5%)
│   ├── ingestion/
│   │   ├── stream_buffer.py         # Sharded streaming buffer with consumer lag metrics
│   │   ├── deduplicator.py          # Watermarked idempotent dedup + state persistence (save/load)
│   │   ├── bronze_writer.py         # Append-only micro-batch Parquet writer
│   │   └── kafka_consumer.py        # Optional Redpanda/Kafka consumer (commit after processing)
│   ├── lakehouse/
│   │   ├── storage_manager.py       # Medallion partitioning & layer stats inspector
│   │   ├── compaction_worker.py     # Asynchronous small-file compaction engine
│   │   └── time_travel.py           # Snapshot catalog & time-travel inspection
│   ├── transforms/
│   │   ├── silver_pipeline.py       # Conformed trade fact pipeline + DLQ quarantine
│   │   └── gold_marts.py            # Kimball star-schema marts (VWAP, slippage, dims)
│   ├── analytics/
│   │   └── lakehouse_sql.py         # Zero-copy DuckDB analytical engine with latency profiling
│   └── chaos/
│       ├── chaos_engine.py          # In-process crash simulation & computed reconciliation
│       └── real_crash.py            # Subprocess worker for the real SIGKILL recovery test
├── dbt/
│   ├── dbt_project.yml
│   ├── profiles.yml
│   └── models/
│       ├── schema.yml               # dbt data contracts and invariant tests
│       ├── staging/
│       │   ├── stg_market_trades.sql
│       │   └── silver_trades_source.sql
│       └── marts/
│           ├── agg_intraday_vwap.sql        # 1-min and 5-min VWAP buckets
│           ├── fct_trade_executions.sql
│           └── fct_execution_slippage.sql
├── tests/
│   ├── test_ingestion_watermark.py  # Late-arriving event quarantine
│   ├── test_deduplication.py        # Idempotent replay filtering
│   ├── test_stream_buffer.py        # Buffer lag/backpressure behaviour
│   ├── test_compaction.py           # Small-file compaction & row preservation
│   ├── test_chaos_recovery.py       # Exactly-once under simulated crash (computed reconciliation)
│   ├── test_real_crash_recovery.py  # Exactly-once under a real subprocess SIGKILL
│   ├── test_silver_incremental.py   # Incremental Silver builds
│   ├── test_dimensional_marts.py    # VWAP math, slippage, and DLQ routing
│   └── test_kafka_consumer.py       # Commit-after-processing offset behaviour
├── benchmarks/
│   ├── benchmark_throughput.py      # Saturates pipeline (100k events) & measures EPS/lag
│   └── benchmark_compaction.py      # Scan latency before vs. after compaction
├── docs/
│   └── BENCHMARKS.md                # Full measured benchmark record
├── dashboard.py                     # Streamlit real-time lakehouse telemetry console
├── docker-compose.yml               # Redpanda + MinIO + dashboard stack
├── Dockerfile                       # Dashboard container definition
└── LICENSE                          # MIT
```

---

## ⚡ Full Execution & Deployment Guide

### 1. Setup

```bash
git clone https://github.com/ishaaaan17/market-stream-lakehouse.git
cd market-stream-lakehouse

python -m venv .venv
source .venv/bin/activate        # Linux/macOS
.\.venv\Scripts\activate         # Windows

pip install -r requirements.txt
```

### 2. Run the test suite

```bash
pytest -v
```

14 tests across 9 files — including the real subprocess `SIGKILL` recovery test, which spawns the ingestion worker, kills it mid-stream, restarts it, and verifies the computed reconciliation.

### 3. Run the benchmarks

```bash
python benchmarks/benchmark_throughput.py
python benchmarks/benchmark_compaction.py
```

Expected results are the tables in **Results** above (full record in `docs/BENCHMARKS.md`; absolute EPS will vary by machine, the ratios and counts should not).

### 4. Build the dbt models

```bash
cd dbt && dbt build
```

Expected: **PASS=17** — 3 mart models, 1 staging model, and the schema contract tests.

### 5. Launch the telemetry dashboard

```bash
streamlit run dashboard.py
```

Open [http://localhost:8501](http://localhost:8501) — live market flow, compaction sweeps, worker-kill chaos tests, and DuckDB SQL over the lakehouse.

### 6. Full stack via Docker (optional)

```bash
docker-compose up -d
```

Spins up **Redpanda** on `:19092`, **Redpanda Console** on [http://localhost:8080](http://localhost:8080), **MinIO S3** on [http://localhost:9001](http://localhost:9001), and the **Streamlit dashboard** on [http://localhost:8501](http://localhost:8501).

---

## Known Limitations

- **The feed is simulated.** There is no real exchange data anywhere in this project; the throughput and latency numbers measure this pipeline processing this generator's output, on one Windows machine (Python 3.13). They demonstrate headroom and correctness behaviour, not production market-data performance.
- **Single-node design.** The stream buffer and the deduplicator's seen-ID set live in one process's memory. The set is capped at 250,000 IDs and the watermark window is 30 s: a duplicate replayed *after* its ID has been evicted, or an event later than the window that was never quarantined by an earlier pass, is outside what this design can catch. A distributed deployment would need a shared state store (e.g. RocksDB/Kafka Streams state) — the state persistence hooks (`save_state`/`load_state`) are the seam where that would attach.
- **"Exactly-once" is scoped to this pipeline.** It means: crash + full replay ⇒ the committed lakehouse set is exactly the unique input set, proven by computed reconciliation. It is not a claim about distributed transactions across external systems.
- **Gold logic exists twice.** The Python builder and the dbt models implement the same marts; both are test-verified, but a change to one must be mirrored in the other by hand.
- **The Kafka path is an adapter, not the default.** The core pipeline runs broker-free on the in-process buffer; the Redpanda consumer is exercised by unit tests with a fake source, not by a live-broker integration test in CI.

## License

MIT License. Free for academic, personal, and commercial research.
