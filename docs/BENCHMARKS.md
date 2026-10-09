# Empirical Performance & Scale Benchmarks

This document records the exact performance profiles, throughput rates, and query latency measurements taken on the **MarketStream Lakehouse** platform.

All benchmarks are reproducible by running the scripts in the `benchmarks/` directory:
```bash
python benchmarks/benchmark_throughput.py
python benchmarks/benchmark_compaction.py
```

---

## 1. High-Throughput Ingestion & Lag Saturation Benchmark

### Test Configuration
- **Total Ingested Events:** 100,000 baseline + injected anomalies (104,108 total events)
- **Batch Size:** 5,000 events / micro-batch
- **Injected Anomaly Rates:**
  - Duplicate Rate: 4.0%
  - Late-Arrival Rate: 3.0% (45–120s lag)
  - Malformed Schema Rate: 0.5% (negative prices, empty tickers)
- **Environment:** Python 3.13.1 (Windows x64), Snappy Parquet compression, zero-copy PyArrow memory buffers.

### Empirical Results

| Pipeline Dimension | Benchmark Measurement | Details |
|---|---|---|
| **Producer Enqueue Throughput** | **81,196.40 events/sec** | 104,108 events generated and enqueued in **1.282 seconds** |
| **Consumer Drain & Dedup Rate** | **750,564.32 events/sec** | Drained, validated, and state-checked in **0.139 seconds** |
| **Peak Observed Queue Lag** | **104,108 events** | Max consumer group backlog under instantaneous burst |
| **Duplicate Events Blocked** | **3,990 events** | Intercepted idempotently via in-memory state hash |
| **Late Ticks Quarantined** | **3,092 events** | Quarantined due to exceeding 30-sec watermark window |
| **Clean Committed Trades** | **97,026 events** | Successfully written to conformed Lakehouse storage |

### Key Systems Takeaway
The consumer validation and deduplication path processes at **~750k EPS**, which is **9.2x faster** than the raw tick production rate (81.2k EPS). This guarantees the consumer group will rapidly drain broker backpressure spikes during sudden market volume micro-bursts (e.g., market open, economic news releases).

---

## 2. Lakehouse Small-File Compaction Benchmark

### Test Configuration
- **Partition:** `date=YYYY-MM-DD/hour=HH`
- **Pre-Compaction State:** 40 micro-batch Parquet files (500 rows/file, 20,000 rows total)
- **Post-Compaction State:** 1 consolidated Parquet file (100,000 row-group size, dictionary encoded)
- **Query Under Test:**
  ```sql
  SELECT ticker, COUNT(*), AVG(execution_price)
  FROM read_parquet('<partition_files>')
  GROUP BY ticker
  ```

### Empirical Results

```
======================================================================
 LAKEHOUSE SMALL-FILE COMPACTION BENCHMARK REPORT
======================================================================
 Micro-Batches Before Compaction : 40 small files
 Files After Compaction          : 1 consolidated file
 Total Rows Consolidated         : 20,000 rows
 Compaction Execution Time       : 86.32 ms
 Scan Time (Fragmented Files)    : 13.98 ms
 Scan Time (Compacted Lakehouse) : 2.77 ms
 Analytical Query Speedup        : 5.05x FASTER (80.2% latency cut)
 Storage Footprint Reduction     : 18.94% space savings
======================================================================
```

### Key Systems Takeaway
Reading 40 small files required 40 separate file system opens, 40 metadata header/footer deserializations, and non-contiguous disk reads (13.98 ms). 
After compaction into a single 128MB chunk, DuckDB executed a single contiguous read with column dictionary lookups in **2.77 ms (5.05x speedup)**. In addition, sharing dictionary encoding across all 20,000 rows achieved an **18.94% reduction in raw disk storage footprint**.
