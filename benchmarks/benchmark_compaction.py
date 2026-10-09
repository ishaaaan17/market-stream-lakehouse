"""
Lakehouse Small-File Problem Compaction Benchmark.
Demonstrates the exact performance penalty of uncompacted micro-batches
versus an asynchronously compacted Lakehouse partition using DuckDB scans.
"""
import sys
import time
import shutil
from pathlib import Path
root_dir = Path(__file__).resolve().parent.parent
if str(root_dir) not in sys.path:
    sys.path.insert(0, str(root_dir))

import duckdb
from src.generator.market_feed import MarketFeedGenerator
from src.ingestion.bronze_writer import BronzeWriter
from src.lakehouse.compaction_worker import CompactionWorker

def run_compaction_benchmark(file_count: int = 40, rows_per_file: int = 500):
    print(f"\n[BENCHMARK] Generating {file_count} fragmented micro-batch Parquet files ({file_count * rows_per_file:,} rows)...")

    bench_dir = Path("data/bronze_bench")
    shutil.rmtree(bench_dir, ignore_errors=True)
    bench_dir.mkdir(parents=True, exist_ok=True)

    writer = BronzeWriter(output_dir=bench_dir)
    feed = MarketFeedGenerator(seed=99)

    created_files = []
    for _ in range(file_count):
        batch = feed.generate_batch(rows_per_file)
        f = writer.write_micro_batch(batch)
        created_files.append(f)

    con = duckdb.connect(":memory:")
    paths_before = [f.as_posix() for f in created_files]

    # Benchmark 1: Scan fragmented files
    t0 = time.perf_counter()
    res1 = con.execute(f"SELECT ticker, COUNT(*), AVG(execution_price) FROM read_parquet({paths_before}) GROUP BY ticker").fetchall()
    scan_time_fragmented_ms = round((time.perf_counter() - t0) * 1000, 2)

    # Run compaction
    print(f"[BENCHMARK] Compacting {file_count} small files into an optimized Lakehouse partition...")
    worker = CompactionWorker()
    partition_dir = created_files[0].parent
    compaction_res = worker.compact_partition(partition_dir, created_files)

    # Benchmark 2: Scan compacted partition
    compacted_file = partition_dir / compaction_res["compacted_file"]
    t1 = time.perf_counter()
    res2 = con.execute(f"SELECT ticker, COUNT(*), AVG(execution_price) FROM read_parquet('{compacted_file.as_posix()}') GROUP BY ticker").fetchall()
    scan_time_compacted_ms = round((time.perf_counter() - t1) * 1000, 2)

    speedup = round(scan_time_fragmented_ms / max(0.01, scan_time_compacted_ms), 2)

    print("=" * 70)
    print(" LAKEHOUSE SMALL-FILE COMPACTION BENCHMARK REPORT")
    print("=" * 70)
    print(f" Micro-Batches Before Compaction : {compaction_res['files_before']} small files")
    print(f" Files After Compaction          : {compaction_res['files_after']} consolidated file")
    print(f" Total Rows Consolidated         : {compaction_res['rows_consolidated']:,} rows")
    print(f" Compaction Execution Time       : {compaction_res['compaction_time_ms']} ms")
    print(f" Scan Time (Fragmented Files)    : {scan_time_fragmented_ms} ms")
    print(f" Scan Time (Compacted Lakehouse) : {scan_time_compacted_ms} ms")
    print(f" Analytical Query Speedup        : {speedup}x FASTER")
    print(f" Storage Footprint Reduction     : {compaction_res['compression_savings_pct']}% savings")
    print("=" * 70)

    # Clean up benchmark temp data
    shutil.rmtree(bench_dir, ignore_errors=True)

    return {
        "files_before": compaction_res["files_before"],
        "files_after": 1,
        "fragmented_ms": scan_time_fragmented_ms,
        "compacted_ms": scan_time_compacted_ms,
        "speedup": speedup
    }

if __name__ == "__main__":
    run_compaction_benchmark()
