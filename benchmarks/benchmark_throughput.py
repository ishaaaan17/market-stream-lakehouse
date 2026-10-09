"""
High-throughput ingestion & consumer lag saturation benchmark.
Pushes 100,000 market execution events through streaming buffers and deduplication,
measuring events-per-second (EPS), queue depth, and commit latencies.
"""
import sys
import time
from pathlib import Path
root_dir = Path(__file__).resolve().parent.parent
if str(root_dir) not in sys.path:
    sys.path.insert(0, str(root_dir))

from src.generator.market_feed import MarketFeedGenerator
from src.generator.anomalies import AnomalyInjector
from src.ingestion.stream_buffer import StreamBuffer
from src.ingestion.deduplicator import WatermarkedDeduplicator

def run_throughput_benchmark(total_events: int = 100_000, batch_size: int = 5_000):
    print(f"\n[BENCHMARK] Starting Lakehouse Ingestion Saturation Benchmark ({total_events:,} events)...")

    feed = MarketFeedGenerator(seed=42)
    injector = AnomalyInjector(duplicate_rate=0.04, late_arrival_rate=0.03, malformed_rate=0.005)
    buffer = StreamBuffer(maxsize=total_events * 2)
    dedup = WatermarkedDeduplicator(watermark_delay_seconds=30)

    # 1. Measure Producer Throughput
    t_gen_start = time.perf_counter()
    raw_events = feed.generate_batch(total_events)
    mutated_events, injection_stats = injector.inject_chaos(raw_events)
    buffer.enqueue_many(mutated_events)
    t_gen_end = time.perf_counter()

    producer_elapsed = t_gen_end - t_gen_start
    producer_eps = round(len(mutated_events) / producer_elapsed, 2)

    # 2. Measure Consumer Drain & Deduplication Throughput
    t_drain_start = time.perf_counter()
    total_accepted = 0
    total_quarantined = 0
    max_observed_lag = buffer.lag

    while buffer.lag > 0:
        batch = buffer.dequeue_batch(batch_size)
        accepted, rejected = dedup.process_batch(batch)
        total_accepted += len(accepted)
        total_quarantined += len(rejected)

    t_drain_end = time.perf_counter()
    consumer_elapsed = t_drain_end - t_drain_start
    consumer_eps = round(len(mutated_events) / consumer_elapsed, 2)

    print("=" * 70)
    print(" INGESTION PIPELINE SATURATION BENCHMARKS")
    print("=" * 70)
    print(f" Producer Throughput      : {producer_eps:,.2f} events/sec ({len(mutated_events):,} events in {producer_elapsed:.3f}s)")
    print(f" Consumer & Deduplicator  : {consumer_eps:,.2f} events/sec (drained & validated in {consumer_elapsed:.3f}s)")
    print(f" Peak Queue Depth Lag     : {max_observed_lag:,} events")
    print(f" Duplicate Events Dropped : {dedup.get_metrics()['deduplicated_events']:,} duplicates")
    print(f" Late Ticks Quarantined   : {dedup.get_metrics()['late_events_quarantined']:,} late events")
    print(f" Total Clean Accepted     : {total_accepted:,} events")
    print("=" * 70)

    return {
        "producer_eps": producer_eps,
        "consumer_eps": consumer_eps,
        "total_events": len(mutated_events),
        "total_accepted": total_accepted,
    }

if __name__ == "__main__":
    run_throughput_benchmark()
