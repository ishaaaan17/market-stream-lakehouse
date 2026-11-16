"""
Chaos Testing & Exactly-Once Semantics Recovery Engine.

This module is the IN-PROCESS SIMULATION: it simulates an ungraceful
consumer failure (worker termination mid-batch) and a broker replay, then
performs a real reconciliation. Every reported number is COMPUTED from
the run — nothing is hardcoded:

- zero duplicates is checked on the committed LIST (len vs len(set)),
- no-data-loss is checked as an exact set equality:
  committed ∪ quarantined == the original unique trade ids,
- phantom records (committed ids that were never in the input) fail it.

For a REAL subprocess SIGKILL test with dedup state persisted to disk,
see src/chaos/real_crash.py (tests/test_real_crash_recovery.py).
"""
import time
from typing import Any
from src.generator.market_feed import MarketFeedGenerator
from src.generator.anomalies import AnomalyInjector
from src.ingestion.stream_buffer import StreamBuffer
from src.ingestion.deduplicator import WatermarkedDeduplicator

class ChaosRecoveryEngine:
    def __init__(self):
        self.feed = MarketFeedGenerator(seed=101)
        self.injector = AnomalyInjector(duplicate_rate=0.08, late_arrival_rate=0.04, malformed_rate=0.01)

    def run_chaos_simulation(self, total_events: int = 25_000, crash_at_event: int = 12_500) -> dict[str, Any]:
        """
        Runs streaming ingestion, simulates a worker crash mid-stream,
        restarts the consumer with stream replay, and reconciles for real.
        """
        # Step 1: Generate original gold-truth events
        raw_events = self.feed.generate_batch(total_events)
        unique_original_ids = {e["trade_id"] for e in raw_events}

        # Step 2: Inject network anomalies (duplicates, late arrivals)
        mutated_events, injection_stats = self.injector.inject_chaos(raw_events)

        # Step 3: Stream Worker Phase 1 (ingests until the simulated crash)
        buffer = StreamBuffer()
        buffer.enqueue_many(mutated_events)

        dedup_worker = WatermarkedDeduplicator(watermark_delay_seconds=60)
        committed_list: list[str] = []       # a LIST on purpose: duplicates must be detectable
        quarantined_ids: set[str] = set()    # ids rejected for a non-duplicate reason

        def _record(accepted, rejected):
            for e in accepted:
                committed_list.append(e["trade_id"])
            for e in rejected:
                if e.get("rejection_reason") != "DUPLICATE_TRADE_ID":
                    quarantined_ids.add(e["trade_id"])

        # Process first half before crash
        batch_1 = buffer.dequeue_batch(crash_at_event)
        accepted_1, rejected_1 = dedup_worker.process_batch(batch_1)
        _record(accepted_1, rejected_1)

        # SIMULATED WORKER CRASH (in-process simulation — for a real
        # SIGKILL of a separate process, see real_crash.py)
        crash_timestamp = time.time()
        # Message broker redelivers remaining events PLUS re-delivers the last 1,000 unacknowledged events
        replayed_overlap = batch_1[-1000:]
        remaining_events = buffer.dequeue_batch(buffer.lag)
        replayed_stream = replayed_overlap + remaining_events

        # Step 4: Worker recovers and consumes the replayed stream
        accepted_2, rejected_2 = dedup_worker.process_batch(replayed_stream)
        _record(accepted_2, rejected_2)

        # Step 5: Reconciliation — all computed, nothing hardcoded
        committed_set = set(committed_list)
        duplicate_records_prevented = dedup_worker.get_metrics()["deduplicated_events"]
        late_events_quarantined = dedup_worker.get_metrics()["late_events_quarantined"]

        is_zero_duplicates = len(committed_list) == len(committed_set)
        # Every original trade must be accounted for: committed or quarantined
        all_unique_preserved = (committed_set | quarantined_ids) == unique_original_ids
        no_phantom_records = committed_set <= unique_original_ids
        data_loss_detected = not all_unique_preserved
        exactly_once_verified = is_zero_duplicates and all_unique_preserved and no_phantom_records

        return {
            "total_input_stream_with_chaos": len(mutated_events),
            "simulated_worker_crash_at": crash_at_event,
            "crash_timestamp": crash_timestamp,
            "replayed_messages_handled": len(replayed_overlap),
            "duplicate_records_intercepted": duplicate_records_prevented,
            "late_events_quarantined": late_events_quarantined,
            "total_clean_trades_committed": len(committed_set),
            "total_unique_quarantined": len(quarantined_ids),
            "is_zero_duplicates": is_zero_duplicates,
            "all_unique_preserved": all_unique_preserved,
            "no_phantom_records": no_phantom_records,
            "exactly_once_verified": exactly_once_verified,
            "data_loss_detected": data_loss_detected,
            "reconciliation_status": (
                "PASSED_ZERO_DUPLICATES_ZERO_DATA_LOSS" if exactly_once_verified
                else "FAILED_RECONCILIATION_MISMATCH"
            ),
        }
