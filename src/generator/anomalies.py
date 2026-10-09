"""
Anomalies and chaos stream injection module.
Generates out-of-order ticks, network duplicate events, and malformed records
to test stream watermarking, idempotent deduplication, and DLQ quarantine.
"""
import copy
import random
from datetime import datetime, timezone, timedelta
from typing import Any

class AnomalyInjector:
    def __init__(self, duplicate_rate: float = 0.05, late_arrival_rate: float = 0.05, malformed_rate: float = 0.01):
        self.duplicate_rate = duplicate_rate
        self.late_arrival_rate = late_arrival_rate
        self.malformed_rate = malformed_rate

    def inject_chaos(self, batch: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], dict[str, int]]:
        """
        Injects duplicates, late-arriving events, and malformed schemas into a clean batch.
        Returns the mutated batch and injection telemetry metrics.
        """
        mutated: list[dict[str, Any]] = []
        metrics = {
            "injected_duplicates": 0,
            "injected_late_arrivals": 0,
            "injected_malformed": 0,
        }

        now = datetime.now(timezone.utc)

        for event in batch:
            event_copy = copy.deepcopy(event)
            r = random.random()

            # 1. Late-arriving event (event_time lagging behind by 45 to 120 seconds)
            if r < self.late_arrival_rate:
                lag_seconds = random.randint(45, 120)
                late_time = now - timedelta(seconds=lag_seconds)
                event_copy["event_timestamp"] = late_time.isoformat()
                event_copy["is_anomaly"] = True
                metrics["injected_late_arrivals"] += 1
                mutated.append(event_copy)

            # 2. Malformed schema (negative price or missing primary key) for DLQ
            elif r < (self.late_arrival_rate + self.malformed_rate):
                malformed_type = random.choice(["negative_price", "missing_ticker", "zero_qty"])
                if malformed_type == "negative_price":
                    event_copy["execution_price"] = -99.99
                elif malformed_type == "missing_ticker":
                    event_copy["ticker"] = ""
                elif malformed_type == "zero_qty":
                    event_copy["quantity"] = 0
                event_copy["is_anomaly"] = True
                metrics["injected_malformed"] += 1
                mutated.append(event_copy)

            else:
                mutated.append(event_copy)

            # 3. Duplicate event injection (re-emitting same trade_id with delay)
            if random.random() < self.duplicate_rate:
                dup_event = copy.deepcopy(event_copy)
                dup_event["is_anomaly"] = True
                mutated.append(dup_event)
                metrics["injected_duplicates"] += 1

        # Shuffle slightly to emulate non-deterministic network arrival
        random.shuffle(mutated)
        return mutated, metrics
