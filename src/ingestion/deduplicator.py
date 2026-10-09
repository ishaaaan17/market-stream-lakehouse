"""
Event-time watermarking and deterministic stream deduplication.
Ensures at-least-once streaming delivery achieves Exactly-Once state in the lakehouse.
"""
from datetime import datetime, timezone, timedelta
from typing import Any
from collections import OrderedDict

class WatermarkedDeduplicator:
    def __init__(self, watermark_delay_seconds: int = 30, max_cache_size: int = 250_000):
        self.watermark_delay = timedelta(seconds=watermark_delay_seconds)
        self.max_cache_size = max_cache_size
        self.current_watermark: datetime = datetime.min.replace(tzinfo=timezone.utc)
        self.seen_ids: OrderedDict[str, datetime] = OrderedDict()
        self.metrics = {
            "processed_events": 0,
            "deduplicated_events": 0,
            "late_events_quarantined": 0,
            "accepted_events": 0,
        }

    def _parse_time(self, ts_str: str) -> datetime:
        try:
            dt = datetime.fromisoformat(ts_str)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt
        except Exception:
            return datetime.now(timezone.utc)

    def process_event(self, event: dict[str, Any]) -> tuple[bool, str]:
        """
        Processes a single event through watermark and deduplication checks.
        Returns: (is_accepted, reason)
        """
        self.metrics["processed_events"] += 1
        trade_id = event.get("trade_id")
        if not trade_id:
            return False, "MISSING_PRIMARY_KEY"

        event_time = self._parse_time(event.get("event_timestamp", ""))

        # Update high-water mark monotonically
        if event_time > self.current_watermark:
            self.current_watermark = event_time

        # 1. Deduplication check
        if trade_id in self.seen_ids:
            self.metrics["deduplicated_events"] += 1
            return False, "DUPLICATE_TRADE_ID"

        # 2. Watermark check: events older than (watermark - delay) are late
        watermark_boundary = self.current_watermark - self.watermark_delay
        if event_time < watermark_boundary:
            self.metrics["late_events_quarantined"] += 1
            return False, "EXCEEDED_EVENT_TIME_WATERMARK"

        # Accept event and record to state cache
        self.seen_ids[trade_id] = event_time
        if len(self.seen_ids) > self.max_cache_size:
            # Evict oldest
            self.seen_ids.popitem(last=False)

        self.metrics["accepted_events"] += 1
        return True, "ACCEPTED"

    def process_batch(self, batch: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        """
        Filters batch into (accepted_events, rejected_events).
        """
        accepted = []
        rejected = []
        for event in batch:
            ok, reason = self.process_event(event)
            if ok:
                accepted.append(event)
            else:
                ev_copy = dict(event)
                ev_copy["rejection_reason"] = reason
                rejected.append(ev_copy)
        return accepted, rejected

    def get_metrics(self) -> dict[str, Any]:
        return dict(self.metrics)
