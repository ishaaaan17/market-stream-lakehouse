"""
Event-time watermarking and deterministic stream deduplication.
Ensures at-least-once streaming delivery achieves Exactly-Once state in the lakehouse.
"""
import json
from datetime import datetime, timezone, timedelta
from pathlib import Path
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
            "invalid_timestamp_quarantined": 0,
            "accepted_events": 0,
        }

    def _parse_time(self, ts_str: str) -> datetime | None:
        """Parses an ISO timestamp. Returns None on failure — a corrupt
        timestamp must be quarantined, never silently replaced with 'now'
        (that would let malformed events pass the watermark check)."""
        try:
            dt = datetime.fromisoformat(ts_str)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt
        except Exception:
            return None

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
        if event_time is None:
            self.metrics["invalid_timestamp_quarantined"] += 1
            return False, "INVALID_EVENT_TIMESTAMP"

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

    # --- State persistence (crash recovery) ---------------------------------
    # Exactly-once after a real crash is only possible if the dedup state
    # survives the process. save_state/load_state persist seen_ids, the
    # watermark and metrics to a JSON file on disk.

    def save_state(self, path: Path | str) -> None:
        state = {
            "seen_ids": {tid: ts.isoformat() for tid, ts in self.seen_ids.items()},
            "current_watermark": self.current_watermark.isoformat(),
            "metrics": dict(self.metrics),
        }
        p = Path(path)
        p.parent.mkdir(parents=True, exist_ok=True)
        tmp = p.with_suffix(p.suffix + ".tmp")
        tmp.write_text(json.dumps(state), encoding="utf-8")
        tmp.replace(p)  # atomic on the same filesystem

    def load_state(self, path: Path | str) -> bool:
        p = Path(path)
        if not p.exists():
            return False
        state = json.loads(p.read_text(encoding="utf-8"))
        self.seen_ids = OrderedDict(
            (tid, datetime.fromisoformat(ts)) for tid, ts in state["seen_ids"].items()
        )
        self.current_watermark = datetime.fromisoformat(state["current_watermark"])
        for k, v in state.get("metrics", {}).items():
            if k in self.metrics:
                self.metrics[k] = v
        return True
