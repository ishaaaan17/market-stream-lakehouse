"""
Unit tests for event-time watermarking and late-arrival quarantine.
"""
from datetime import datetime, timezone, timedelta
import pytest
from src.ingestion.deduplicator import WatermarkedDeduplicator

def test_watermark_advancement_and_late_arrival():
    dedup = WatermarkedDeduplicator(watermark_delay_seconds=30)
    base_time = datetime(2026, 10, 9, 12, 0, 0, tzinfo=timezone.utc)

    # Event 1 at T=0
    e1 = {
        "trade_id": "T1",
        "ticker": "AAPL",
        "event_timestamp": base_time.isoformat(),
        "execution_price": 220.0
    }
    ok1, reason1 = dedup.process_event(e1)
    assert ok1 is True
    assert reason1 == "ACCEPTED"
    assert dedup.current_watermark == base_time

    # Event 2 advances watermark to T+60s
    time_future = base_time + timedelta(seconds=60)
    e2 = {
        "trade_id": "T2",
        "ticker": "AAPL",
        "event_timestamp": time_future.isoformat(),
        "execution_price": 221.0
    }
    ok2, reason2 = dedup.process_event(e2)
    assert ok2 is True
    assert dedup.current_watermark == time_future

    # Event 3 arrives late: timestamp is T+10s (which is >30s behind current watermark T+60s)
    late_time = base_time + timedelta(seconds=10)
    e3 = {
        "trade_id": "T3",
        "ticker": "AAPL",
        "event_timestamp": late_time.isoformat(),
        "execution_price": 220.5
    }
    ok3, reason3 = dedup.process_event(e3)
    assert ok3 is False
    assert reason3 == "EXCEEDED_EVENT_TIME_WATERMARK"
    assert dedup.get_metrics()["late_events_quarantined"] == 1
