"""
Unit tests for deterministic stream deduplication.
"""
from datetime import datetime, timezone
import pytest
from src.ingestion.deduplicator import WatermarkedDeduplicator

def test_idempotent_deduplication():
    dedup = WatermarkedDeduplicator(watermark_delay_seconds=60)
    now = datetime.now(timezone.utc).isoformat()

    event = {
        "trade_id": "TRADE_XYZ_123",
        "ticker": "NVDA",
        "event_timestamp": now,
        "execution_price": 130.50
    }

    # First delivery: Must be accepted
    ok1, reason1 = dedup.process_event(event)
    assert ok1 is True
    assert reason1 == "ACCEPTED"

    # Second delivery (duplicate): Must be rejected idempotently
    ok2, reason2 = dedup.process_event(event)
    assert ok2 is False
    assert reason2 == "DUPLICATE_TRADE_ID"

    # Third delivery (duplicate): Still rejected
    ok3, reason3 = dedup.process_event(event)
    assert ok3 is False
    assert reason3 == "DUPLICATE_TRADE_ID"

    metrics = dedup.get_metrics()
    assert metrics["accepted_events"] == 1
    assert metrics["deduplicated_events"] == 2

def test_invalid_timestamp_is_quarantined_not_accepted_as_now():
    dedup = WatermarkedDeduplicator(watermark_delay_seconds=60)
    bad = {"trade_id": "T_BAD_TS", "ticker": "AAPL",
           "event_timestamp": "not-a-timestamp", "execution_price": 100.0}
    ok, reason = dedup.process_event(bad)
    assert ok is False
    assert reason == "INVALID_EVENT_TIMESTAMP"
    assert dedup.get_metrics()["invalid_timestamp_quarantined"] == 1
    # The corrupt id must not poison the dedup state
    assert "T_BAD_TS" not in dedup.seen_ids

def test_state_persistence_round_trip(tmp_path):
    dedup = WatermarkedDeduplicator(watermark_delay_seconds=60)
    now = datetime.now(timezone.utc).isoformat()
    ev = {"trade_id": "T_PERSIST", "ticker": "NVDA",
          "event_timestamp": now, "execution_price": 130.0}
    assert dedup.process_event(ev)[0] is True
    state_file = tmp_path / "dedup_state.json"
    dedup.save_state(state_file)

    revived = WatermarkedDeduplicator(watermark_delay_seconds=60)
    assert revived.load_state(state_file) is True
    # After a restart, the same trade is still recognised as a duplicate
    ok, reason = revived.process_event(ev)
    assert ok is False and reason == "DUPLICATE_TRADE_ID"
