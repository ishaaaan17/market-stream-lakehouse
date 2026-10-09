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
