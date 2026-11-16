"""Kafka/Redpanda consumer tests — a fake in-memory source implements the
same protocol, so no live broker is needed in CI."""
import json
from datetime import datetime, timezone

from src.ingestion.kafka_consumer import TradeStreamConsumer
from src.ingestion.stream_buffer import StreamBuffer


class FakeMessage:
    def __init__(self, payload):
        self._payload = payload

    def value(self):
        return self._payload


class FakeConsumer:
    """Iterable message source with kafka-style commit recording."""

    def __init__(self, messages):
        self._messages = messages
        self.committed = []

    def __iter__(self):
        return iter(self._messages)

    def commit(self, message=None):
        self.committed.append(message)


def _trade(tid):
    return {"trade_id": tid, "ticker": "AAPL",
            "event_timestamp": datetime.now(timezone.utc).isoformat(),
            "execution_price": 225.0, "quantity": 100}


def test_consumer_processes_dedups_and_commits_after_processing():
    t1, t2 = _trade("K1"), _trade("K2")
    messages = [
        FakeMessage(json.dumps(t1).encode()),   # bytes payload
        FakeMessage(json.dumps(t1).encode()),   # duplicate delivery
        FakeMessage(t2),                        # already-decoded dict
        FakeMessage(b"not json at all"),        # undecodable -> skipped, still committed
    ]
    source = FakeConsumer(messages)
    buffer = StreamBuffer()
    consumer = TradeStreamConsumer(source, buffer=buffer)

    metrics = consumer.consume()

    assert metrics["received"] == 4
    assert metrics["accepted"] == 2          # K1 once, K2 once
    assert metrics["rejected"] == 1          # the duplicate
    assert metrics["undecodable"] == 1
    assert metrics["offsets_committed"] == 4  # every message committed, after processing
    assert len(source.committed) == 4

    drained = buffer.dequeue_batch(10)
    assert {e["trade_id"] for e in drained} == {"K1", "K2"}


def test_consumer_max_messages_limit():
    source = FakeConsumer([FakeMessage(_trade(f"K{i}")) for i in range(10)])
    consumer = TradeStreamConsumer(source, buffer=StreamBuffer())
    metrics = consumer.consume(max_messages=3)
    assert metrics["received"] == 3
    assert metrics["accepted"] == 3
