"""
Optional Redpanda / Kafka consumer.

Wires a real broker into the pipeline: messages from a trades topic are
deserialised, run through the WatermarkedDeduplicator, accepted trades are
enqueued into the StreamBuffer, and offsets are committed ONLY after a
message has been processed (at-least-once delivery + idempotent dedup =
exactly-once lakehouse state, the same contract the chaos tests prove).

The consumer core (`TradeStreamConsumer`) depends only on a minimal
message-source protocol, so it is fully unit-testable with a fake source
and CI never needs a live broker:

    source: iterable of messages; each message is either
      - a dict (already-decoded trade), or
      - an object with .value() returning bytes/str/dict (kafka-python style)
    source.commit(message) is called after each processed message when the
    source provides it (kafka-python's consumer.commit() takes no message
    argument — both signatures are handled).

For a real broker, use create_kafka_consumer(), which lazily imports
kafka-python (optional dependency, see requirements.txt).
"""
import json
from typing import Any, Iterable, Protocol

from src.ingestion.deduplicator import WatermarkedDeduplicator
from src.ingestion.stream_buffer import StreamBuffer


class MessageSource(Protocol):
    def __iter__(self) -> Iterable[Any]: ...


class TradeStreamConsumer:
    def __init__(self, source: MessageSource,
                 buffer: StreamBuffer | None = None,
                 deduplicator: WatermarkedDeduplicator | None = None):
        self.source = source
        self.buffer = buffer or StreamBuffer()
        self.dedup = deduplicator or WatermarkedDeduplicator()
        self.metrics = {"received": 0, "accepted": 0, "rejected": 0,
                        "undecodable": 0, "offsets_committed": 0}

    @staticmethod
    def _extract(message: Any) -> dict | None:
        """Normalises a broker message into a trade dict, or None."""
        value = message
        if hasattr(message, "value") and callable(message.value):
            value = message.value()
        if isinstance(value, (bytes, bytearray)):
            value = value.decode("utf-8")
        if isinstance(value, str):
            try:
                value = json.loads(value)
            except json.JSONDecodeError:
                return None
        return value if isinstance(value, dict) else None

    def _commit(self, message: Any) -> None:
        commit = getattr(self.source, "commit", None)
        if commit is None:
            return
        try:
            commit(message)          # fake / per-message style
        except TypeError:
            commit()                 # kafka-python style: commit() no args
        self.metrics["offsets_committed"] += 1

    def consume(self, max_messages: int | None = None) -> dict[str, Any]:
        """Consumes up to max_messages (or until the source is exhausted).

        Returns run metrics. Offsets are committed per message, after
        processing — never before.
        """
        for message in self.source:
            if max_messages is not None and self.metrics["received"] >= max_messages:
                break
            self.metrics["received"] += 1
            trade = self._extract(message)
            if trade is None:
                self.metrics["undecodable"] += 1
                self._commit(message)
                continue
            accepted, rejected = self.dedup.process_batch([trade])
            if accepted:
                self.buffer.enqueue_many(accepted)
                self.metrics["accepted"] += len(accepted)
            if rejected:
                self.metrics["rejected"] += len(rejected)
            self._commit(message)
        return dict(self.metrics)


def create_kafka_consumer(topic: str, bootstrap_servers: str = "localhost:19092",
                          group_id: str = "marketstream-lakehouse",
                          buffer: StreamBuffer | None = None,
                          deduplicator: WatermarkedDeduplicator | None = None) -> TradeStreamConsumer:
    """Builds a TradeStreamConsumer backed by a real Kafka/Redpanda broker.

    Requires the optional kafka-python package:
        pip install kafka-python
    """
    try:
        from kafka import KafkaConsumer
    except ImportError as e:
        raise RuntimeError(
            "kafka-python is not installed. Install the optional broker "
            "dependency with: pip install kafka-python"
        ) from e
    raw = KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap_servers,
        group_id=group_id,
        enable_auto_commit=False,  # this module commits after processing
        auto_offset_reset="earliest",
    )
    return TradeStreamConsumer(raw, buffer=buffer, deduplicator=deduplicator)
