"""Unit tests for StreamBuffer bounded-capacity behaviour."""
from src.ingestion.stream_buffer import StreamBuffer


def _ev(i):
    return {"trade_id": f"T{i}"}


def test_enqueue_returns_actual_count_and_never_silently_drops():
    buf = StreamBuffer(maxsize=5)
    accepted = buf.enqueue_many([_ev(i) for i in range(5)])
    assert accepted == 5
    assert buf.dropped_count == 0

    # Buffer full: the next 3 must be REFUSED and counted, not evict oldest
    accepted2 = buf.enqueue_many([_ev(i) for i in range(5, 8)])
    assert accepted2 == 0
    assert buf.dropped_count == 3
    assert buf.stats()["total_dropped"] == 3
    assert buf.stats()["total_enqueued"] == 5

    # Oldest events are still there (no silent eviction)
    batch = buf.dequeue_batch(5)
    assert [e["trade_id"] for e in batch] == ["T0", "T1", "T2", "T3", "T4"]

    # After draining, capacity frees up again
    assert buf.enqueue_many([_ev(99)]) == 1


def test_partial_accept_when_nearly_full():
    buf = StreamBuffer(maxsize=4)
    buf.enqueue_many([_ev(1), _ev(2), _ev(3)])
    assert buf.enqueue_many([_ev(4), _ev(5), _ev(6)]) == 1
    assert buf.dropped_count == 2
    assert buf.lag == 4
