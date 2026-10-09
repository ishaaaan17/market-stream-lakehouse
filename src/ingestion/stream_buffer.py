"""
Thread-safe sharded streaming buffer.
Simulates a distributed broker consumer group with queue depth and consumer lag tracking.
"""
import time
from collections import deque
from threading import Lock
from typing import Any

class StreamBuffer:
    def __init__(self, maxsize: int = 500_000):
        self.maxsize = maxsize
        self._queue: deque[dict[str, Any]] = deque(maxlen=maxsize)
        self._lock = Lock()
        self.total_enqueued = 0
        self.total_dequeued = 0
        self.start_time = time.time()

    def enqueue_many(self, events: list[dict[str, Any]]) -> int:
        """Enqueues a batch of events thread-safely."""
        with self._lock:
            for ev in events:
                self._queue.append(ev)
            count = len(events)
            self.total_enqueued += count
            return count

    def dequeue_batch(self, batch_size: int) -> list[dict[str, Any]]:
        """Drains up to batch_size events."""
        with self._lock:
            batch = []
            while self._queue and len(batch) < batch_size:
                batch.append(self._queue.popleft())
            self.total_dequeued += len(batch)
            return batch

    @property
    def lag(self) -> int:
        """Current consumer group lag (unprocessed events waiting in queue)."""
        with self._lock:
            return len(self._queue)

    def stats(self) -> dict[str, Any]:
        """Returns buffer throughput and lag metrics."""
        elapsed = max(0.001, time.time() - self.start_time)
        return {
            "queue_depth_lag": self.lag,
            "total_enqueued": self.total_enqueued,
            "total_dequeued": self.total_dequeued,
            "ingestion_rate_eps": round(self.total_enqueued / elapsed, 2),
            "drain_rate_eps": round(self.total_dequeued / elapsed, 2),
        }
