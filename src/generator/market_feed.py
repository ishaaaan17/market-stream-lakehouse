"""
High-throughput market feed generator.
Simulates institutional equity order executions with realistic tick dynamics,
micro-bursts, arrival prices, and execution slippage.
"""
import uuid
import time
import random
from datetime import datetime, timezone, timedelta
from typing import Iterator
import pyarrow as pa
from config.settings import config

class MarketFeedGenerator:
    def __init__(self, seed: int = 42):
        random.seed(seed)
        self.seq_counter = 0
        self.base_prices = {
            "AAPL": 225.50,
            "NVDA": 132.80,
            "MSFT": 448.20,
            "GOOGL": 178.60,
            "AMZN": 186.40,
            "META": 585.10,
            "TSLA": 250.30,
            "BRK.B": 452.00,
            "JPM": 218.40,
            "V": 282.10,
        }
        self.current_prices = dict(self.base_prices)
        self.counterparties = [f"CPTY_{i:03d}" for i in range(1, 25)]
        self.traders = [f"DESK_ALG_{i:02d}" for i in range(1, 10)]

    def _tick_price(self, ticker: str) -> tuple[float, float]:
        """
        Simulates geometric price drift and returns (arrival_mid_price, execution_price).
        Includes realistic execution slippage depending on side and market impact.
        """
        curr = self.current_prices[ticker]
        drift = random.gauss(0, 0.0012) * curr
        arrival_mid = round(curr + drift, 2)
        self.current_prices[ticker] = max(1.0, arrival_mid)

        # Micro slippage: Execution price deviated slightly from arrival mid
        spread = round(max(0.01, arrival_mid * 0.0003), 2)
        slippage_direction = 1 if random.random() > 0.48 else -1
        execution_price = round(arrival_mid + (slippage_direction * (spread / 2.0)), 2)

        return arrival_mid, max(0.01, execution_price)

    def generate_event(
        self,
        timestamp_override: datetime | None = None,
        trade_id_override: str | None = None
    ) -> dict:
        """Generates a single institutional execution report."""
        self.seq_counter += 1
        ticker = random.choice(config.tickers)
        arrival_price, exec_price = self._tick_price(ticker)
        event_time = timestamp_override or datetime.now(timezone.utc)
        trade_id = trade_id_override or f"TRD_{uuid.uuid4().hex[:12].upper()}"

        side = random.choice(config.sides)
        qty = random.choice([100, 200, 500, 1000, 2500, 5000, 10000])

        return {
            "trade_id": trade_id,
            "sequence_number": self.seq_counter,
            "ticker": ticker,
            "event_timestamp": event_time.isoformat(),
            "arrival_price": arrival_price,
            "execution_price": exec_price,
            "quantity": qty,
            "notional_value": round(exec_price * qty, 2),
            "side": side,
            "order_type": random.choice(config.order_types),
            "venue": random.choice(config.venues),
            "counterparty_id": random.choice(self.counterparties),
            "trader_id": random.choice(self.traders),
            "is_anomaly": False,
        }

    def generate_batch(self, batch_size: int, timestamp: datetime | None = None) -> list[dict]:
        """Generates a micro-batch of execution reports."""
        base_time = timestamp or datetime.now(timezone.utc)
        return [
            self.generate_event(timestamp_override=base_time + timedelta(microseconds=i * 10))
            for i in range(batch_size)
        ]

    def stream_batches(self, batch_size: int, total_batches: int) -> Iterator[list[dict]]:
        """Streams batches over time."""
        for _ in range(total_batches):
            yield self.generate_batch(batch_size)
