"""
Bronze layer append-only micro-batch writer.
Persists raw ingested market trades to partitioned Parquet files.
Simulates real-world micro-batch streaming fragmentation.
"""
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
import pyarrow as pa
import pyarrow.parquet as pq
from config.settings import config

class BronzeWriter:
    def __init__(self, output_dir: Path | None = None):
        self.output_dir = output_dir or config.bronze_dir
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.schema = pa.schema([
            ("trade_id", pa.string()),
            ("sequence_number", pa.int64()),
            ("ticker", pa.string()),
            ("event_timestamp", pa.string()),
            ("arrival_price", pa.float64()),
            ("execution_price", pa.float64()),
            ("quantity", pa.int64()),
            ("notional_value", pa.float64()),
            ("side", pa.string()),
            ("order_type", pa.string()),
            ("venue", pa.string()),
            ("counterparty_id", pa.string()),
            ("trader_id", pa.string()),
            ("is_anomaly", pa.bool_()),
            ("ingestion_timestamp", pa.string()),
        ])

    def write_micro_batch(self, events: list[dict[str, Any]]) -> Path | None:
        """
        Writes a micro-batch of events as a raw Parquet file into Bronze partition.
        Partitioning: date=YYYY-MM-DD/hour=HH/batch_<uuid>.parquet
        """
        if not events:
            return None

        now = datetime.now(timezone.utc)
        date_str = now.strftime("%Y-%m-%d")
        hour_str = now.strftime("%H")

        partition_dir = self.output_dir / f"date={date_str}" / f"hour={hour_str}"
        partition_dir.mkdir(parents=True, exist_ok=True)

        batch_id = uuid.uuid4().hex[:8]
        filename = partition_dir / f"bronze_batch_{batch_id}.parquet"

        ingestion_ts = now.isoformat()
        records = []
        for ev in events:
            rec = dict(ev)
            rec["ingestion_timestamp"] = ingestion_ts
            records.append(rec)

        table = pa.Table.from_pylist(records, schema=self.schema)
        pq.write_table(table, filename, compression="snappy")
        return filename
