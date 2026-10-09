"""
Configuration and settings for market-stream-lakehouse.
Centralizes paths, partition schemas, watermarks, and compaction thresholds.
"""
from pathlib import Path
from pydantic import BaseModel, Field

PROJECT_ROOT = Path(__file__).resolve().parent.parent

class LakehouseConfig(BaseModel):
    # Base storage directories
    data_dir: Path = Field(default=PROJECT_ROOT / "data")
    bronze_dir: Path = Field(default=PROJECT_ROOT / "data" / "bronze")
    silver_dir: Path = Field(default=PROJECT_ROOT / "data" / "silver")
    gold_dir: Path = Field(default=PROJECT_ROOT / "data" / "gold")
    dlq_dir: Path = Field(default=PROJECT_ROOT / "data" / "dlq")

    # Ingestion & Streaming properties
    default_batch_size: int = 5_000
    watermark_delay_seconds: int = 30
    flush_interval_seconds: float = 1.0

    # Compaction Worker settings (Solving the Small-File Problem)
    # Files smaller than target_file_size_bytes or partitions with > min_file_count_for_compaction
    min_file_count_for_compaction: int = 4
    target_compacted_file_size_mb: int = 128
    small_file_threshold_bytes: int = 5 * 1024 * 1024  # 5 MB

    # Realistic Market Assets
    tickers: list[str] = [
        "AAPL", "NVDA", "MSFT", "GOOGL", "AMZN",
        "META", "TSLA", "BRK.B", "JPM", "V"
    ]
    venues: list[str] = ["XNAS", "XNYS", "BATS", "ARCA", "IEXG"]
    order_types: list[str] = ["LIMIT", "MARKET", "IOC", "POST_ONLY"]
    sides: list[str] = ["BUY", "SELL"]

config = LakehouseConfig()
