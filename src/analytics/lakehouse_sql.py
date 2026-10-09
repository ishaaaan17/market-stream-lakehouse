"""
High-performance analytical SQL query engine over Lakehouse Parquet tiers.
Uses zero-copy DuckDB execution with query profiling and latency tracking.
"""
import time
from typing import Any
import duckdb
import pandas as pd
from config.settings import config
from src.lakehouse.storage_manager import StorageManager

class LakehouseQueryEngine:
    def __init__(self, storage_manager: StorageManager | None = None):
        self.storage = storage_manager or StorageManager()
        self.con = duckdb.connect(":memory:")

    def query(self, sql: str) -> tuple[pd.DataFrame, float]:
        """Executes analytical SQL query and returns (dataframe, elapsed_ms)."""
        t0 = time.perf_counter()
        df = self.con.execute(sql).df()
        elapsed_ms = round((time.perf_counter() - t0) * 1000, 2)
        return df, elapsed_ms

    def get_vwap_curve(self, ticker: str = "AAPL") -> pd.DataFrame:
        """Retrieves intraday VWAP curve for a specified instrument."""
        vwap_path = config.gold_dir / "agg_intraday_vwap.parquet"
        if not vwap_path.exists():
            return pd.DataFrame()

        sql = f"""
            SELECT time_window_1min, total_shares_traded, vwap_price, avg_arrival_price, avg_slippage_bps
            FROM read_parquet('{vwap_path.as_posix()}')
            WHERE ticker = '{ticker}'
            ORDER BY time_window_1min ASC
        """
        df, _ = self.query(sql)
        return df

    def get_venue_liquidity_summary(self) -> pd.DataFrame:
        """Summarizes traded liquidity and slippage across execution venues."""
        venue_path = config.gold_dir / "dim_venues.parquet"
        if not venue_path.exists():
            return pd.DataFrame()

        sql = f"""
            SELECT venue, execution_count, total_volume_traded, avg_venue_slippage_bps
            FROM read_parquet('{venue_path.as_posix()}')
            ORDER BY total_volume_traded DESC
        """
        df, _ = self.query(sql)
        return df

    def benchmark_scan_latency(self, layer: str = "bronze") -> dict[str, Any]:
        """
        Measures exact scan latency over a layer's Parquet files.
        Used to prove query acceleration before vs. after file compaction!
        """
        files = self.storage.get_partition_files(layer)
        if not files:
            return {"file_count": 0, "scan_latency_ms": 0.0, "row_count": 0}

        paths_str = [f.as_posix() for f in files]
        t0 = time.perf_counter()
        row_count = self.con.execute(f"SELECT COUNT(*) FROM read_parquet({paths_str})").fetchone()[0]
        elapsed_ms = round((time.perf_counter() - t0) * 1000, 2)

        return {
            "layer": layer,
            "file_count": len(files),
            "row_count": row_count,
            "scan_latency_ms": elapsed_ms
        }
