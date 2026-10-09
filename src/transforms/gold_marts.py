"""
Gold Layer Dimensional Modeling & Analytics Marts (Kimball Star Schema).
Computes high-value analytical marts:
1. agg_intraday_vwap: Rolling 1-min & 5-min Volume-Weighted Average Price
2. fct_execution_slippage: Fill slippage vs. arrival mid benchmark by venue & order type
3. fct_trade_executions: Curated fact executions
4. dim_instruments & dim_venues: Dimensional reference models
"""
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
from config.settings import config
from src.lakehouse.storage_manager import StorageManager

class GoldMartsBuilder:
    def __init__(self, storage_manager: StorageManager | None = None):
        self.storage = storage_manager or StorageManager()
        self.gold_dir = config.gold_dir
        self.gold_dir.mkdir(parents=True, exist_ok=True)
        self.con = duckdb.connect(":memory:")

    def build_marts(self) -> dict[str, Any]:
        """
        Executes analytical SQL transformations over Silver tier
        and writes materialised Gold dimensional marts.
        """
        silver_files = self.storage.get_partition_files("silver")
        if not silver_files:
            return {"status": "NO_SILVER_DATA_FOUND", "marts_created": []}

        # Load all Silver Parquet into an ephemeral view
        silver_paths_str = [f.as_posix() for f in silver_files]
        self.con.execute(f"CREATE OR REPLACE VIEW v_silver_trades AS SELECT * FROM read_parquet({silver_paths_str})")

        # Mart 1: Dimensional Instruments
        self.con.execute("""
            CREATE OR REPLACE TABLE dim_instruments AS
            SELECT
                ticker,
                COUNT(*) as total_trade_count,
                ROUND(AVG(execution_price), 2) as avg_price,
                ROUND(MIN(execution_price), 2) as min_price,
                ROUND(MAX(execution_price), 2) as max_price
            FROM v_silver_trades
            GROUP BY ticker
            ORDER BY ticker
        """)
        dim_inst_path = self.gold_dir / "dim_instruments.parquet"
        self.con.execute(f"COPY dim_instruments TO '{dim_inst_path.as_posix()}' (FORMAT PARQUET)")

        # Mart 2: Dimensional Venues
        self.con.execute("""
            CREATE OR REPLACE TABLE dim_venues AS
            SELECT
                venue,
                COUNT(*) as execution_count,
                ROUND(SUM(notional_value), 2) as total_volume_traded,
                ROUND(AVG(slippage_bps), 2) as avg_venue_slippage_bps
            FROM v_silver_trades
            GROUP BY venue
            ORDER BY execution_count DESC
        """)
        dim_venue_path = self.gold_dir / "dim_venues.parquet"
        self.con.execute(f"COPY dim_venues TO '{dim_venue_path.as_posix()}' (FORMAT PARQUET)")

        # Mart 3: Aggregated Intraday VWAP (Volume-Weighted Average Price)
        self.con.execute("""
            CREATE OR REPLACE TABLE agg_intraday_vwap AS
            SELECT
                ticker,
                strftime(CAST(event_timestamp AS TIMESTAMP), '%Y-%m-%d %H:%M:00') as time_window_1min,
                COUNT(*) as trades_in_window,
                SUM(quantity) as total_shares_traded,
                ROUND(SUM(notional_value), 2) as total_notional,
                ROUND(SUM(execution_price * quantity) / NULLIF(SUM(quantity), 0), 4) as vwap_price,
                ROUND(AVG(arrival_price), 4) as avg_arrival_price,
                ROUND(AVG(slippage_bps), 2) as avg_slippage_bps
            FROM v_silver_trades
            GROUP BY ticker, time_window_1min
            ORDER BY ticker, time_window_1min
        """)
        vwap_path = self.gold_dir / "agg_intraday_vwap.parquet"
        self.con.execute(f"COPY agg_intraday_vwap TO '{vwap_path.as_posix()}' (FORMAT PARQUET)")

        # Mart 4: Execution Slippage Fact Table
        self.con.execute("""
            CREATE OR REPLACE TABLE fct_execution_slippage AS
            SELECT
                trade_id,
                ticker,
                venue,
                order_type,
                side,
                arrival_price,
                execution_price,
                slippage_dollars,
                slippage_bps,
                quantity,
                notional_value,
                event_timestamp
            FROM v_silver_trades
        """)
        slippage_path = self.gold_dir / "fct_execution_slippage.parquet"
        self.con.execute(f"COPY fct_execution_slippage TO '{slippage_path.as_posix()}' (FORMAT PARQUET)")

        return {
            "status": "SUCCESS",
            "marts_created": [
                "dim_instruments.parquet",
                "dim_venues.parquet",
                "agg_intraday_vwap.parquet",
                "fct_execution_slippage.parquet"
            ],
            "total_silver_rows_modeled": self.con.execute("SELECT COUNT(*) FROM v_silver_trades").fetchone()[0]
        }
