"""
Unit tests for Silver curation, Dead-Letter Queue (DLQ) quarantine, and Gold data marts.
Everything runs against an isolated lakehouse under tmp_path — no test
touches the real data/ directories.
"""
import pyarrow.parquet as pq
import pytest
from src.ingestion.bronze_writer import BronzeWriter
from src.lakehouse.storage_manager import StorageManager
from src.transforms.silver_pipeline import SilverPipeline
from src.transforms.gold_marts import GoldMartsBuilder
from src.analytics.lakehouse_sql import LakehouseQueryEngine


def test_silver_validation_and_dlq_quarantine(tmp_path):
    pipeline = SilverPipeline(storage_manager=StorageManager(base_dir=tmp_path))

    # Valid clean record
    valid_rec = {
        "trade_id": "T_CLEAN_1",
        "ticker": "AAPL",
        "execution_price": 225.00,
        "quantity": 100,
        "arrival_price": 224.95,
        "side": "BUY"
    }
    is_valid, reason, conformed = pipeline.validate_and_conform_record(valid_rec)
    assert is_valid is True
    assert reason == "CLEAN"
    assert conformed["slippage_dollars"] == 0.05
    assert conformed["slippage_bps"] > 0

    # Invalid record 1: Negative execution price
    neg_price_rec = {
        "trade_id": "T_BAD_1",
        "ticker": "AAPL",
        "execution_price": -10.0,
        "quantity": 100
    }
    is_valid, reason, _ = pipeline.validate_and_conform_record(neg_price_rec)
    assert is_valid is False
    assert reason == "NON_POSITIVE_EXECUTION_PRICE"

    # Invalid record 2: Missing ticker
    empty_ticker_rec = {
        "trade_id": "T_BAD_2",
        "ticker": "",
        "execution_price": 100.0,
        "quantity": 100
    }
    is_valid, reason, _ = pipeline.validate_and_conform_record(empty_ticker_rec)
    assert is_valid is False
    assert reason == "EMPTY_TICKER_SYMBOL"


def _bronze_event(trade_id, seq, price, qty, ts):
    return {
        "trade_id": trade_id,
        "sequence_number": seq,
        "ticker": "AAPL",
        "event_timestamp": ts,
        "arrival_price": price,
        "execution_price": price,
        "quantity": qty,
        "notional_value": round(price * qty, 2),
        "side": "BUY",
        "order_type": "LIMIT",
        "venue": "XNAS",
        "counterparty_id": "CPTY_001",
        "trader_id": "DESK_ALG_01",
        "is_anomaly": False,
    }


def test_gold_marts_creation_and_vwap_math(tmp_path):
    storage = StorageManager(base_dir=tmp_path)
    writer = BronzeWriter(output_dir=storage.bronze_dir)

    # Two AAPL trades in the same minute with hand-computable VWAP:
    # (100.0*100 + 110.0*300) / 400 = 107.5
    events = [
        _bronze_event("T_VWAP_1", 1, 100.0, 100, "2026-10-09T12:00:10+00:00"),
        _bronze_event("T_VWAP_2", 2, 110.0, 300, "2026-10-09T12:00:40+00:00"),
    ]
    writer.write_micro_batch(events)

    silver_pipe = SilverPipeline(storage_manager=storage)
    silver_res = silver_pipe.process_bronze_to_silver()
    assert silver_res["status"] == "SUCCESS"
    assert silver_res["clean_records"] == 2

    gold_builder = GoldMartsBuilder(storage_manager=storage)
    gold_res = gold_builder.build_marts()
    assert gold_res["status"] == "SUCCESS"
    assert "agg_intraday_vwap.parquet" in gold_res["marts_created"]
    assert "agg_intraday_vwap_5min.parquet" in gold_res["marts_created"]
    assert "fct_trade_executions.parquet" in gold_res["marts_created"]

    # 1-minute VWAP: exact numeric assertion
    vwap = pq.read_table(storage.gold_dir / "agg_intraday_vwap.parquet").to_pylist()
    assert len(vwap) == 1
    assert vwap[0]["time_window_1min"] == "2026-10-09 12:00:00"
    assert vwap[0]["vwap_price"] == pytest.approx(107.5)
    assert vwap[0]["total_shares_traded"] == 400

    # 5-minute VWAP: both trades fall in the 12:00 bucket, same VWAP
    vwap5 = pq.read_table(storage.gold_dir / "agg_intraday_vwap_5min.parquet").to_pylist()
    assert len(vwap5) == 1
    assert vwap5[0]["time_window_5min"] == "2026-10-09 12:00:00"
    assert vwap5[0]["vwap_price"] == pytest.approx(107.5)

    # Fact table: one atomic row per trade, FK keys populated
    fct = pq.read_table(storage.gold_dir / "fct_trade_executions.parquet").to_pylist()
    assert len(fct) == 2
    assert {r["ticker_key"] for r in fct} == {"AAPL"}
    assert {r["venue_key"] for r in fct} == {"XNAS"}

    # Query engine reads the same isolated gold layer
    sql_engine = LakehouseQueryEngine(storage_manager=storage)
    vwap_df = sql_engine.get_vwap_curve("AAPL")
    assert not vwap_df.empty
    assert float(vwap_df.iloc[0]["vwap_price"]) == pytest.approx(107.5)
