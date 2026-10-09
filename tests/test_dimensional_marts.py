"""
Unit tests for Silver curation, Dead-Letter Queue (DLQ) quarantine, and Gold data marts.
"""
import shutil
import pytest
from src.generator.market_feed import MarketFeedGenerator
from src.ingestion.bronze_writer import BronzeWriter
from src.transforms.silver_pipeline import SilverPipeline
from src.transforms.gold_marts import GoldMartsBuilder
from src.analytics.lakehouse_sql import LakehouseQueryEngine

def test_silver_validation_and_dlq_quarantine():
    pipeline = SilverPipeline()

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

def test_gold_marts_creation_and_vwap_math(tmp_path):
    # End-to-end integration: Generate bronze -> Promote to Silver -> Build Gold marts
    feed = MarketFeedGenerator(seed=77)
    writer = BronzeWriter()

    # Generate 500 events
    batch = feed.generate_batch(500)
    writer.write_micro_batch(batch)

    # Run Silver promotion
    silver_pipe = SilverPipeline()
    silver_res = silver_pipe.process_bronze_to_silver()
    assert silver_res["status"] == "SUCCESS"
    assert silver_res["clean_records"] > 0

    # Build Gold marts
    gold_builder = GoldMartsBuilder()
    gold_res = gold_builder.build_marts()
    assert gold_res["status"] == "SUCCESS"
    assert "agg_intraday_vwap.parquet" in gold_res["marts_created"]

    # Verify query engine retrieves VWAP
    sql_engine = LakehouseQueryEngine()
    vwap_df = sql_engine.get_vwap_curve("AAPL")
    assert not vwap_df.empty
    assert "vwap_price" in vwap_df.columns
