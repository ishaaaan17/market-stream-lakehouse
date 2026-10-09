"""
Streamlit Lakehouse Telemetry & Analytics Dashboard.
Provides real-time visibility into ingestion throughput, consumer lag,
compaction status, VWAP curves, and chaos test reconciliation.
"""
import sys
from pathlib import Path
root_dir = Path(__file__).resolve().parent
if str(root_dir) not in sys.path:
    sys.path.insert(0, str(root_dir))

import streamlit as st
import pandas as pd
import duckdb
from config.settings import config
from src.generator.market_feed import MarketFeedGenerator
from src.ingestion.bronze_writer import BronzeWriter
from src.transforms.silver_pipeline import SilverPipeline
from src.transforms.gold_marts import GoldMartsBuilder
from src.analytics.lakehouse_sql import LakehouseQueryEngine
from src.lakehouse.compaction_worker import CompactionWorker
from src.chaos.chaos_engine import ChaosRecoveryEngine

st.set_page_config(
    page_title="MarketStream Lakehouse | Telemetry & Analytics",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom Styling (Dark theme with violet accents matching portfolio)
st.markdown("""
<style>
    .main { background-color: #0d1117; color: #c9d1d9; }
    .stMetric { background-color: #161b22; padding: 15px; border-radius: 8px; border: 1px solid #30363d; }
    h1, h2, h3 { color: #C4B5FD !important; font-family: 'JetBrains Mono', monospace; }
</style>
""", unsafe_allow_html=True)

st.title("⚡ MarketStream Lakehouse Platform")
st.caption("High-Throughput Financial Stream Processing · Medallion Lakehouse · Asynchronous Compaction · Exactly-Once Semantics")

# Sidebar Controls
st.sidebar.header("🕹️ Stream & Pipeline Control")

if st.sidebar.button("🚀 Ingest Streaming Batch (2,500 Trades)", use_container_width=True):
    with st.spinner("Streaming executions into Lakehouse..."):
        feed = MarketFeedGenerator()
        batch = feed.generate_batch(2500)
        writer = BronzeWriter()
        writer.write_micro_batch(batch)

        silver_pipe = SilverPipeline()
        silver_pipe.process_bronze_to_silver()

        gold_builder = GoldMartsBuilder()
        gold_builder.build_marts()
    st.sidebar.success("Ingested 2,500 executions & promoted to Gold!")

if st.sidebar.button("🧹 Run Small-File Compaction Sweep", use_container_width=True):
    with st.spinner("Compacting fragmented micro-batches..."):
        worker = CompactionWorker()
        results = worker.run_compaction_sweep(layer="bronze")
        st.sidebar.info(f"Compacted {len(results)} partition(s) successfully!")

st.sidebar.divider()
st.sidebar.subheader("Chaos Testing")
if st.sidebar.button("💥 Run Chaos Worker-Kill Test", use_container_width=True):
    with st.spinner("Injecting duplicates, killing worker, and asserting reconciliation..."):
        chaos = ChaosRecoveryEngine()
        res = chaos.run_chaos_simulation(total_events=5000, crash_at_event=2500)
        st.session_state["chaos_res"] = res

# Metrics Overview
sql_engine = LakehouseQueryEngine()
storage = sql_engine.storage
bronze_stats = storage.inspect_layer_stats("bronze")
silver_stats = storage.inspect_layer_stats("silver")
gold_stats = storage.inspect_layer_stats("gold")

col1, col2, col3, col4 = st.columns(4)
col1.metric("Bronze Files (Raw)", f"{bronze_stats['file_count']} files", f"{bronze_stats['total_size_mb']} MB")
col2.metric("Small Files Detected", f"{bronze_stats['small_files_detected']} files", "Fragmentation Index")
col3.metric("Silver Fact Tables", f"{silver_stats['file_count']} files", f"{silver_stats['total_size_mb']} MB")
col4.metric("Gold Analytical Marts", f"{gold_stats['file_count']} files", "Kimball Star Schema")

st.divider()

# Tabs for Telemetry & Analytics
tab1, tab2, tab3, tab4 = st.tabs(["📈 Real-Time Market Analytics", "💥 Chaos Recovery Proof", "⚙️ Lakehouse Compaction", "🔍 SQL Query Console"])

with tab1:
    st.subheader("Intraday VWAP vs Arrival Benchmark")
    selected_ticker = st.selectbox("Select Instrument Ticker", config.tickers, index=0)

    vwap_df = sql_engine.get_vwap_curve(selected_ticker)
    if not vwap_df.empty:
        col_c1, col_c2 = st.columns([2, 1])
        with col_c1:
            st.line_chart(vwap_df.set_index("time_window_1min")[["vwap_price", "avg_arrival_price"]])
        with col_c2:
            st.dataframe(vwap_df[["time_window_1min", "total_shares_traded", "vwap_price", "avg_slippage_bps"]], use_container_width=True)
    else:
        st.info("No trades ingested yet. Click 'Ingest Streaming Batch' in the sidebar to stream trades.")

    st.subheader("Venue Execution Liquidity & Slippage Breakdown")
    venue_df = sql_engine.get_venue_liquidity_summary()
    if not venue_df.empty:
        st.dataframe(venue_df, use_container_width=True)

with tab2:
    st.subheader("Chaos Engineering & Exactly-Once Semantics Assertion")
    st.markdown("""
    This test forcefully terminates the consumer worker (`SIGKILL`) halfway through an in-flight stream
    with injected network duplicate events and late arrivals, triggering replayed messages from the broker.
    It proves **zero duplicate executions** and **zero lost transactions** in the conformed Lakehouse layer.
    """)

    if "chaos_res" in st.session_state:
        res = st.session_state["chaos_res"]
        st.success(f"Status: {res['reconciliation_status']}")

        m_col1, m_col2, m_col3, m_col4 = st.columns(4)
        m_col1.metric("Input Chaos Stream", f"{res['total_input_stream_with_chaos']:,} events")
        m_col2.metric("Worker Crash At", f"Event #{res['simulated_worker_crash_at']:,}")
        m_col3.metric("Duplicates Blocked", f"{res['duplicate_records_intercepted']:,} events")
        m_col4.metric("Clean Committed Trades", f"{res['total_clean_trades_committed']:,} trades")

        st.json(res)
    else:
        st.info("Click 'Run Chaos Worker-Kill Test' in the sidebar to simulate a live worker failure.")

with tab3:
    st.subheader("Small-File Compaction Engine Metrics")
    st.markdown("""
    In high-throughput micro-batch ingestion, writing frequent small files causes metadata thrashing
    and degrades analytical query scans. The **CompactionWorker** consolidates these into optimal chunks.
    """)
    st.json({
        "min_files_for_compaction_threshold": config.min_file_count_for_compaction,
        "target_compacted_size_mb": config.target_compacted_file_size_mb,
        "small_file_definition_bytes": config.small_file_threshold_bytes,
        "bronze_partition_stats": bronze_stats,
    })

with tab4:
    st.subheader("Interactive Lakehouse SQL Console")
    query_input = st.text_area("DuckDB SQL Query", "SELECT ticker, COUNT(*) as trades, ROUND(SUM(notional_value), 2) as total_volume, ROUND(AVG(slippage_bps), 2) as avg_slippage FROM 'data/silver/**/*.parquet' GROUP BY ticker ORDER BY total_volume DESC", height=100)
    if st.button("Execute Query"):
        try:
            res_df, elapsed_ms = sql_engine.query(query_input)
            st.caption(f"Query executed in **{elapsed_ms} ms**")
            st.dataframe(res_df, use_container_width=True)
        except Exception as e:
            st.error(f"SQL Error: {e}")
