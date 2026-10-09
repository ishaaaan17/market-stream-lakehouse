"""
MarketStream Lakehouse: telemetry and analytics dashboard.
Ingestion throughput, small-file compaction, VWAP curves, SQL console
and the chaos test that proves exactly-once delivery.
"""
import sys
import time
from pathlib import Path

root_dir = Path(__file__).resolve().parent
if str(root_dir) not in sys.path:
    sys.path.insert(0, str(root_dir))

import altair as alt
import pandas as pd
import streamlit as st

from config.settings import config
from src.generator.market_feed import MarketFeedGenerator
from src.ingestion.bronze_writer import BronzeWriter
from src.transforms.silver_pipeline import SilverPipeline
from src.transforms.gold_marts import GoldMartsBuilder
from src.analytics.lakehouse_sql import LakehouseQueryEngine
from src.lakehouse.compaction_worker import CompactionWorker
from src.chaos.chaos_engine import ChaosRecoveryEngine

st.set_page_config(
    page_title="MarketStream Lakehouse",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ──────────────────────────────────────────────────────────────
# Design tokens
# ──────────────────────────────────────────────────────────────
INK = "#0A0C14"
SURFACE = "#111522"
RAISED = "#171C2E"
LINE = "#232A40"
TEXT = "#E6E9F5"
MUTED = "#8089A8"
VIOLET = "#8B7CFF"
VIOLET_SOFT = "#C4B5FD"
TEAL = "#3DD6B0"
AMBER = "#F5B454"
RED = "#FF6B7A"

STYLE = """
<style>
@import url('https://fonts.googleapis.com/css2?family=Manrope:wght@400;500;600;700;800&family=IBM+Plex+Mono:wght@400;500;600&display=swap');

:root {
  --ink: #0A0C14; --surface: #111522; --raised: #171C2E; --line: #232A40;
  --text: #E6E9F5; --muted: #8089A8; --violet: #8B7CFF; --violet-soft: #C4B5FD;
  --teal: #3DD6B0; --amber: #F5B454; --red: #FF6B7A;
  --bronze: #D08A5B; --silver: #A9B4D0; --gold: #E8C468;
  --mono: 'IBM Plex Mono', ui-monospace, monospace;
}

html, body, .stApp, [class*="css"] { font-family: 'Manrope', system-ui, sans-serif; }
.stApp {
  background:
    radial-gradient(900px 420px at 85% -10%, rgba(139,124,255,.14), transparent 60%),
    radial-gradient(700px 380px at -5% 0%, rgba(61,214,176,.06), transparent 60%),
    var(--ink);
  color: var(--text);
}
#MainMenu, footer { visibility: hidden; }
header[data-testid="stHeader"] { background: transparent; }
.block-container { padding-top: 2rem; padding-bottom: 4rem; max-width: 1280px; }

/* Sidebar */
[data-testid="stSidebar"] { background: var(--surface); border-right: 1px solid var(--line); }
[data-testid="stSidebar"] .block-container { padding-top: 1.5rem; }
.brand { display:flex; align-items:center; gap:.7rem; margin-bottom:1.4rem; }
.brand-mark {
  width:34px; height:34px; border-radius:10px; display:grid; place-items:center; font-size:1.05rem;
  background: linear-gradient(135deg, var(--violet), #5B4BDB); box-shadow: 0 6px 20px rgba(139,124,255,.35);
}
.brand b { font-size:1.02rem; font-weight:700; letter-spacing:-.01em; }
.brand span { display:block; font-size:.74rem; color:var(--muted); margin-top:1px; }
.side-label { font-size:.78rem; font-weight:600; color:var(--muted); margin:1.2rem 0 .5rem; }

/* Buttons */
.stButton > button, .stDownloadButton > button {
  border-radius: 10px; border: 1px solid var(--line); background: var(--raised); color: var(--text);
  font-weight: 600; font-size: .88rem; padding: .6rem .9rem; transition: border-color .15s, background .15s, transform .15s;
}
.stButton > button:hover, .stDownloadButton > button:hover { border-color: var(--violet); background: #1c2340; color: #fff; }
.stButton > button:active { transform: translateY(1px); }
.stButton > button:focus-visible { outline: 2px solid var(--violet-soft); outline-offset: 2px; }
.stButton > button[kind="primary"] {
  background: linear-gradient(135deg, #7C6BFF, #5B4BDB); border: none; color: #fff;
  box-shadow: 0 8px 22px rgba(124,107,255,.28);
}
.stButton > button[kind="primary"]:hover { filter: brightness(1.08); }

/* Hero */
.hero { display:flex; justify-content:space-between; align-items:flex-start; gap:1.5rem; flex-wrap:wrap; margin-bottom:1.6rem; }
.title { font-size:2.35rem; font-weight:800; letter-spacing:-.035em; line-height:1.05; margin:0; }
.title em { font-style:normal; color: var(--violet-soft); }
.lede { color:var(--muted); font-size:1rem; margin:.65rem 0 0; max-width:56ch; line-height:1.55; }
.pill {
  display:inline-flex; align-items:center; gap:.55rem; padding:.5rem .95rem; border-radius:999px;
  background: var(--surface); border:1px solid var(--line); font-size:.84rem; font-weight:600; color: var(--muted);
}
.pill i { width:8px; height:8px; border-radius:50%; background: var(--muted); }
.pill.ok { color: var(--teal); border-color: rgba(61,214,176,.35); background: rgba(61,214,176,.07); }
.pill.ok i { background: var(--teal); box-shadow: 0 0 0 0 rgba(61,214,176,.6); animation: ping 2.2s infinite; }
.pill.bad { color: var(--red); border-color: rgba(255,107,122,.4); background: rgba(255,107,122,.07); }
.pill.bad i { background: var(--red); }
@keyframes ping { 70% { box-shadow: 0 0 0 9px rgba(61,214,176,0); } 100% { box-shadow: 0 0 0 0 rgba(61,214,176,0); } }

/* Pipeline: the one signature element */
.pipe { display:grid; grid-template-columns: 1fr 72px 1fr 72px 1fr; align-items:center; margin: .4rem 0 2rem; }
.stage {
  background: var(--surface); border:1px solid var(--line); border-radius:16px; padding:1.15rem 1.3rem 1.2rem; position:relative; overflow:hidden;
}
.stage::before { content:""; position:absolute; left:0; top:0; bottom:0; width:3px; background: var(--c); }
.stage .name { display:flex; align-items:center; justify-content:space-between; font-weight:700; font-size:.95rem; color: var(--c); }
.stage .num { font-family: var(--mono); font-size:2.3rem; font-weight:600; letter-spacing:-.04em; margin:.55rem 0 .1rem; color: var(--text); }
.stage .sub { color: var(--muted); font-size:.84rem; }
.stage .note { color: var(--muted); font-size:.8rem; margin-top:.75rem; padding-top:.7rem; border-top:1px dashed var(--line); }
.stage.bronze { --c: var(--bronze); } .stage.silver { --c: var(--silver); } .stage.gold { --c: var(--gold); }
.chip { font-size:.72rem; font-weight:600; padding:.18rem .55rem; border-radius:999px; border:1px solid var(--line); color: var(--muted); }
.chip.warn { color: var(--amber); border-color: rgba(245,180,84,.4); background: rgba(245,180,84,.08); }
.chip.ok { color: var(--teal); border-color: rgba(61,214,176,.35); background: rgba(61,214,176,.07); }
.flow { height:2px; margin:0 8px; border-radius:2px; position:relative;
  background: repeating-linear-gradient(90deg, var(--violet) 0 10px, transparent 10px 20px); background-size: 40px 2px;
  animation: drift 1.4s linear infinite; opacity:.75; }
.flow::after { content:""; position:absolute; right:-2px; top:-4px; border-left:8px solid var(--violet); border-top:5px solid transparent; border-bottom:5px solid transparent; }
@keyframes drift { to { background-position: 40px 0; } }

/* Tiles */
.tiles { display:grid; grid-template-columns: repeat(auto-fit, minmax(190px, 1fr)); gap:.9rem; margin: .2rem 0 1.2rem; }
.tile { background: var(--surface); border:1px solid var(--line); border-radius:14px; padding:1rem 1.15rem; }
.tile .k { color: var(--muted); font-size:.82rem; font-weight:500; }
.tile .v { font-family: var(--mono); font-size:1.55rem; font-weight:600; letter-spacing:-.03em; margin-top:.35rem; }
.tile .s { color: var(--muted); font-size:.78rem; margin-top:.2rem; }
.tile.ok .v { color: var(--teal); } .tile.warn .v { color: var(--amber); } .tile.bad .v { color: var(--red); } .tile.violet .v { color: var(--violet-soft); }

/* Panels */
.panel { background: var(--surface); border:1px solid var(--line); border-radius:16px; padding:1.3rem 1.4rem; margin-bottom:1rem; }
.panel h4 { margin:0 0 .35rem; font-size:1.02rem; font-weight:700; letter-spacing:-.01em; color: var(--text) !important; }
.panel p { margin:0; color: var(--muted); font-size:.92rem; line-height:1.6; max-width:78ch; }
.section-title { font-size:1.15rem; font-weight:700; letter-spacing:-.015em; margin: 1.4rem 0 .7rem; }

/* Verdict banner */
.verdict { display:flex; gap:1rem; align-items:center; padding:1.1rem 1.3rem; border-radius:14px; margin-bottom:1rem;
  border:1px solid rgba(61,214,176,.4); background: linear-gradient(90deg, rgba(61,214,176,.12), rgba(61,214,176,.03)); }
.verdict.bad { border-color: rgba(255,107,122,.45); background: linear-gradient(90deg, rgba(255,107,122,.13), rgba(255,107,122,.03)); }
.verdict .badge { width:38px; height:38px; border-radius:50%; display:grid; place-items:center; font-weight:800; background: var(--teal); color:#04261d; }
.verdict.bad .badge { background: var(--red); color:#2b0309; }
.verdict b { display:block; font-size:1rem; } .verdict span { color: var(--muted); font-size:.86rem; }

/* Stacked bar and benchmark bars */
.stack { display:flex; height:16px; border-radius:8px; overflow:hidden; background: var(--raised); margin:.9rem 0 .6rem; }
.stack div { height:100%; }
.legend { display:flex; gap:1.3rem; flex-wrap:wrap; color: var(--muted); font-size:.82rem; }
.legend i { display:inline-block; width:9px; height:9px; border-radius:3px; margin-right:.4rem; }
.bench { margin:.6rem 0 .2rem; }
.bench .row { display:grid; grid-template-columns: 110px 1fr 70px; gap:.8rem; align-items:center; margin:.55rem 0; font-size:.84rem; color: var(--muted); }
.bench .bar { height:12px; border-radius:6px; background: var(--raised); overflow:hidden; }
.bench .bar span { display:block; height:100%; border-radius:6px; animation: grow .9s cubic-bezier(.2,.8,.2,1) both; transform-origin:left; }
.bench .val { font-family: var(--mono); color: var(--text); text-align:right; }
@keyframes grow { from { transform: scaleX(0); } to { transform: scaleX(1); } }

/* Activity log */
.log { font-size:.8rem; color: var(--muted); line-height:1.5; }
.log div { padding:.4rem 0; border-bottom:1px solid var(--line); }
.log time { font-family: var(--mono); color: var(--violet-soft); margin-right:.5rem; font-size:.74rem; }

/* Streamlit widgets */
.stTabs [data-baseweb="tab-list"] { gap:.3rem; border-bottom:1px solid var(--line); }
.stTabs [data-baseweb="tab"] { height:44px; padding:0 1rem; border-radius:10px 10px 0 0; color: var(--muted); font-weight:600; background:transparent; }
.stTabs [aria-selected="true"] { color: var(--text) !important; }
.stTabs [data-baseweb="tab-highlight"] { background: var(--violet); height:2px; }
.stTabs [data-baseweb="tab-border"] { display:none; }
textarea, [data-baseweb="select"] > div, .stTextInput input {
  background: var(--surface) !important; border-color: var(--line) !important; border-radius:10px !important;
}
textarea { font-family: var(--mono) !important; font-size:.86rem !important; line-height:1.55 !important; color: var(--violet-soft) !important; }
[data-testid="stDataFrame"] { border:1px solid var(--line); border-radius:12px; overflow:hidden; }
[data-testid="stExpander"] { border:1px solid var(--line); border-radius:12px; background: var(--surface); }
hr { border-color: var(--line) !important; }

@media (max-width: 900px) {
  .pipe { grid-template-columns: 1fr; gap:.6rem; }
  .flow { width:2px; height:28px; margin:0 auto; background: repeating-linear-gradient(180deg, var(--violet) 0 8px, transparent 8px 16px); animation:none; }
  .flow::after { display:none; }
  .title { font-size:1.8rem; }
}
@media (prefers-reduced-motion: reduce) {
  .flow, .pill.ok i, .bench .bar span { animation: none; }
}
</style>
"""
st.markdown(STYLE, unsafe_allow_html=True)


# ──────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────
def html(block: str) -> None:
    """Render HTML without markdown mistaking indented lines for code blocks."""
    st.markdown("".join(line.strip() for line in block.splitlines()), unsafe_allow_html=True)


def tile(label: str, value: str, sub: str = "", tone: str = "") -> str:
    return f'<div class="tile {tone}"><div class="k">{label}</div><div class="v">{value}</div><div class="s">{sub}</div></div>'


def tiles(*items: str) -> None:
    html(f'<div class="tiles">{"".join(items)}</div>')


def stat(d: dict, key: str, default=0):
    try:
        return d.get(key, default)
    except Exception:
        return default


def log_event(message: str) -> None:
    st.session_state.setdefault("log", [])
    st.session_state["log"].insert(0, (time.strftime("%H:%M:%S"), message))
    st.session_state["log"] = st.session_state["log"][:6]


# ──────────────────────────────────────────────────────────────
# Sidebar
# ──────────────────────────────────────────────────────────────
with st.sidebar:
    html('<div class="brand"><div class="brand-mark">⚡</div><div><b>MarketStream</b><span>Lakehouse control room</span></div></div>')

    html('<div class="side-label">Pipeline</div>')
    if st.button("Ingest 2,500 trades", type="primary", use_container_width=True):
        t0 = time.perf_counter()
        with st.spinner("Streaming executions through bronze, silver and gold..."):
            feed = MarketFeedGenerator()
            batch = feed.generate_batch(2500)
            writer = BronzeWriter()
            writer.write_micro_batch(batch)

            silver_pipe = SilverPipeline()
            silver_pipe.process_bronze_to_silver()

            gold_builder = GoldMartsBuilder()
            gold_builder.build_marts()
        took = time.perf_counter() - t0
        log_event(f"Ingested 2,500 trades and rebuilt gold marts in {took:.1f}s")
        st.toast("2,500 trades promoted to gold", icon="✅")

    if st.button("Compact small files", use_container_width=True):
        with st.spinner("Merging fragmented micro-batches..."):
            worker = CompactionWorker()
            results = worker.run_compaction_sweep(layer="bronze")
        log_event(f"Compaction sweep merged {len(results)} partition(s)")
        st.toast(f"Compacted {len(results)} partition(s)", icon="🧹")

    html('<div class="side-label">Resilience</div>')
    if st.button("Run worker-kill chaos test", use_container_width=True):
        with st.spinner("Injecting duplicates, killing the worker, reconciling..."):
            chaos = ChaosRecoveryEngine()
            res = chaos.run_chaos_simulation(total_events=5000, crash_at_event=2500)
            st.session_state["chaos_res"] = res
        log_event("Chaos test finished: " + str(res.get("reconciliation_status", "done")))
        st.toast("Chaos test finished", icon="💥")

    if st.session_state.get("log"):
        html('<div class="side-label">Recent activity</div>')
        rows = "".join(f"<div><time>{t}</time>{m}</div>" for t, m in st.session_state["log"])
        html(f'<div class="log">{rows}</div>')


# ──────────────────────────────────────────────────────────────
# Data for the page
# ──────────────────────────────────────────────────────────────
sql_engine = LakehouseQueryEngine()
storage = sql_engine.storage
bronze_stats = storage.inspect_layer_stats("bronze")
silver_stats = storage.inspect_layer_stats("silver")
gold_stats = storage.inspect_layer_stats("gold")

chaos_res = st.session_state.get("chaos_res")
chaos_ok = False
if chaos_res:
    _status = str(chaos_res.get("reconciliation_status", "")).lower()
    chaos_ok = any(word in _status for word in ("pass", "success", "verified", "ok")) and not any(
        word in _status for word in ("fail", "mismatch", "error")
    )

# ──────────────────────────────────────────────────────────────
# Hero
# ──────────────────────────────────────────────────────────────
if chaos_res is None:
    pill = '<div class="pill"><i></i>Chaos test not run yet</div>'
elif chaos_ok:
    pill = '<div class="pill ok"><i></i>Exactly-once verified</div>'
else:
    pill = '<div class="pill bad"><i></i>Check chaos results</div>'

html(f"""
<div class="hero">
  <div>
    <div class="title">MarketStream <em>Lakehouse</em></div>
    <p class="lede">Trades stream into bronze, get cleaned in silver and land in gold marts. Small files get merged in the background, and a killed worker never loses or doubles a trade.</p>
  </div>
  {pill}
</div>
""")

# Pipeline strip
small = stat(bronze_stats, "small_files_detected")
small_chip = (
    f'<span class="chip warn">{small} small files</span>' if small else '<span class="chip ok">no small files</span>'
)
html(f"""
<div class="pipe">
  <div class="stage bronze">
    <div class="name">Bronze {small_chip}</div>
    <div class="num">{stat(bronze_stats, "file_count")}</div>
    <div class="sub">files, {stat(bronze_stats, "total_size_mb")} MB</div>
    <div class="note">Raw ticks, append-only, partitioned by date and ticker</div>
  </div>
  <div class="flow"></div>
  <div class="stage silver">
    <div class="name">Silver <span class="chip">deduplicated</span></div>
    <div class="num">{stat(silver_stats, "file_count")}</div>
    <div class="sub">files, {stat(silver_stats, "total_size_mb")} MB</div>
    <div class="note">Validated trades, 30 second watermark, bad ticks to the DLQ</div>
  </div>
  <div class="flow"></div>
  <div class="stage gold">
    <div class="name">Gold <span class="chip">star schema</span></div>
    <div class="num">{stat(gold_stats, "file_count")}</div>
    <div class="sub">files, {stat(gold_stats, "total_size_mb")} MB</div>
    <div class="note">VWAP, slippage and venue marts built with dbt</div>
  </div>
</div>
""")

# ──────────────────────────────────────────────────────────────
# Tabs
# ──────────────────────────────────────────────────────────────
tab_market, tab_chaos, tab_compact, tab_sql = st.tabs(
    ["Market analytics", "Chaos recovery", "Compaction", "SQL console"]
)

# ---- Market analytics ----------------------------------------
with tab_market:
    pick, _ = st.columns([1, 3])
    with pick:
        selected_ticker = st.selectbox("Ticker", config.tickers, index=0)

    vwap_df = sql_engine.get_vwap_curve(selected_ticker)

    if vwap_df is not None and not vwap_df.empty:
        last = vwap_df.iloc[-1]
        tiles(
            tile("Latest VWAP", f"{last['vwap_price']:,.2f}", f"{selected_ticker}, newest 1 min window", "violet"),
            tile("Arrival price", f"{last['avg_arrival_price']:,.2f}", "benchmark for the same window"),
            tile(
                "Avg slippage",
                f"{vwap_df['avg_slippage_bps'].mean():,.2f} bps",
                "across all windows",
                "warn" if vwap_df["avg_slippage_bps"].mean() > 0 else "ok",
            ),
            tile("Shares traded", f"{int(vwap_df['total_shares_traded'].sum()):,}", f"{len(vwap_df)} one-minute windows"),
        )

        chart_df = vwap_df.copy()
        x_type = "O"
        try:
            chart_df["time_window_1min"] = pd.to_datetime(chart_df["time_window_1min"])
            x_type = "T"
        except Exception:
            pass

        names = {"vwap_price": "VWAP", "avg_arrival_price": "Arrival benchmark"}
        long_df = chart_df.melt(
            id_vars="time_window_1min",
            value_vars=list(names),
            var_name="series",
            value_name="price",
        )
        long_df["series"] = long_df["series"].map(names)

        chart = (
            alt.Chart(long_df)
            .mark_line(strokeWidth=2.5, interpolate="monotone")
            .encode(
                x=alt.X(f"time_window_1min:{x_type}", title=None),
                y=alt.Y("price:Q", scale=alt.Scale(zero=False), title=None),
                color=alt.Color(
                    "series:N",
                    scale=alt.Scale(domain=["VWAP", "Arrival benchmark"], range=[VIOLET, AMBER]),
                    legend=alt.Legend(orient="top-left", title=None, labelColor=TEXT, symbolType="stroke"),
                ),
                strokeDash=alt.StrokeDash(
                    "series:N",
                    scale=alt.Scale(domain=["VWAP", "Arrival benchmark"], range=[[1, 0], [5, 4]]),
                    legend=None,
                ),
                tooltip=[
                    alt.Tooltip("time_window_1min", title="Window"),
                    alt.Tooltip("series", title="Series"),
                    alt.Tooltip("price:Q", title="Price", format=",.2f"),
                ],
            )
            .properties(height=340, background="transparent")
            .configure_view(strokeWidth=0)
            .configure_axis(
                gridColor="#1B2136", domainColor=LINE, tickColor=LINE, labelColor=MUTED, labelFontSize=11
            )
        )

        left, right = st.columns([5, 3], gap="large")
        with left:
            html('<div class="section-title">Intraday VWAP against arrival price</div>')
            st.altair_chart(chart, use_container_width=True)
        with right:
            html('<div class="section-title">Window detail</div>')
            st.dataframe(
                vwap_df[["time_window_1min", "total_shares_traded", "vwap_price", "avg_slippage_bps"]],
                use_container_width=True,
                height=340,
                hide_index=True,
            )
    else:
        html("""
        <div class="panel">
          <h4>No trades yet</h4>
          <p>Press <b>Ingest 2,500 trades</b> in the sidebar. VWAP curves and venue breakdowns appear here as soon as gold is built.</p>
        </div>
        """)

    html('<div class="section-title">Venue liquidity and slippage</div>')
    venue_df = sql_engine.get_venue_liquidity_summary()
    if venue_df is not None and not venue_df.empty:
        st.dataframe(venue_df, use_container_width=True, hide_index=True)
    else:
        st.caption("Venue data shows up after the first ingest.")

# ---- Chaos recovery ------------------------------------------
with tab_chaos:
    html("""
    <div class="panel">
      <h4>What this test does</h4>
      <p>It SIGKILLs the consumer halfway through a stream that already contains duplicate and late events, then lets the broker replay messages.
      If the lakehouse is exactly-once, the clean layer ends with no duplicates and no missing trades.</p>
    </div>
    """)

    if chaos_res:
        status = str(chaos_res.get("reconciliation_status", ""))
        total_in = int(chaos_res.get("total_input_stream_with_chaos", 0))
        dups = int(chaos_res.get("duplicate_records_intercepted", 0))
        clean = int(chaos_res.get("total_clean_trades_committed", 0))
        crash = int(chaos_res.get("simulated_worker_crash_at", 0))

        cls = "" if chaos_ok else "bad"
        badge = "✓" if chaos_ok else "!"
        html(f"""
        <div class="verdict {cls}">
          <div class="badge">{badge}</div>
          <div><b>{status}</b><span>Worker killed at event {crash:,}, stream replayed, results reconciled against the source.</span></div>
        </div>
        """)

        tiles(
            tile("Input stream", f"{total_in:,}", "events including injected chaos"),
            tile("Worker crashed at", f"#{crash:,}", "SIGKILL, no graceful shutdown", "warn"),
            tile("Duplicates blocked", f"{dups:,}", "intercepted before silver", "violet"),
            tile("Clean trades committed", f"{clean:,}", "unique executions in silver", "ok" if chaos_ok else "bad"),
        )

        if total_in > 0:
            p_clean = clean / total_in * 100
            p_dup = dups / total_in * 100
            p_rest = max(0.0, 100 - p_clean - p_dup)
            html(f"""
            <div class="panel">
              <h4>Where the input stream went</h4>
              <div class="stack">
                <div style="width:{p_clean:.2f}%; background:{TEAL}"></div>
                <div style="width:{p_dup:.2f}%; background:{VIOLET}"></div>
                <div style="width:{p_rest:.2f}%; background:{LINE}"></div>
              </div>
              <div class="legend">
                <span><i style="background:{TEAL}"></i>Committed ({p_clean:.1f}%)</span>
                <span><i style="background:{VIOLET}"></i>Duplicates blocked ({p_dup:.1f}%)</span>
                <span><i style="background:{LINE}"></i>Other, such as late or malformed ({p_rest:.1f}%)</span>
              </div>
            </div>
            """)

        with st.expander("Raw result"):
            st.json(chaos_res)
    else:
        html("""
        <div class="panel">
          <h4>Nothing to show yet</h4>
          <p>Press <b>Run worker-kill chaos test</b> in the sidebar. It takes a few seconds and the results appear here.</p>
        </div>
        """)

# ---- Compaction ----------------------------------------------
with tab_compact:
    html("""
    <div class="panel">
      <h4>Why compaction matters</h4>
      <p>Micro-batch ingestion writes tiny files all day. Thousands of them slow every analytical scan and strain metadata.
      The compaction worker merges them into large Parquet files in the background.</p>
    </div>
    """)

    total_files = max(int(stat(bronze_stats, "file_count")), 1)
    small_files = int(stat(bronze_stats, "small_files_detected"))
    frag = small_files / total_files * 100
    frag_tone = "ok" if frag < 25 else ("warn" if frag < 60 else "bad")

    tiles(
        tile("Bronze fragmentation", f"{frag:.0f}%", f"{small_files} of {total_files} files are small", frag_tone),
        tile("Compaction trigger", f"{config.min_file_count_for_compaction}+ files", "per partition"),
        tile("Target file size", f"{config.target_compacted_file_size_mb} MB", "after merging"),
        tile("Small file cutoff", f"{config.small_file_threshold_bytes / 1024:,.0f} KB", "anything below counts as small"),
    )

    html(f"""
    <div class="panel">
      <h4>Recorded benchmark</h4>
      <p>Scan latency on the project's own test data, before and after one compaction sweep.</p>
      <div class="bench">
        <div class="row"><span>Before</span><div class="bar"><span style="width:100%; background:{AMBER}"></span></div><span class="val">13.98 ms</span></div>
        <div class="row"><span>After</span><div class="bar"><span style="width:{2.77 / 13.98 * 100:.1f}%; background:{TEAL}"></span></div><span class="val">2.77 ms</span></div>
      </div>
    </div>
    """)

    tiles(
        tile("Scan speedup", "5.05x", "DuckDB over compacted files", "ok"),
        tile("Disk footprint", "-18.94%", "dictionary encoding and row groups", "ok"),
    )

    with st.expander("Bronze partition stats"):
        st.json(bronze_stats)

# ---- SQL console ---------------------------------------------
PRESETS = {
    "Volume by ticker": (
        "SELECT ticker, COUNT(*) AS trades, ROUND(SUM(notional_value), 2) AS total_volume, "
        "ROUND(AVG(slippage_bps), 2) AS avg_slippage "
        "FROM 'data/silver/**/*.parquet' GROUP BY ticker ORDER BY total_volume DESC"
    ),
    "Worst slippage first": (
        "SELECT ticker, ROUND(AVG(slippage_bps), 2) AS avg_slippage, COUNT(*) AS trades "
        "FROM 'data/silver/**/*.parquet' GROUP BY ticker ORDER BY avg_slippage DESC"
    ),
    "Total trades in silver": "SELECT COUNT(*) AS trades FROM 'data/silver/**/*.parquet'",
}


def apply_preset() -> None:
    st.session_state["sql_text"] = PRESETS[st.session_state["sql_preset"]]


st.session_state.setdefault("sql_text", PRESETS["Volume by ticker"])

with tab_sql:
    top_l, top_r = st.columns([1, 2])
    with top_l:
        st.selectbox("Start from a preset", list(PRESETS), key="sql_preset", on_change=apply_preset)

    query_input = st.text_area("DuckDB SQL", key="sql_text", height=140)

    run = st.button("Run query", type="primary")
    if run:
        try:
            res_df, elapsed_ms = sql_engine.query(query_input)
            tiles(
                tile("Rows returned", f"{len(res_df):,}", "", "violet"),
                tile("Query time", f"{elapsed_ms} ms", "DuckDB over parquet", "ok"),
            )
            st.dataframe(res_df, use_container_width=True, hide_index=True)
            st.download_button(
                "Download CSV",
                res_df.to_csv(index=False).encode("utf-8"),
                file_name="query_result.csv",
                mime="text/csv",
            )
        except Exception as e:
            html(f"""
            <div class="verdict bad">
              <div class="badge">!</div>
              <div><b>The query failed</b><span>{str(e)[:300]}</span></div>
            </div>
            """)
