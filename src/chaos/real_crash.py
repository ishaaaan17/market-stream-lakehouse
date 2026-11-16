"""
REAL crash recovery: ingestion in a subprocess, killed with SIGKILL.

Unlike chaos_engine.py (an in-process simulation), this module runs the
worker as a separate OS process:

1. The parent writes the full event stream to events.jsonl.
2. Worker process #1 loads dedup state from disk (if any), processes the
   stream in batches, and after EVERY batch atomically persists
   (a) the dedup state (state.json) and (b) the committed trade ids
   (committed.jsonl). At the crash point it kills ITSELF with SIGKILL —
   no cleanup, no finally blocks, exactly like a real hard crash.
3. The parent verifies the worker really died by SIGKILL, then starts
   worker process #2, which reloads the persisted state and replays the
   FULL stream (at-least-once redelivery). Already-seen trades are
   rejected as duplicates; the rest are committed.
4. Reconciliation is computed on the committed list: zero duplicates,
   and committed == the original unique trade ids exactly.

Run the test:  pytest tests/test_real_crash_recovery.py -v
Run manually:  python -m src.chaos.real_crash demo
"""
import argparse
import json
import os
import signal
import subprocess
import sys
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
BATCH_SIZE = 100


def _worker(events_file: str, state_file: str, committed_file: str, crash_after: int) -> None:
    """Worker entry point (runs in the subprocess)."""
    if str(PROJECT_ROOT) not in sys.path:
        sys.path.insert(0, str(PROJECT_ROOT))
    from src.ingestion.deduplicator import WatermarkedDeduplicator

    dedup = WatermarkedDeduplicator(watermark_delay_seconds=3600)
    dedup.load_state(state_file)

    events = [json.loads(line) for line in Path(events_file).read_text(encoding="utf-8").splitlines() if line.strip()]
    processed = 0
    committed_fh = open(committed_file, "a", encoding="utf-8")
    for start in range(0, len(events), BATCH_SIZE):
        batch = events[start:start + BATCH_SIZE]
        accepted, _rejected = dedup.process_batch(batch)
        for e in accepted:
            committed_fh.write(e["trade_id"] + "\n")
        committed_fh.flush()
        os.fsync(committed_fh.fileno())
        dedup.save_state(state_file)
        processed += len(batch)
        if crash_after and processed >= crash_after:
            committed_fh.flush()
            os.kill(os.getpid(), signal.SIGKILL)  # hard crash, no cleanup
    committed_fh.close()


def run_real_crash_recovery(work_dir: Path | str, total_events: int = 2_000,
                            crash_at: int = 900, seed: int = 7) -> dict[str, Any]:
    """Runs the full kill/restart/replay cycle and returns computed reconciliation."""
    if str(PROJECT_ROOT) not in sys.path:
        sys.path.insert(0, str(PROJECT_ROOT))
    from src.generator.market_feed import MarketFeedGenerator

    work = Path(work_dir)
    work.mkdir(parents=True, exist_ok=True)
    events_file = work / "events.jsonl"
    state_file = work / "state.json"
    committed_file = work / "committed.jsonl"
    for f in (state_file, committed_file):
        if f.exists():
            f.unlink()

    feed = MarketFeedGenerator(seed=seed)
    raw_events = feed.generate_batch(total_events)
    # At-least-once flavour: the first 200 events are also delivered twice
    stream = raw_events + [dict(e) for e in raw_events[:200]]
    events_file.write_text("\n".join(json.dumps(e) for e in stream), encoding="utf-8")
    unique_original_ids = {e["trade_id"] for e in raw_events}

    env = dict(os.environ, PYTHONPATH=str(PROJECT_ROOT))
    cmd = [sys.executable, "-m", "src.chaos.real_crash", "worker",
           "--events", str(events_file), "--state", str(state_file),
           "--committed", str(committed_file)]

    # Phase 1: worker that will be SIGKILLed mid-stream
    p1 = subprocess.run(cmd + ["--crash-after", str(crash_at)],
                        cwd=PROJECT_ROOT, env=env, capture_output=True, text=True, timeout=120)
    sigkill_confirmed = (p1.returncode == -signal.SIGKILL)

    # Phase 2: restarted worker replays the FULL stream against persisted state
    p2 = subprocess.run(cmd + ["--crash-after", "0"],
                        cwd=PROJECT_ROOT, env=env, capture_output=True, text=True, timeout=120)
    if p2.returncode != 0:
        raise RuntimeError(f"recovery worker failed: {p2.stderr[-2000:]}")

    committed_list = [line.strip() for line in
                      committed_file.read_text(encoding="utf-8").splitlines() if line.strip()]
    committed_set = set(committed_list)
    is_zero_duplicates = len(committed_list) == len(committed_set)
    all_unique_preserved = committed_set == unique_original_ids
    return {
        "total_original_unique": len(unique_original_ids),
        "stream_deliveries": len(stream),
        "crash_after_events": crash_at,
        "phase1_returncode": p1.returncode,
        "sigkill_confirmed": sigkill_confirmed,
        "total_clean_trades_committed": len(committed_set),
        "is_zero_duplicates": is_zero_duplicates,
        "all_unique_preserved": all_unique_preserved,
        "data_loss_detected": not all_unique_preserved,
        "exactly_once_verified": bool(sigkill_confirmed and is_zero_duplicates and all_unique_preserved),
        "reconciliation_status": (
            "PASSED_REAL_SIGKILL_ZERO_DUPLICATES_ZERO_DATA_LOSS"
            if sigkill_confirmed and is_zero_duplicates and all_unique_preserved
            else "FAILED_REAL_CRASH_RECONCILIATION"
        ),
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="mode", required=True)
    w = sub.add_parser("worker")
    w.add_argument("--events", required=True)
    w.add_argument("--state", required=True)
    w.add_argument("--committed", required=True)
    w.add_argument("--crash-after", type=int, default=0)
    sub.add_parser("demo")
    args = parser.parse_args()
    if args.mode == "worker":
        _worker(args.events, args.state, args.committed, args.crash_after)
    else:
        import tempfile
        with tempfile.TemporaryDirectory() as td:
            print(json.dumps(run_real_crash_recovery(td), indent=2))
