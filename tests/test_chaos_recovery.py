"""
Integration test for Exactly-Once stream semantics under worker crash and message replay.
"""
import pytest
from src.chaos.chaos_engine import ChaosRecoveryEngine

def test_chaos_worker_crash_and_recovery():
    chaos_engine = ChaosRecoveryEngine()
    result = chaos_engine.run_chaos_simulation(total_events=5_000, crash_at_event=2_500)

    assert result["reconciliation_status"] == "PASSED_ZERO_DUPLICATES_ZERO_DATA_LOSS"
    assert result["exactly_once_verified"] is True
    assert result["data_loss_detected"] is False
    assert result["duplicate_records_intercepted"] > 0
    assert result["total_clean_trades_committed"] > 0
