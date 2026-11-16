"""REAL crash test: a subprocess worker is SIGKILLed mid-stream and the
pipeline must still end with zero duplicates and zero data loss, because
the deduplicator state is persisted to disk."""
import os

import pytest

from src.chaos.real_crash import run_real_crash_recovery

pytestmark = pytest.mark.skipif(os.name != "posix", reason="SIGKILL semantics are POSIX")


def test_real_sigkill_crash_and_recovery(tmp_path):
    result = run_real_crash_recovery(tmp_path, total_events=1_200, crash_at=500)

    assert result["sigkill_confirmed"] is True, result
    assert result["is_zero_duplicates"] is True, result
    assert result["all_unique_preserved"] is True, result
    assert result["data_loss_detected"] is False, result
    assert result["exactly_once_verified"] is True, result
    assert result["total_clean_trades_committed"] == result["total_original_unique"]
