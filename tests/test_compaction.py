"""
Unit tests for the Lakehouse Compaction Engine.
Verifies consolidation of fragmented micro-batch Parquet files into a single optimized file.
"""
import shutil
import pytest
from pathlib import Path
from src.generator.market_feed import MarketFeedGenerator
from src.ingestion.bronze_writer import BronzeWriter
from src.lakehouse.storage_manager import StorageManager
from src.lakehouse.compaction_worker import CompactionWorker
import pyarrow.parquet as pq

@pytest.fixture
def temp_lakehouse(tmp_path):
    test_bronze = tmp_path / "bronze"
    test_bronze.mkdir(parents=True, exist_ok=True)
    yield test_bronze
    shutil.rmtree(tmp_path, ignore_errors=True)

def test_compaction_worker_consolidation(temp_lakehouse):
    writer = BronzeWriter(output_dir=temp_lakehouse)
    feed = MarketFeedGenerator(seed=42)

    # Write 6 small micro-batch files (50 records each)
    total_written_rows = 0
    created_files = []
    for _ in range(6):
        batch = feed.generate_batch(50)
        total_written_rows += len(batch)
        f_path = writer.write_micro_batch(batch)
        created_files.append(f_path)

    assert len(created_files) == 6

    # Verify partition directory has 6 files before compaction
    partition_dir = created_files[0].parent
    files_before = list(partition_dir.glob("*.parquet"))
    assert len(files_before) == 6

    # Execute compaction worker
    worker = CompactionWorker()
    compaction_res = worker.compact_partition(partition_dir, files_before)

    assert compaction_res["status"] == "COMPACTED_SUCCESS"
    assert compaction_res["files_before"] == 6
    assert compaction_res["files_after"] == 1
    assert compaction_res["rows_consolidated"] == total_written_rows

    # Verify partition directory now has exactly 1 compacted file
    files_after = list(partition_dir.glob("*.parquet"))
    assert len(files_after) == 1
    assert "compacted_" in files_after[0].name

    # Verify row count in compacted file matches original total rows
    compacted_table = pq.read_table(files_after[0])
    assert compacted_table.num_rows == total_written_rows
