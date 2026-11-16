"""Silver pipeline must be incremental: re-running it must not duplicate
silver rows, and each run must register a snapshot in the catalog."""
import pyarrow.parquet as pq

from src.generator.market_feed import MarketFeedGenerator
from src.ingestion.bronze_writer import BronzeWriter
from src.lakehouse.storage_manager import StorageManager
from src.lakehouse.time_travel import SnapshotCatalog
from src.transforms.silver_pipeline import SilverPipeline


def _silver_rows(storage):
    rows = 0
    for f in storage.get_partition_files("silver"):
        rows += pq.read_table(f).num_rows
    return rows


def test_silver_rerun_does_not_duplicate(tmp_path):
    storage = StorageManager(base_dir=tmp_path)
    writer = BronzeWriter(output_dir=storage.bronze_dir)
    feed = MarketFeedGenerator(seed=5)
    writer.write_micro_batch(feed.generate_batch(120))

    pipe = SilverPipeline(storage_manager=storage)
    r1 = pipe.process_bronze_to_silver()
    assert r1["status"] == "SUCCESS"
    rows_after_1 = _silver_rows(storage)
    assert rows_after_1 == r1["clean_records"] > 0

    # Second run over the same bronze: nothing new, no duplicated silver
    r2 = pipe.process_bronze_to_silver()
    assert r2["status"] == "NO_NEW_BRONZE_DATA"
    assert r2["bronze_files_skipped_previously_processed"] >= 1
    assert _silver_rows(storage) == rows_after_1

    # A genuinely new bronze batch IS processed on the next run
    writer.write_micro_batch(feed.generate_batch(60))
    r3 = pipe.process_bronze_to_silver()
    assert r3["status"] == "SUCCESS"
    assert _silver_rows(storage) > rows_after_1

    # Snapshot catalog was wired in: one snapshot per successful run
    catalog = SnapshotCatalog(catalog_path=tmp_path / "snapshots_metadata.json")
    snaps = catalog.list_snapshots(layer="silver")
    assert len(snaps) == 2
    assert snaps[0]["operation"] == "bronze_to_silver"
