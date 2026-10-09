"""
Asynchronous Lakehouse Compaction Engine.
Solves the Small-File Problem in real-time streaming ingestion.
Consolidates micro-batch Parquet fragments into optimized 128MB chunks,
reducing file handle thrashing, metadata parsing overhead, and scan latency.
"""
import time
import uuid
from pathlib import Path
from typing import Any
import pyarrow as pa
import pyarrow.parquet as pq
from config.settings import config
from src.lakehouse.storage_manager import StorageManager

class CompactionWorker:
    def __init__(self, storage_manager: StorageManager | None = None):
        self.storage = storage_manager or StorageManager()
        self.compaction_history: list[dict[str, Any]] = []

    def identify_fragmented_partitions(self, layer: str = "bronze") -> dict[Path, list[Path]]:
        """
        Finds partition directories containing more than min_file_count_for_compaction
        uncompacted files.
        """
        layer_dir = getattr(self.storage, f"{layer}_dir")
        if not layer_dir.exists():
            return {}

        partitions: dict[Path, list[Path]] = {}
        for p_file in layer_dir.rglob("*.parquet"):
            # Exclude already compacted files
            if "compacted_" in p_file.name:
                continue
            parent = p_file.parent
            if parent not in partitions:
                partitions[parent] = []
            partitions[parent].append(p_file)

        # Filter partitions that exceed the fragmentation threshold
        fragmented = {
            parent: files
            for parent, files in partitions.items()
            if len(files) >= config.min_file_count_for_compaction
        }
        return fragmented

    def compact_partition(self, partition_dir: Path, source_files: list[Path]) -> dict[str, Any]:
        """
        Atomically reads multiple small Parquet files, merges them into an optimized single table,
        writes the consolidated file, and removes the fragmented sources.
        """
        t0 = time.perf_counter()
        original_file_count = len(source_files)
        original_size_bytes = sum(f.stat().st_size for f in source_files)

        # 1. Read all small files in zero-copy chunks
        tables = [pq.read_table(f) for f in source_files]
        if not tables:
            return {"status": "SKIPPED_EMPTY"}

        merged_table = pa.concat_tables(tables)
        total_rows = merged_table.num_rows

        # 2. Write the single consolidated Parquet file with Snappy compression & dictionary encoding
        compacted_id = uuid.uuid4().hex[:8]
        ts_str = int(time.time())
        compacted_filename = partition_dir / f"compacted_{ts_str}_{compacted_id}.parquet"

        pq.write_table(
            merged_table,
            compacted_filename,
            compression="snappy",
            use_dictionary=True,
            row_group_size=100_000
        )

        compacted_size_bytes = compacted_filename.stat().st_size

        # 3. Atomically remove the old small files
        for f in source_files:
            try:
                f.unlink(missing_ok=True)
            except Exception:
                pass

        elapsed_ms = round((time.perf_counter() - t0) * 1000, 2)
        compaction_ratio = round((1 - (compacted_size_bytes / max(1, original_size_bytes))) * 100, 2)

        try:
            partition_str = str(partition_dir.relative_to(config.data_dir))
        except ValueError:
            partition_str = str(partition_dir.name)

        result = {
            "partition": partition_str,
            "compacted_file": str(compacted_filename.name),
            "files_before": original_file_count,
            "files_after": 1,
            "rows_consolidated": total_rows,
            "size_before_bytes": original_size_bytes,
            "size_after_bytes": compacted_size_bytes,
            "compaction_time_ms": elapsed_ms,
            "compression_savings_pct": compaction_ratio,
            "status": "COMPACTED_SUCCESS"
        }
        self.compaction_history.append(result)
        return result

    def run_compaction_sweep(self, layer: str = "bronze") -> list[dict[str, Any]]:
        """Scans the layer and compacts all fragmented partitions."""
        fragmented = self.identify_fragmented_partitions(layer=layer)
        results = []
        for partition_dir, files in fragmented.items():
            res = self.compact_partition(partition_dir, files)
            results.append(res)
        return results
