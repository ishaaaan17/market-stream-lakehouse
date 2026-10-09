"""
Lakehouse Storage Manager.
Handles Medallion architecture layout, metadata tracking, and partition resolution.
"""
from pathlib import Path
from typing import Any
import pyarrow.parquet as pq
from config.settings import config

class StorageManager:
    def __init__(self, base_dir: Path | None = None):
        self.base_dir = base_dir or config.data_dir
        self.bronze_dir = config.bronze_dir
        self.silver_dir = config.silver_dir
        self.gold_dir = config.gold_dir
        self.dlq_dir = config.dlq_dir

    def get_partition_files(self, layer: str, partition_path: str = "") -> list[Path]:
        """Returns all Parquet files within a specific layer and subpartition."""
        target_dir = getattr(self, f"{layer}_dir", self.base_dir / layer)
        if partition_path:
            target_dir = target_dir / partition_path

        if not target_dir.exists():
            return []
        return sorted([p for p in target_dir.rglob("*.parquet") if p.is_file()])

    def inspect_layer_stats(self, layer: str) -> dict[str, Any]:
        """Calculates total files, total size, and average file size for a medallion layer."""
        files = self.get_partition_files(layer)
        total_size = sum(f.stat().st_size for f in files)
        file_count = len(files)
        avg_size_kb = (total_size / file_count / 1024) if file_count > 0 else 0.0

        return {
            "layer": layer,
            "file_count": file_count,
            "total_size_bytes": total_size,
            "total_size_mb": round(total_size / (1024 * 1024), 3),
            "avg_file_size_kb": round(avg_size_kb, 2),
            "small_files_detected": sum(1 for f in files if f.stat().st_size < config.small_file_threshold_bytes)
        }
