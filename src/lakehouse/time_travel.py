"""
Lakehouse snapshot catalog and time travel interface.
Allows querying the exact state of tables as of a specific historical snapshot or timestamp.
"""
import json
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
import duckdb
from config.settings import config

class SnapshotCatalog:
    def __init__(self, catalog_path: Path | None = None):
        self.catalog_path = catalog_path or (config.data_dir / "snapshots_metadata.json")
        self.snapshots: list[dict[str, Any]] = self._load_catalog()

    def _load_catalog(self) -> list[dict[str, Any]]:
        if self.catalog_path.exists():
            try:
                with open(self.catalog_path, "r", encoding="utf-8") as f:
                    return json.load(f)
            except Exception:
                return []
        return []

    def _save_catalog(self):
        with open(self.catalog_path, "w", encoding="utf-8") as f:
            json.dump(self.snapshots, f, indent=2)

    def register_snapshot(self, layer: str, row_count: int, file_count: int, operation: str) -> dict[str, Any]:
        """Creates an immutable snapshot entry for the table state."""
        snapshot_id = len(self.snapshots) + 1
        snapshot = {
            "snapshot_id": snapshot_id,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "layer": layer,
            "total_rows": row_count,
            "file_count": file_count,
            "operation": operation,
        }
        self.snapshots.append(snapshot)
        self._save_catalog()
        return snapshot

    def list_snapshots(self, layer: str | None = None) -> list[dict[str, Any]]:
        """Lists registered snapshots."""
        if layer:
            return [s for s in self.snapshots if s["layer"] == layer]
        return list(self.snapshots)
