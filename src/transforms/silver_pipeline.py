"""
Silver Layer Pipeline: Cleansing, Validation, Schema Enforcement, and DLQ Quarantine.
Promotes raw Bronze records to curated, conformed Silver fact tables.
"""
import json
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
import pyarrow as pa
import pyarrow.parquet as pq
from src.lakehouse.storage_manager import StorageManager
from src.lakehouse.time_travel import SnapshotCatalog

class SilverPipeline:
    def __init__(self, storage_manager: StorageManager | None = None,
                 silver_dir: Path | None = None, dlq_dir: Path | None = None):
        self.storage = storage_manager or StorageManager()
        # Layer dirs come from the storage manager (which derives them
        # from its base_dir), not from global config — so an isolated
        # lakehouse (e.g. in tests) stays isolated.
        self.silver_dir = Path(silver_dir) if silver_dir is not None else self.storage.silver_dir
        self.dlq_dir = Path(dlq_dir) if dlq_dir is not None else self.storage.dlq_dir
        self.silver_dir.mkdir(parents=True, exist_ok=True)
        self.dlq_dir.mkdir(parents=True, exist_ok=True)
        # Incremental processing manifest: bronze files already promoted
        self.manifest_path = self.silver_dir / ".silver_manifest.json"

    # --- incremental manifest ------------------------------------------------
    def _load_manifest(self) -> set[str]:
        if self.manifest_path.exists():
            try:
                return set(json.loads(self.manifest_path.read_text(encoding="utf-8")))
            except Exception:
                return set()
        return set()

    def _save_manifest(self, processed: set[str]) -> None:
        self.manifest_path.write_text(json.dumps(sorted(processed), indent=2), encoding="utf-8")

    @staticmethod
    def _file_key(p: Path) -> str:
        return f"{p.as_posix()}|{p.stat().st_size}"

    def validate_and_conform_record(self, record: dict[str, Any]) -> tuple[bool, str, dict[str, Any]]:
        """
        Validates record against strict institutional data contracts.
        Returns: (is_valid, quarantine_reason, conformed_record)
        """
        # Contract 1: Non-null primary key
        trade_id = str(record.get("trade_id", "")).strip()
        if not trade_id or trade_id.lower() == "none":
            return False, "NULL_OR_EMPTY_TRADE_ID", record

        # Contract 2: Valid ticker symbol
        ticker = str(record.get("ticker", "")).strip().upper()
        if not ticker:
            return False, "EMPTY_TICKER_SYMBOL", record

        # Contract 3: Positive execution price
        try:
            exec_price = float(record.get("execution_price", 0))
            if exec_price <= 0:
                return False, "NON_POSITIVE_EXECUTION_PRICE", record
        except (ValueError, TypeError):
            return False, "INVALID_PRICE_FORMAT", record

        # Contract 4: Positive quantity
        try:
            qty = int(record.get("quantity", 0))
            if qty <= 0:
                return False, "NON_POSITIVE_QUANTITY", record
        except (ValueError, TypeError):
            return False, "INVALID_QUANTITY_FORMAT", record

        # Contract 5: Valid arrival price
        try:
            arrival_price = float(record.get("arrival_price", exec_price))
        except Exception:
            arrival_price = exec_price

        # Compute derived slippage metrics
        slippage_dollars = round(exec_price - arrival_price, 4)
        slippage_bps = round(((exec_price - arrival_price) / max(0.01, arrival_price)) * 10_000, 2)

        conformed = {
            "trade_id": trade_id,
            "sequence_number": int(record.get("sequence_number", 0)),
            "ticker": ticker,
            "event_timestamp": str(record.get("event_timestamp", "")),
            "arrival_price": arrival_price,
            "execution_price": exec_price,
            "quantity": qty,
            "notional_value": round(exec_price * qty, 2),
            "slippage_dollars": slippage_dollars,
            "slippage_bps": slippage_bps,
            "side": str(record.get("side", "BUY")).upper(),
            "order_type": str(record.get("order_type", "LIMIT")).upper(),
            "venue": str(record.get("venue", "XNAS")).upper(),
            "counterparty_id": str(record.get("counterparty_id", "UNKNOWN")),
            "trader_id": str(record.get("trader_id", "UNKNOWN")),
            "silver_processed_at": datetime.now(timezone.utc).isoformat(),
        }
        return True, "CLEAN", conformed

    def process_bronze_to_silver(self) -> dict[str, Any]:
        """
        Reads unpromoted Bronze files, validates contracts, deduplicates,
        routes errors to DLQ, and commits clean data to Silver layer.
        """
        all_bronze_files = self.storage.get_partition_files("bronze")
        if not all_bronze_files:
            return {"status": "NO_BRONZE_DATA_FOUND", "clean_records": 0, "dlq_records": 0}

        # Incremental: skip bronze files a previous run already promoted.
        # Previously every run rescanned ALL of bronze and appended the
        # same trades to silver again (duplicates on every re-run).
        already = self._load_manifest()
        bronze_files = [f for f in all_bronze_files if self._file_key(f) not in already]
        skipped = len(all_bronze_files) - len(bronze_files)
        if not bronze_files:
            return {"status": "NO_NEW_BRONZE_DATA", "clean_records": 0, "dlq_records": 0,
                    "bronze_files_scanned": 0, "bronze_files_skipped_previously_processed": skipped}

        clean_records = []
        dlq_records = []
        seen_trades = set()

        for f_path in bronze_files:
            tbl = pq.read_table(f_path)
            for row in tbl.to_pylist():
                is_valid, reason, conformed = self.validate_and_conform_record(row)
                if not is_valid:
                    bad_row = dict(row)
                    bad_row["dlq_reason"] = reason
                    bad_row["quarantined_at"] = datetime.now(timezone.utc).isoformat()
                    dlq_records.append(bad_row)
                else:
                    t_id = conformed["trade_id"]
                    if t_id not in seen_trades:
                        seen_trades.add(t_id)
                        clean_records.append(conformed)

        # Write clean Silver Parquet file
        now = datetime.now(timezone.utc)
        date_str = now.strftime("%Y-%m-%d")
        silver_partition = self.silver_dir / f"date={date_str}"
        silver_partition.mkdir(parents=True, exist_ok=True)

        # Collision-safe filenames: microsecond timestamp + uuid suffix.
        # (Second-resolution names collided when two runs landed in the
        # same second, silently overwriting the previous silver file.)
        run_id = f"{now.strftime('%H%M%S')}_{now.microsecond:06d}_{uuid.uuid4().hex[:8]}"

        if clean_records:
            silver_table = pa.Table.from_pylist(clean_records)
            silver_file = silver_partition / f"silver_trades_{run_id}.parquet"
            pq.write_table(silver_table, silver_file, compression="snappy")

        # Write Dead Letter Queue records if any
        if dlq_records:
            dlq_table = pa.Table.from_pylist(dlq_records)
            dlq_file = self.dlq_dir / f"dlq_quarantine_{run_id}.parquet"
            pq.write_table(dlq_table, dlq_file, compression="snappy")

        # Mark these bronze files as processed, and register a snapshot
        # in the lakehouse catalog (wires SnapshotCatalog into the pipeline).
        already.update(self._file_key(f) for f in bronze_files)
        self._save_manifest(already)
        try:
            catalog = SnapshotCatalog(catalog_path=self.silver_dir.parent / "snapshots_metadata.json")
            catalog.register_snapshot(
                layer="silver",
                row_count=len(clean_records),
                file_count=1 if clean_records else 0,
                operation="bronze_to_silver",
            )
        except Exception:
            pass  # snapshot registration must never break the pipeline

        return {
            "status": "SUCCESS",
            "bronze_files_scanned": len(bronze_files),
            "bronze_files_skipped_previously_processed": skipped,
            "clean_records": len(clean_records),
            "dlq_records": len(dlq_records),
            "unique_trades_committed": len(seen_trades),
        }
