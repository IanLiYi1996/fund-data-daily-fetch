"""Base processor for transforming raw parquet data into MCP-ready JSON."""

import io
import json
import math
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any, Optional

import pandas as pd
from botocore.exceptions import ClientError
from shared.utils.logger import get_logger


class BaseProcessor(ABC):
    """Abstract base for data processors that read parquet and write JSON.

    Handles: S3 read/write, NaN sanitization, dual-write (latest + dated).
    """

    def __init__(self, s3_client, bucket: str, date_str: str) -> None:
        self.s3 = s3_client
        self.bucket = bucket
        self.date_str = date_str
        self.logger = get_logger(self.__class__.__name__)
        self._write_count = 0

    # ── S3 Read ─────────────────────────────────────────────────────────

    def read_parquet(self, category: str, name: str) -> Optional[pd.DataFrame]:
        """Read s3://{bucket}/{category}/{date}/{name}.parquet into DataFrame."""
        s3_key = f"{category}/{self.date_str}/{name}.parquet"
        try:
            resp = self.s3.get_object(Bucket=self.bucket, Key=s3_key)
            buf = io.BytesIO(resp["Body"].read())
            df = pd.read_parquet(buf)
            self.logger.info(f"Read {s3_key}: {len(df)} rows, {len(df.columns)} cols")
            return df
        except ClientError as e:
            if e.response["Error"]["Code"] == "NoSuchKey":
                self.logger.warning(f"Parquet not found: {s3_key}")
                return None
            self.logger.error(f"S3 error reading {s3_key}: {e}")
            return None
        except Exception as e:
            self.logger.error(f"Failed to read {s3_key}: {e}")
            return None

    # ── S3 Write (dual-write) ───────────────────────────────────────────

    def write_json(self, data: Any, path: str) -> None:
        """Dual-write wrapped JSON to data/latest/{path} and data/{date}/{path}."""
        wrapped = {
            "cached_at": datetime.now().isoformat(timespec="seconds"),
            "source": "data-processor",
            "date": self.date_str,
            "data": self.sanitize(data),
        }
        body = json.dumps(wrapped, ensure_ascii=False, separators=(",", ":")).encode("utf-8")

        for prefix in (f"data/latest/{path}", f"data/{self.date_str}/{path}"):
            self.s3.put_object(
                Bucket=self.bucket,
                Key=prefix,
                Body=body,
                ContentType="application/json; charset=utf-8",
            )
        self._write_count += 1

    def write_per_code_json(
        self,
        records: list,
        prefix: str,
        code_field: str,
        top_n: int,
        sort_field: Optional[str] = None,
        sort_ascending: bool = False,
    ) -> int:
        """Write top_n per-code JSON files under {prefix}/top/{code}.json.

        Returns the number of files written.
        """
        if not records:
            return 0

        if sort_field:
            records = sorted(
                records,
                key=lambda r: self._safe_sort_key(r.get(sort_field)),
                reverse=not sort_ascending,
            )

        written = 0
        for record in records[:top_n]:
            code = record.get(code_field)
            if not code:
                continue
            self.write_json(record, f"{prefix}/top/{code}.json")
            written += 1
        return written

    # ── Sanitization ────────────────────────────────────────────────────

    def sanitize(self, obj: Any) -> Any:
        """Recursively replace NaN/Inf with None for JSON compatibility."""
        if isinstance(obj, dict):
            return {k: self.sanitize(v) for k, v in obj.items()}
        if isinstance(obj, list):
            return [self.sanitize(v) for v in obj]
        if isinstance(obj, float):
            if math.isnan(obj) or math.isinf(obj):
                return None
        if isinstance(obj, pd.Timestamp):
            return obj.isoformat()
        return obj

    # ── DataFrame helpers ───────────────────────────────────────────────

    def df_to_records(self, df: pd.DataFrame) -> list:
        """Convert DataFrame to list of dicts with NaN → None."""
        return json.loads(df.to_json(orient="records", force_ascii=False, date_format="iso"))

    def safe_float(self, val: Any) -> Optional[float]:
        """Convert to float safely, returning None for invalid values."""
        try:
            f = float(val)
            if math.isnan(f) or math.isinf(f):
                return None
            return round(f, 4)
        except (TypeError, ValueError):
            return None

    # ── Processing entry point ──────────────────────────────────────────

    @abstractmethod
    def process(self) -> dict:
        """Run processing. Return summary dict with counts/stats."""

    # ── Private helpers ─────────────────────────────────────────────────

    @staticmethod
    def _safe_sort_key(val: Any) -> float:
        """Return a sortable float; NaN/None → -inf."""
        try:
            f = float(val)
            if math.isnan(f) or math.isinf(f):
                return float("-inf")
            return f
        except (TypeError, ValueError):
            return float("-inf")
