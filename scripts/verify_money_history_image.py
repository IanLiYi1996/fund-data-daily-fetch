"""Offline regression against an immutable production-compatible image."""
from __future__ import annotations

import argparse
import subprocess


PROGRAM = r'''
from datetime import date
from unittest.mock import Mock
import pandas as pd
import handler

target = date(2026, 11, 23)
missing = date(2026, 11, 22)
code = "001973"
names = pd.DataFrame([{
    "fund_code": code, "fund_name": "Diagnostic", "fund_type": "货币型",
}])
held = pd.DataFrame([{
    "fund_code": code, "trade_date": target, "unit_nav": 0.959,
    "ingested_at": pd.Timestamp("2026-11-23T17:00:00"),
}])
source = pd.DataFrame([
    {"fund_code": code, "trade_date": missing, "unit_nav": 0.3655},
    {"fund_code": code, "trade_date": target, "unit_nav": 0.959},
])
table = Mock()
table.scan.return_value.to_arrow.return_value.to_pandas.side_effect = lambda: held.copy()
catalog = Mock()
catalog.load_table.return_value = table
handler._fetch_batches = lambda *args: iter([[(code, source.copy(), None)]])
writer = Mock()
def write(table, frame, **kwargs):
    global held
    held = pd.concat([held, frame.assign(
        ingested_at=pd.Timestamp("2026-11-24T00:00:00"),
    )], ignore_index=True)
    return {"rows_appended": len(frame)}
writer.write.side_effect = write
first = handler._verify_money_funds(catalog, target, writer, names=names)
print("First verification:", first, flush=True)
assert writer.write.call_count == 1, "Missing Sunday was not committed"
assert list(writer.write.call_args.args[1].trade_date) == [missing]
assert first["added"] == 1 and first["corrected"] == 0
assert first["complete"] and first["errors"] == 0
second = handler._verify_money_funds(catalog, target, writer, names=names)
assert writer.write.call_count == 1, "Idempotent retry wrote a duplicate"
assert second["added"] == second["corrected"] == 0
assert second["complete"] and second["errors"] == 0
print("PASS: real production verifier fills the source hole and is idempotent.")
'''


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--image", required=True)
    args = parser.parse_args()
    subprocess.run([
        "docker", "run", "--rm", "-i", "--network", "none", "--read-only",
        "--cap-drop", "ALL", "--security-opt", "no-new-privileges",
        "--env", "WAREHOUSE_PATH=s3://diagnostic-no-write/",
        "--entrypoint", "python", args.image, "-B", "-",
    ], input=PROGRAM, text=True, check=True)


if __name__ == "__main__":
    main()
