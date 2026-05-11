#!/usr/bin/env python3
"""Print table-creation plan for the fund_data_lake Glue database.

Run this BEFORE deploying to verify the registry is valid:
    uv run python scripts/iceberg_init_tables.py --dry-run

Or to actually create empty tables in Glue:
    uv run python scripts/iceberg_init_tables.py --apply --bucket=fund-data-...
"""
from __future__ import annotations

import argparse
import sys

sys.path.insert(0, "lambda")
from shared.schemas import TABLES  # noqa: E402


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--apply", action="store_true")
    p.add_argument("--bucket", help="S3 bucket for warehouse")
    p.add_argument("--s3-prefix", default="fund-data-pipeline/",
                   help="Key prefix under the bucket (default: fund-data-pipeline/)")
    p.add_argument("--region", default="us-east-1")
    p.add_argument("--database", default="fund_data_lake")
    args = p.parse_args()
    s3_prefix = args.s3_prefix
    if s3_prefix and not s3_prefix.endswith("/"):
        s3_prefix += "/"

    if args.dry_run == args.apply:
        p.error("specify exactly one of --dry-run or --apply")

    print(f"# {len(TABLES)} tables in registry\n")
    for name, spec in TABLES.items():
        print(f"## {name} ({spec.write_mode})")
        print(f"  PK: {spec.identifier_fields}")
        print(f"  Partition: {[f.name for f in spec.partition_spec.fields]}")
        print(f"  Schema fields: {[f.name for f in spec.schema.fields]}")
        print()

    if args.dry_run:
        return 0

    if not args.bucket:
        p.error("--apply requires --bucket")

    from shared.storage.iceberg_writer import IcebergWriter
    writer = IcebergWriter.from_glue(
        database=args.database,
        warehouse=f"s3://{args.bucket}/{s3_prefix}iceberg/",
    )
    existing_namespaces = [n[0] for n in writer.catalog.list_namespaces()]
    if args.database not in existing_namespaces:
        writer.catalog.create_namespace(args.database)

    for name, spec in TABLES.items():
        identifier = (args.database, name)
        if writer.catalog.table_exists(identifier):
            print(f"[skip] {name} (exists)")
            continue
        writer.catalog.create_table(
            identifier=identifier,
            schema=spec.schema,
            partition_spec=spec.partition_spec,
        )
        print(f"[create] {name}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
