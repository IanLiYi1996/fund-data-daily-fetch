#!/usr/bin/env bash
# Convenience wrapper for running the backfill locally from the repo root.
# All args forwarded to the script.
set -euo pipefail
cd "$(dirname "$0")/.."
exec uv run python lambda/backfill-runner/backfill_fund_history.py "$@"
