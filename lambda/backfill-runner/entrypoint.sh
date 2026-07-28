#!/bin/sh
set -e

# Dispatch on --mode (default: fund_history for backward compatibility).
mode=fund_history
args=""
while [ $# -gt 0 ]; do
    case "$1" in
        --mode)
            mode="$2"
            shift 2
            ;;
        --mode=*)
            mode="${1#--mode=}"
            shift
            ;;
        *)
            args="$args $1"
            shift
            ;;
    esac
done

case "$mode" in
    fund_history)
        exec python /app/backfill_fund_history.py $args
        ;;
    portfolio)
        exec python /app/backfill_portfolio_hold.py $args
        ;;
    *)
        echo "unknown mode: $mode" >&2
        exit 1
        ;;
esac
