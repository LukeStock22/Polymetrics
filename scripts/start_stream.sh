#!/usr/bin/env bash
# ============================================================
# start_stream.sh — Launch the Polymarket market data streamer.
#
# This runs outside Airflow as a long-lived process.
# The streamer connects to the Polymarket CLOB websocket,
# runs anomaly detectors on each trade, and flushes results
# to Snowflake every --flush-interval seconds.
#
# Usage:
#   ./scripts/start_stream.sh              # defaults
#   ./scripts/start_stream.sh --flush-interval 60 --log-level DEBUG
#
# Prerequisites:
#   pip install -r requirements-streaming.txt
#   Export Snowflake credentials (see below).
# ============================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# ---- Snowflake credentials (edit or export before running) ----
export SNOWFLAKE_ACCOUNT="${SNOWFLAKE_ACCOUNT:-UNB02139}"
export SNOWFLAKE_USER="${SNOWFLAKE_USER:?Set SNOWFLAKE_USER}"
export SNOWFLAKE_PASSWORD="${SNOWFLAKE_PASSWORD:-}"
export SNOWFLAKE_PRIVATE_KEY_PATH="${SNOWFLAKE_PRIVATE_KEY_PATH:-}"
export SNOWFLAKE_DATABASE="${SNOWFLAKE_DATABASE:-DOG_DB}"
export SNOWFLAKE_WAREHOUSE="${SNOWFLAKE_WAREHOUSE:-DOG_WH}"
export SNOWFLAKE_ROLE="${SNOWFLAKE_ROLE:-TRAINING_ROLE}"

# ---- MFA: prompt for Duo TOTP if not set ----
if [ -z "${SNOWFLAKE_MFA_PASSCODE:-}" ] && [ -z "${SNOWFLAKE_PRIVATE_KEY_PATH:-}" ]; then
    read -rp "Enter Duo TOTP code: " SNOWFLAKE_MFA_PASSCODE
    export SNOWFLAKE_MFA_PASSCODE
fi

# ---- Ensure src/ is on PYTHONPATH ----
export PYTHONPATH="${PROJECT_ROOT}/src:${PYTHONPATH:-}"

echo "Starting Polymarket streamer..."
echo "  Project root: $PROJECT_ROOT"
echo "  Snowflake:    $SNOWFLAKE_ACCOUNT / $SNOWFLAKE_DATABASE"
echo "  Extra args:   $*"

exec python -m polymarket_streaming "$@"
