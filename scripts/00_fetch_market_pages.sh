#!/usr/bin/env bash
set -euo pipefail

LIMIT="${LIMIT:-200}"
SLEEP="${SLEEP:-0.15}"

fetch_pages () {
  local NAME="$1"
  local URL_BASE="$2"
  local OFFSET=412000

  while true; do
    local OUT="data/raw/gamma/markets/${NAME}_limit${LIMIT}_offset${OFFSET}.json"
    echo "GET ${OUT}"

    curl -s "${URL_BASE}&limit=${LIMIT}&offset=${OFFSET}" -o "${OUT}"

    # stop when empty array
    local COUNT
    COUNT="$(jq 'length' < "${OUT}")"
    echo "  offset=${OFFSET} count=${COUNT}"
    if [[ "${COUNT}" -eq 0 ]]; then
      rm -f "${OUT}"   # remove the final empty page
      break
    fi

    OFFSET=$((OFFSET + LIMIT))
    sleep "${SLEEP}"
  done
}

# Active + open (not closed)
fetch_pages "active_closedfalse" "https://gamma-api.polymarket.com/markets?active=true&closed=false"

# Closed markets (historical)
fetch_pages "closed_true" "https://gamma-api.polymarket.com/markets?closed=true"