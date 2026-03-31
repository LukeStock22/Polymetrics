#!/usr/bin/env bash
set -euo pipefail

SLEEP="${SLEEP:-0.10}"
OVERWRITE="${OVERWRITE:-0}"   # set to 1 to re-fetch everything

mkdir -p data/raw/gamma/market_details data/raw/gamma/market_tags

if [[ ! -f data/market_ids.txt ]]; then
  echo "Missing data/market_ids.txt — run scripts/01_build_market_id_list.sh first"
  exit 1
fi

fetch_one () {
  local ID="$1"
  local OUT1="data/raw/gamma/market_details/market_${ID}.json"
  local OUT2="data/raw/gamma/market_tags/market_${ID}_tags.json"

  if [[ "${OVERWRITE}" -ne 1 ]]; then
    [[ -f "${OUT1}" && -s "${OUT1}" ]] && [[ -f "${OUT2}" && -s "${OUT2}" ]] && return 0
  fi

  echo "Fetching market ${ID}"

  # Market detail
  curl -s "https://gamma-api.polymarket.com/markets/${ID}" -o "${OUT1}.tmp"
  mv "${OUT1}.tmp" "${OUT1}"

  # Market tags
  curl -s "https://gamma-api.polymarket.com/markets/${ID}/tags" -o "${OUT2}.tmp"
  mv "${OUT2}.tmp" "${OUT2}"

  sleep "${SLEEP}"
}

# Loop through IDs (idempotent because files are per-ID)
while read -r ID; do
  [[ -z "${ID}" ]] && continue
  fetch_one "${ID}"
done < data/market_ids.txt