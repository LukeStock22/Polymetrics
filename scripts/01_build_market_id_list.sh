#!/usr/bin/env bash
set -euo pipefail

mkdir -p data
shopt -s nullglob

market_pages=(data/raw/gamma/markets/*.json)
if [[ ${#market_pages[@]} -eq 0 ]]; then
  echo "No market page files found under data/raw/gamma/markets" >&2
  exit 1
fi

# Extract IDs from every saved page and dedupe.
jq -r '.[].id' "${market_pages[@]}" \
  | awk 'NF' \
  | sort -u \
  > data/market_ids.txt
