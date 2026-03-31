#!/usr/bin/env bash
set -euo pipefail

mkdir -p data

# Extract IDs from every saved page and dedupe.
jq -r '.[].id' data/raw/gamma/markets/*.json \
  | awk 'NF' \
  | sort -u \
  > data/market_ids.txt