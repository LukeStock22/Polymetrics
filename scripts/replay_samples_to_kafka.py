"""Replay saved Polymarket sample trades into local Kafka on the Mac.

Use this for Mac demos (docker-compose + Flink) so we don't need a
second live WebSocket subscription running. Reads the committed sample
CSV and produces events to polymarket.trades.raw on a loop with a
realistic ~5-events-per-second pacing.

Usage (env loaded from .env.streaming so localhost:9092 is used):
    python3 scripts/replay_samples_to_kafka.py

Stop with Ctrl-C.
"""

from __future__ import annotations

import csv
import json
import logging
import os
import signal
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from confluent_kafka import Producer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-7s %(name)s: %(message)s",
)
log = logging.getLogger("replay")

SAMPLE = Path(__file__).resolve().parents[1] / "data" / "samples" / "trades_sample_100.csv"
TOPIC = os.environ.get("KAFKA_TOPIC_TRADES", "polymarket.trades.raw")
BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
RATE = float(os.environ.get("REPLAY_RATE_PER_SEC", "5"))

_running = True
def _stop(*_):
    global _running
    _running = False
signal.signal(signal.SIGINT, _stop)
signal.signal(signal.SIGTERM, _stop)


def _csv_row_to_event(row: dict) -> dict:
    """Transform a sample CSV row into the producer's event schema."""
    return {
        "asset_id": row.get("asset", ""),
        "market_condition_id": row.get("conditionId", ""),
        "side": row.get("side", "").upper(),
        "price": float(row.get("price") or 0),
        "size": float(row.get("size") or 0),
        "fee_rate_bps": 0.0,
        "ws_timestamp": datetime.now(timezone.utc).isoformat(),
        "raw_payload": {k: v for k, v in row.items() if v},
    }


def main() -> None:
    if not SAMPLE.exists():
        log.error("Sample file missing: %s", SAMPLE)
        sys.exit(1)

    with SAMPLE.open() as f:
        rows = list(csv.DictReader(f))
    log.info("Loaded %d sample rows from %s", len(rows), SAMPLE.name)

    producer = Producer({"bootstrap.servers": BOOTSTRAP, "client.id": "replay-mac"})
    log.info("Producing to %s on %s at ~%.1f/s (Ctrl-C to stop)", TOPIC, BOOTSTRAP, RATE)

    interval = 1.0 / RATE
    sent = 0
    while _running:
        for row in rows:
            if not _running:
                break
            event = _csv_row_to_event(row)
            producer.produce(
                topic=TOPIC,
                key=event["market_condition_id"].encode(),
                value=json.dumps(event).encode(),
            )
            sent += 1
            if sent % 50 == 0:
                log.info("sent=%d", sent)
            producer.poll(0)
            time.sleep(interval)

    log.info("Flushing…")
    producer.flush(10)
    log.info("Done. sent=%d", sent)


if __name__ == "__main__":
    main()
