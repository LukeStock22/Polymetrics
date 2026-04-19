"""Kafka → Snowflake sink.

Consumes the raw trade and book topics and batch-inserts into
DOG_SCHEMA.CLOB_TRADES_STREAM / DOG_SCHEMA.CLOB_BOOK_SNAPSHOTS.

This is the interim sink used while the PyFlink job is being built;
once the Flink job is live, it replaces this process as the detector
path but this consumer is still useful as a simple raw-landing sink
(or backup path).

Usage:
    python -m polymarket_streaming.kafka_consumer_to_snowflake \
        --flush-interval 30 --flush-max-rows 5000
"""

from __future__ import annotations

import argparse
import json
import logging
import signal
import time

from confluent_kafka import Consumer, KafkaError

from polymarket_streaming.config import KafkaConfig, SnowflakeConfig, StreamConfig
from polymarket_streaming.snowflake_loader import SnowflakeLoader

log = logging.getLogger(__name__)


def _trade_row(payload: dict) -> dict:
    return {
        "asset_id": payload["asset_id"],
        "market_condition_id": payload["market_condition_id"],
        "side": payload["side"],
        "price": payload["price"],
        "size": payload["size"],
        "fee_rate_bps": payload["fee_rate_bps"],
        "ws_timestamp": payload["ws_timestamp"],
        "raw_payload": json.dumps(payload.get("raw_payload", {})),
    }


def _book_row(payload: dict) -> dict:
    return {
        "asset_id": payload["asset_id"],
        "market_condition_id": payload["market_condition_id"],
        "best_bid": payload.get("best_bid"),
        "best_ask": payload.get("best_ask"),
        "spread": payload.get("spread"),
        "bid_depth": json.dumps(payload.get("bid_depth", [])),
        "ask_depth": json.dumps(payload.get("ask_depth", [])),
        "snapshot_ts": payload["snapshot_ts"],
        "raw_payload": json.dumps(payload.get("raw_payload", {})),
    }


class KafkaToSnowflake:
    def __init__(
        self,
        kafka_config: KafkaConfig,
        sf_config: SnowflakeConfig,
        flush_every_sec: float,
        flush_max_rows: int,
    ) -> None:
        self._kc = kafka_config
        self._consumer = Consumer({
            "bootstrap.servers": kafka_config.bootstrap_servers,
            "group.id": kafka_config.consumer_group,
            "auto.offset.reset": "latest",
            "enable.auto.commit": False,
        })
        self._loader = SnowflakeLoader(sf_config)
        self._flush_every = flush_every_sec
        self._flush_max = flush_max_rows
        self._trades: list[dict] = []
        self._books: list[dict] = []
        self._last_flush = time.monotonic()
        self._running = True

    def run(self) -> None:
        self._consumer.subscribe([self._kc.topic_trades, self._kc.topic_books])
        log.info("Subscribed to %s, %s", self._kc.topic_trades, self._kc.topic_books)

        try:
            while self._running:
                msg = self._consumer.poll(1.0)
                if msg is None:
                    self._maybe_flush()
                    continue
                if msg.error():
                    if msg.error().code() != KafkaError._PARTITION_EOF:
                        log.warning("Consumer error: %s", msg.error())
                    continue

                try:
                    payload = json.loads(msg.value())
                except (json.JSONDecodeError, TypeError):
                    log.warning("Bad JSON on %s offset=%s; skipping", msg.topic(), msg.offset())
                    continue

                if msg.topic() == self._kc.topic_trades:
                    self._trades.append(_trade_row(payload))
                elif msg.topic() == self._kc.topic_books:
                    self._books.append(_book_row(payload))

                self._maybe_flush()
        finally:
            self._flush_now()
            self._consumer.close()
            self._loader.close()

    def _maybe_flush(self) -> None:
        total = len(self._trades) + len(self._books)
        elapsed = time.monotonic() - self._last_flush
        if total >= self._flush_max or (total > 0 and elapsed >= self._flush_every):
            self._flush_now()

    def _flush_now(self) -> None:
        if not (self._trades or self._books):
            return
        trades, books = self._trades, self._books
        try:
            if trades:
                self._loader.insert_trades(trades)
            if books:
                self._loader.insert_book_snapshots(books)
            self._consumer.commit(asynchronous=False)
            log.info("Flushed trades=%d books=%d; offsets committed", len(trades), len(books))
            self._trades = []
            self._books = []
            self._last_flush = time.monotonic()
        except Exception:
            log.exception("Flush failed; offsets NOT committed, will retry")

    def stop(self) -> None:
        self._running = False


def main() -> None:
    parser = argparse.ArgumentParser(description="Kafka → Snowflake sink")
    parser.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    parser.add_argument("--flush-interval", type=float, default=30.0)
    parser.add_argument("--flush-max-rows", type=int, default=5000)
    args = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s %(levelname)-8s %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    sink = KafkaToSnowflake(
        KafkaConfig(),
        SnowflakeConfig(),
        flush_every_sec=args.flush_interval,
        flush_max_rows=args.flush_max_rows,
    )

    def _shutdown(signum, frame):
        log.info("Received signal %s, stopping", signum)
        sink.stop()

    signal.signal(signal.SIGINT, _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    sink.run()


if __name__ == "__main__":
    main()
