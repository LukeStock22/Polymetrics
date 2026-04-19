"""Kafka consumer that lands raw trades + books to Snowflake AND runs
the 4 anomaly detectors, writing detector hits to DETECTED_ANOMALIES.

Replaces kafka_consumer_to_snowflake in production: a single process
handles the raw-landing path + the detection path, keyed per-market
state held in-memory.

Usage:
    python -m polymarket_streaming.kafka_consumer_with_detectors \
        --flush-interval 30 --flush-max-rows 5000
"""

from __future__ import annotations

import argparse
import json
import logging
import signal
import time
from datetime import datetime

from confluent_kafka import Consumer, KafkaError

from polymarket_detection.scorer import AnomalyScorer
from polymarket_streaming.config import DetectorConfig, KafkaConfig, SnowflakeConfig, StreamConfig
from polymarket_streaming.message_handler import BookEvent, TradeEvent
from polymarket_streaming.snowflake_loader import SnowflakeLoader
from polymarket_streaming.subscription_manager import SubscriptionManager

log = logging.getLogger(__name__)


# ---------------- deserialization ----------------

def _parse_iso(ts: str) -> datetime:
    return datetime.fromisoformat(ts.replace("Z", "+00:00"))


def _dict_to_trade(d: dict) -> TradeEvent:
    return TradeEvent(
        asset_id=d["asset_id"],
        market=d["market_condition_id"],
        price=float(d["price"]),
        size=float(d["size"]),
        side=d["side"],
        fee_rate_bps=float(d.get("fee_rate_bps", 0)),
        timestamp=_parse_iso(d["ws_timestamp"]),
        raw=d.get("raw_payload", {}),
    )


def _dict_to_book(d: dict) -> BookEvent:
    return BookEvent(
        asset_id=d["asset_id"],
        market=d["market_condition_id"],
        bids=d.get("bid_depth", []),
        asks=d.get("ask_depth", []),
        best_bid=d.get("best_bid"),
        best_ask=d.get("best_ask"),
        spread=d.get("spread"),
        timestamp=_parse_iso(d["snapshot_ts"]),
        raw=d.get("raw_payload", {}),
    )


# ---------------- row shaping for SnowflakeLoader ----------------

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


# ---------------- main loop ----------------

class KafkaDetectingSink:
    def __init__(
        self,
        stream_config: StreamConfig,
        detector_config: DetectorConfig,
        flush_every_sec: float,
        flush_max_rows: int,
        end_date_refresh_sec: float = 3600,
    ) -> None:
        self._sc = stream_config
        self._kc = stream_config.kafka
        self._consumer = Consumer({
            "bootstrap.servers": self._kc.bootstrap_servers,
            "group.id": self._kc.consumer_group + "-detectors",
            "auto.offset.reset": "latest",
            "enable.auto.commit": False,
        })
        self._loader = SnowflakeLoader(stream_config.snowflake)
        self._scorer = AnomalyScorer(detector_config)
        self._sub_mgr = SubscriptionManager(stream_config)
        self._flush_every = flush_every_sec
        self._flush_max = flush_max_rows
        self._end_date_refresh_sec = end_date_refresh_sec
        self._last_end_date_refresh = 0.0

        # Buffered rows awaiting flush
        self._trades: list[dict] = []
        self._books: list[dict] = []
        self._anomalies: list[dict] = []

        self._last_flush = time.monotonic()
        self._running = True
        self._stats = {"trades_seen": 0, "books_seen": 0, "anomalies_flagged": 0}

    # ----- lifecycle -----

    def run(self) -> None:
        self._refresh_end_dates()
        self._consumer.subscribe([self._kc.topic_trades, self._kc.topic_books])
        log.info("Subscribed to %s, %s", self._kc.topic_trades, self._kc.topic_books)

        try:
            while self._running:
                self._maybe_refresh_end_dates()

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

                try:
                    if msg.topic() == self._kc.topic_trades:
                        self._handle_trade(payload)
                    elif msg.topic() == self._kc.topic_books:
                        self._handle_book(payload)
                except Exception:
                    log.exception("Detector error processing message on %s; continuing", msg.topic())

                self._maybe_flush()
        finally:
            self._flush_now()
            self._consumer.close()
            self._loader.close()
            log.info("Consumer stopped. Stats: %s", self._stats)

    def stop(self) -> None:
        self._running = False

    # ----- handlers -----

    def _handle_trade(self, payload: dict) -> None:
        self._stats["trades_seen"] += 1
        self._trades.append(_trade_row(payload))

        event = _dict_to_trade(payload)
        anomalies = self._scorer.score(event)
        for a in anomalies:
            self._stats["anomalies_flagged"] += 1
            self._anomalies.append({
                "detector_type": a.detector_type,
                "market_condition_id": a.market_condition_id,
                "asset_id": a.asset_id,
                "severity_score": a.severity,
                "details": json.dumps(a.details),
                "triggering_trades": json.dumps(a.triggering_trades),
            })
            log.warning(
                "ANOMALY [%s] market=%s severity=%.3f: %s",
                a.detector_type, a.market_condition_id[:16], a.severity, a.summary,
            )

    def _handle_book(self, payload: dict) -> None:
        self._stats["books_seen"] += 1
        self._books.append(_book_row(payload))
        self._scorer.update_book(_dict_to_book(payload))

    # ----- market end-date refresh (for timing-burst detector) -----

    def _maybe_refresh_end_dates(self) -> None:
        if time.monotonic() - self._last_end_date_refresh >= self._end_date_refresh_sec:
            self._refresh_end_dates()

    def _refresh_end_dates(self) -> None:
        try:
            self._sub_mgr.refresh()
            self._scorer.set_market_end_dates(self._sub_mgr.market_end_dates)
            log.info("Refreshed %d market end dates", len(self._sub_mgr.market_end_dates))
            self._last_end_date_refresh = time.monotonic()
        except Exception:
            log.exception("End-date refresh failed; will retry on next interval")
            # still mark time so we don't hammer the source
            self._last_end_date_refresh = time.monotonic()

    # ----- flush -----

    def _maybe_flush(self) -> None:
        total = len(self._trades) + len(self._books) + len(self._anomalies)
        elapsed = time.monotonic() - self._last_flush
        if total >= self._flush_max or (total > 0 and elapsed >= self._flush_every):
            self._flush_now()

    def _flush_now(self) -> None:
        if not (self._trades or self._books or self._anomalies):
            return
        trades, books, anomalies = self._trades, self._books, self._anomalies
        try:
            if trades:
                self._loader.insert_trades(trades)
            if books:
                self._loader.insert_book_snapshots(books)
            if anomalies:
                self._loader.insert_anomalies(anomalies)
            self._consumer.commit(asynchronous=False)
            log.info(
                "Flushed trades=%d books=%d anomalies=%d; offsets committed",
                len(trades), len(books), len(anomalies),
            )
            self._trades = []
            self._books = []
            self._anomalies = []
            self._last_flush = time.monotonic()
        except Exception:
            log.exception("Flush failed; offsets NOT committed, will retry")


def main() -> None:
    parser = argparse.ArgumentParser(description="Kafka → detectors → Snowflake sink")
    parser.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    parser.add_argument("--flush-interval", type=float, default=30.0)
    parser.add_argument("--flush-max-rows", type=int, default=5000)
    parser.add_argument("--end-date-refresh-sec", type=float, default=3600,
                        help="How often to refresh market end dates from Snowflake")
    args = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s %(levelname)-8s %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    sink = KafkaDetectingSink(
        stream_config=StreamConfig(),
        detector_config=DetectorConfig(),
        flush_every_sec=args.flush_interval,
        flush_max_rows=args.flush_max_rows,
        end_date_refresh_sec=args.end_date_refresh_sec,
    )

    def _shutdown(signum, frame):
        log.info("Received signal %s, stopping", signum)
        sink.stop()

    signal.signal(signal.SIGINT, _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    sink.run()


if __name__ == "__main__":
    main()
