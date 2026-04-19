"""Kafka producer wrapper for Polymarket streaming.

Publishes parsed TradeEvent and BookEvent instances to Kafka topics as
JSON. Uses confluent-kafka-python (librdkafka) — keys events by
market_condition_id so per-market ordering is preserved inside a
partition.
"""

from __future__ import annotations

import json
import logging
from typing import Any

from confluent_kafka import Producer

from polymarket_streaming.config import KafkaConfig
from polymarket_streaming.message_handler import BookEvent, TradeEvent

log = logging.getLogger(__name__)


def _trade_to_dict(e: TradeEvent) -> dict[str, Any]:
    return {
        "asset_id": e.asset_id,
        "market_condition_id": e.market,
        "side": e.side,
        "price": e.price,
        "size": e.size,
        "fee_rate_bps": e.fee_rate_bps,
        "ws_timestamp": e.timestamp.isoformat(),
        "raw_payload": e.raw,
    }


def _book_to_dict(e: BookEvent) -> dict[str, Any]:
    return {
        "asset_id": e.asset_id,
        "market_condition_id": e.market,
        "best_bid": e.best_bid,
        "best_ask": e.best_ask,
        "spread": e.spread,
        "bid_depth": e.bids[:10],
        "ask_depth": e.asks[:10],
        "snapshot_ts": e.timestamp.isoformat(),
        "raw_payload": e.raw,
    }


class KafkaPublisher:
    """Publishes parsed WS events to Kafka topics."""

    def __init__(self, config: KafkaConfig) -> None:
        self._config = config
        self._producer = Producer({
            "bootstrap.servers": config.bootstrap_servers,
            "client.id": config.client_id,
            "linger.ms": config.linger_ms,
            "batch.size": config.batch_size,
            "compression.type": config.compression,
            "acks": "all",
            "enable.idempotence": True,
            "retries": 10,
        })
        self._stats = {"trades": 0, "books": 0, "delivery_errors": 0}
        log.info("Kafka producer connected to %s", config.bootstrap_servers)

    def _on_delivery(self, err, msg) -> None:
        if err is not None:
            self._stats["delivery_errors"] += 1
            log.warning("Kafka delivery failed on %s: %s", msg.topic(), err)

    def publish_trade(self, event: TradeEvent) -> None:
        self._producer.produce(
            topic=self._config.topic_trades,
            key=event.market.encode("utf-8"),
            value=json.dumps(_trade_to_dict(event)).encode("utf-8"),
            callback=self._on_delivery,
        )
        self._stats["trades"] += 1
        self._producer.poll(0)

    def publish_book(self, event: BookEvent) -> None:
        self._producer.produce(
            topic=self._config.topic_books,
            key=event.market.encode("utf-8"),
            value=json.dumps(_book_to_dict(event)).encode("utf-8"),
            callback=self._on_delivery,
        )
        self._stats["books"] += 1
        self._producer.poll(0)

    def flush(self, timeout_sec: float = 10.0) -> None:
        remaining = self._producer.flush(timeout_sec)
        if remaining > 0:
            log.warning("Kafka flush timed out; %d messages still pending", remaining)

    @property
    def stats(self) -> dict:
        return dict(self._stats)

    def close(self) -> None:
        self.flush(30.0)
