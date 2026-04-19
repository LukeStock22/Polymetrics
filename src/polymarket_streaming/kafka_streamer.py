"""Polymarket WS → Kafka streamer.

Simpler than ws_client.MarketStreamer — just forwards parsed events to
Kafka topics. Anomaly detection happens downstream (Kafka consumer
and, eventually, a PyFlink job).

Usage:
    python -m polymarket_streaming --sink kafka
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import signal

import websockets
import websockets.asyncio.client as ws_client

from polymarket_streaming.config import KafkaConfig, StreamConfig
from polymarket_streaming.kafka_publisher import KafkaPublisher
from polymarket_streaming.message_handler import (
    BookEvent,
    MarketResolvedEvent,
    TradeEvent,
    parse_message,
)
from polymarket_streaming.subscription_manager import SubscriptionManager

log = logging.getLogger(__name__)


class KafkaStreamer:
    def __init__(self, stream_config: StreamConfig) -> None:
        self._sc = stream_config
        self._sub_mgr = SubscriptionManager(stream_config)
        self._publisher = KafkaPublisher(stream_config.kafka)
        self._running = True
        self._stats = {"trades": 0, "books": 0, "resolved": 0}

    async def run(self) -> None:
        log.info("Resolving markets to subscribe to...")
        self._sub_mgr.refresh()
        if not self._sub_mgr.asset_ids:
            log.error("No asset IDs found; aborting")
            return
        log.info("Subscribing to %d asset IDs", len(self._sub_mgr.asset_ids))

        backoff = self._sc.reconnect_delay_sec
        while self._running:
            try:
                await self._connect_and_stream()
                backoff = self._sc.reconnect_delay_sec
            except (
                websockets.exceptions.ConnectionClosed,
                websockets.exceptions.InvalidStatus,
                OSError,
            ) as exc:
                log.warning("WS lost: %s; reconnecting in %.0fs", exc, backoff)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, self._sc.max_reconnect_delay_sec)
            except asyncio.CancelledError:
                log.info("Cancelled")
                break

        self._publisher.close()
        log.info("Streamer stopped. Stats: %s", {**self._stats, **self._publisher.stats})

    def stop(self) -> None:
        self._running = False

    async def _connect_and_stream(self) -> None:
        async with ws_client.connect(
            self._sc.ws_url,
            ping_interval=self._sc.ping_interval_sec,
            ping_timeout=self._sc.ping_interval_sec * 2,
            close_timeout=5,
        ) as ws:
            sub_msg = self._sub_mgr.build_subscribe_message()
            await ws.send(sub_msg)
            log.info("Subscription sent")

            async for raw_msg in ws:
                if not self._running:
                    break
                for event in parse_message(raw_msg):
                    if isinstance(event, TradeEvent):
                        self._publisher.publish_trade(event)
                        self._stats["trades"] += 1
                    elif isinstance(event, BookEvent):
                        self._publisher.publish_book(event)
                        self._stats["books"] += 1
                    elif isinstance(event, MarketResolvedEvent):
                        self._stats["resolved"] += 1
                        log.info("Market resolved: %s", event.market[:16])


def main() -> None:
    parser = argparse.ArgumentParser(description="Polymarket WS → Kafka streamer")
    parser.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    args = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s %(levelname)-8s %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    streamer = KafkaStreamer(StreamConfig())
    loop = asyncio.new_event_loop()

    def _shutdown(sig):
        log.info("Received %s, stopping", sig.name)
        streamer.stop()

    for s in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(s, _shutdown, s)

    try:
        loop.run_until_complete(streamer.run())
    finally:
        loop.close()


if __name__ == "__main__":
    main()
