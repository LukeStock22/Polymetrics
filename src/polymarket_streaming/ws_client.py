"""Async websocket client that streams Polymarket market data,
runs anomaly detectors on each trade, and flushes results to Snowflake.

Usage:
    python -m polymarket_streaming.ws_client [--flush-interval 30]
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import signal
import sys
import time

import websockets
import websockets.asyncio.client as ws_client

from polymarket_streaming.buffer import StreamBuffer
from polymarket_streaming.config import DetectorConfig, StreamConfig
from polymarket_streaming.message_handler import (
    BookEvent,
    MarketResolvedEvent,
    TradeEvent,
    parse_message,
)
from polymarket_streaming.snowflake_loader import SnowflakeLoader
from polymarket_streaming.subscription_manager import SubscriptionManager

from polymarket_detection.scorer import AnomalyScorer

log = logging.getLogger(__name__)


class MarketStreamer:
    """Connects to Polymarket WS, detects anomalies, writes to Snowflake."""

    def __init__(
        self,
        stream_config: StreamConfig,
        detector_config: DetectorConfig,
    ) -> None:
        self._sc = stream_config
        self._dc = detector_config
        self._sub_mgr = SubscriptionManager(stream_config)
        self._buffer = StreamBuffer(stream_config)
        self._loader = SnowflakeLoader(stream_config.snowflake)
        self._scorer = AnomalyScorer(detector_config)
        self._running = True
        self._stats = {"trades": 0, "books": 0, "anomalies": 0, "flushes": 0}

    # ---- public entry point ----

    async def run(self) -> None:
        log.info("Resolving markets to subscribe to...")
        self._sub_mgr.refresh()

        if not self._sub_mgr.asset_ids:
            log.error("No asset IDs found. Cannot start streaming.")
            return

        # Pass market end dates to timing detector
        self._scorer.set_market_end_dates(self._sub_mgr.market_end_dates)

        log.info("Subscribing to %d asset IDs", len(self._sub_mgr.asset_ids))

        backoff = self._sc.reconnect_delay_sec
        while self._running:
            try:
                await self._connect_and_stream()
                backoff = self._sc.reconnect_delay_sec  # reset on clean disconnect
            except (
                websockets.exceptions.ConnectionClosed,
                websockets.exceptions.InvalidStatus,
                OSError,
            ) as exc:
                log.warning("WS connection lost: %s — reconnecting in %.0fs", exc, backoff)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, self._sc.max_reconnect_delay_sec)
            except asyncio.CancelledError:
                log.info("Streamer cancelled, shutting down")
                break

        # Final flush
        self._buffer.flush(self._loader)
        self._loader.close()
        log.info("Streamer stopped. Stats: %s", self._stats)

    def stop(self) -> None:
        self._running = False

    # ---- internal ----

    async def _connect_and_stream(self) -> None:
        async with ws_client.connect(
            self._sc.ws_url,
            ping_interval=self._sc.ping_interval_sec,
            ping_timeout=self._sc.ping_interval_sec * 2,
            close_timeout=5,
        ) as ws:
            # Send subscription
            sub_msg = self._sub_mgr.build_subscribe_message()
            await ws.send(sub_msg)
            log.info("Subscription sent, waiting for events...")

            async for raw_msg in ws:
                if not self._running:
                    break

                events = parse_message(raw_msg)
                for event in events:
                    self._handle_event(event)

                # Periodic flush
                if self._buffer.should_flush():
                    self._buffer.flush(self._loader)
                    self._stats["flushes"] += 1

    def _handle_event(self, event) -> None:
        if isinstance(event, TradeEvent):
            self._stats["trades"] += 1
            self._buffer.add_trade(event)

            # Run anomaly detectors
            anomalies = self._scorer.score(event)
            for a in anomalies:
                self._stats["anomalies"] += 1
                self._buffer.add_anomaly(
                    detector_type=a.detector_type,
                    market_condition_id=a.market_condition_id,
                    asset_id=a.asset_id,
                    severity_score=a.severity,
                    details=a.details,
                    triggering_trades=a.triggering_trades,
                )
                log.warning(
                    "ANOMALY [%s] market=%s severity=%.3f: %s",
                    a.detector_type, a.market_condition_id[:16],
                    a.severity, a.summary,
                )

        elif isinstance(event, BookEvent):
            self._stats["books"] += 1
            self._buffer.add_book(event)
            # Update scorer with latest book state for price impact detection
            self._scorer.update_book(event)

        elif isinstance(event, MarketResolvedEvent):
            log.info("Market resolved: %s → %s", event.market[:16], event.winning_outcome)


# ---- CLI entry point ----

def main() -> None:
    parser = argparse.ArgumentParser(description="Polymarket market data streamer")
    parser.add_argument("--flush-interval", type=float, default=30, help="Seconds between Snowflake flushes")
    parser.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    args = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s %(levelname)-8s %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    stream_cfg = StreamConfig(flush_interval_sec=args.flush_interval)
    detector_cfg = DetectorConfig()

    streamer = MarketStreamer(stream_cfg, detector_cfg)

    # Graceful shutdown on SIGINT / SIGTERM
    loop = asyncio.new_event_loop()

    def _shutdown(sig):
        log.info("Received %s, stopping...", sig.name)
        streamer.stop()

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, _shutdown, sig)

    try:
        loop.run_until_complete(streamer.run())
    finally:
        loop.close()


if __name__ == "__main__":
    main()
