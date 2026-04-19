"""In-memory buffer that collects streaming events and flushes them
to Snowflake on a schedule or when the buffer is full."""

from __future__ import annotations

import json
import logging
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from polymarket_streaming.config import StreamConfig
    from polymarket_streaming.snowflake_loader import SnowflakeLoader

from polymarket_streaming.message_handler import BookEvent, TradeEvent

log = logging.getLogger(__name__)


@dataclass
class _TradeRow:
    asset_id: str
    market_condition_id: str
    side: str
    price: float
    size: float
    fee_rate_bps: float
    ws_timestamp: str          # ISO-8601 string
    raw_payload: str           # JSON string


@dataclass
class _BookRow:
    asset_id: str
    market_condition_id: str
    best_bid: float | None
    best_ask: float | None
    spread: float | None
    bid_depth: str             # JSON string
    ask_depth: str             # JSON string
    snapshot_ts: str
    raw_payload: str


@dataclass
class _AnomalyRow:
    detector_type: str
    market_condition_id: str
    asset_id: str
    severity_score: float
    details: str               # JSON string
    triggering_trades: str     # JSON string


class StreamBuffer:
    """Collects trade, book, and anomaly events and flushes them in batches."""

    def __init__(self, config: StreamConfig) -> None:
        self._config = config
        self._trades: list[_TradeRow] = []
        self._books: list[_BookRow] = []
        self._anomalies: list[_AnomalyRow] = []
        self._last_flush = time.monotonic()

    # ---- public: add events ----

    def add_trade(self, event: TradeEvent) -> None:
        self._trades.append(_TradeRow(
            asset_id=event.asset_id,
            market_condition_id=event.market,
            side=event.side,
            price=event.price,
            size=event.size,
            fee_rate_bps=event.fee_rate_bps,
            ws_timestamp=event.timestamp.isoformat(),
            raw_payload=json.dumps(event.raw),
        ))

    def add_book(self, event: BookEvent) -> None:
        self._books.append(_BookRow(
            asset_id=event.asset_id,
            market_condition_id=event.market,
            best_bid=event.best_bid,
            best_ask=event.best_ask,
            spread=event.spread,
            bid_depth=json.dumps(event.bids[:10]),   # top 10 levels
            ask_depth=json.dumps(event.asks[:10]),
            snapshot_ts=event.timestamp.isoformat(),
            raw_payload=json.dumps(event.raw),
        ))

    def add_anomaly(
        self,
        detector_type: str,
        market_condition_id: str,
        asset_id: str,
        severity_score: float,
        details: dict,
        triggering_trades: list[dict],
    ) -> None:
        self._anomalies.append(_AnomalyRow(
            detector_type=detector_type,
            market_condition_id=market_condition_id,
            asset_id=asset_id,
            severity_score=severity_score,
            details=json.dumps(details),
            triggering_trades=json.dumps(triggering_trades),
        ))

    # ---- flush logic ----

    @property
    def total_rows(self) -> int:
        return len(self._trades) + len(self._books) + len(self._anomalies)

    def should_flush(self) -> bool:
        elapsed = time.monotonic() - self._last_flush
        if elapsed >= self._config.flush_interval_sec:
            return True
        if self.total_rows >= self._config.flush_max_rows:
            return True
        return False

    def flush(self, loader: SnowflakeLoader) -> None:
        """Write buffered rows to Snowflake and reset."""
        t0 = time.monotonic()
        trades = self._trades[:]
        books = self._books[:]
        anomalies = self._anomalies[:]

        self._trades.clear()
        self._books.clear()
        self._anomalies.clear()
        self._last_flush = time.monotonic()

        if not (trades or books or anomalies):
            return

        try:
            if trades:
                loader.insert_trades([asdict(r) for r in trades])
            if books:
                loader.insert_book_snapshots([asdict(r) for r in books])
            if anomalies:
                loader.insert_anomalies([asdict(r) for r in anomalies])

            elapsed_ms = (time.monotonic() - t0) * 1000
            log.info(
                "Flushed %d trades, %d books, %d anomalies in %.0f ms",
                len(trades), len(books), len(anomalies), elapsed_ms,
            )
        except Exception:
            # Put rows back so they are retried on the next flush.
            self._trades = trades + self._trades
            self._books = books + self._books
            self._anomalies = anomalies + self._anomalies
            log.exception("Flush to Snowflake failed; rows re-queued")
