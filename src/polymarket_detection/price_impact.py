"""Detect trades that cause abnormally large price movements.

Compares the trade price to the most recent best bid/ask from the
orderbook.  If a single trade moves the price by more than a
configurable fraction of the spread, flag it.
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from typing import TYPE_CHECKING

from polymarket_detection.base import AnomalyEvent, Detector

if TYPE_CHECKING:
    from polymarket_streaming.config import DetectorConfig
    from polymarket_streaming.message_handler import BookEvent, TradeEvent


@dataclass
class _BookState:
    best_bid: float = 0.0
    best_ask: float = 1.0
    last_trade_price: float | None = None


class PriceImpactDetector(Detector):
    """Flag trades whose price impact exceeds a fraction of the spread."""

    def __init__(self, config: DetectorConfig) -> None:
        self._threshold = config.price_impact_threshold
        # asset_id -> latest book state
        self._books: dict[str, _BookState] = defaultdict(_BookState)

    @property
    def name(self) -> str:
        return "price_impact"

    def update_book(self, event: BookEvent) -> None:
        state = self._books[event.asset_id]
        if event.best_bid is not None:
            state.best_bid = event.best_bid
        if event.best_ask is not None:
            state.best_ask = event.best_ask

    def check(self, event: TradeEvent) -> AnomalyEvent | None:
        state = self._books[event.asset_id]
        prev_price = state.last_trade_price
        state.last_trade_price = event.price

        if prev_price is None:
            return None

        spread = state.best_ask - state.best_bid
        if spread <= 0:
            spread = 0.01  # avoid division by zero

        price_move = abs(event.price - prev_price)
        impact_ratio = price_move / spread

        if impact_ratio < self._threshold:
            return None

        # severity: scales from 0.4 at threshold to ~1.0 at 5x threshold
        severity = min(1.0, 0.3 + 0.7 * min(impact_ratio / (self._threshold * 5), 1.0))

        return AnomalyEvent(
            detector_type=self.name,
            market_condition_id=event.market,
            asset_id=event.asset_id,
            severity=severity,
            summary=(
                f"Price impact: {event.side} of {event.size:.2f} moved price "
                f"{prev_price:.4f} → {event.price:.4f} "
                f"({impact_ratio:.2f}x the spread of {spread:.4f})"
            ),
            details={
                "prev_price": prev_price,
                "new_price": event.price,
                "price_move": price_move,
                "spread": spread,
                "impact_ratio": impact_ratio,
                "side": event.side,
                "size": event.size,
            },
            triggering_trades=[event.raw],
        )

    def reset(self) -> None:
        self._books.clear()
