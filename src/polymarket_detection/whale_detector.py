"""Detect unusually large single trades (whale orders).

Maintains a running distribution of trade sizes per market using a
sorted insertion approach.  Flags trades whose size exceeds a
configurable percentile threshold.
"""

from __future__ import annotations

import bisect
from collections import defaultdict
from typing import TYPE_CHECKING

from polymarket_detection.base import AnomalyEvent, Detector

if TYPE_CHECKING:
    from polymarket_streaming.config import DetectorConfig
    from polymarket_streaming.message_handler import TradeEvent


class WhaleDetector(Detector):
    """Flag individual trades that are abnormally large for their market."""

    def __init__(self, config: DetectorConfig) -> None:
        self._percentile = config.whale_percentile / 100.0
        self._min_usd = config.whale_min_usd
        # market -> sorted list of recent trade sizes (capped at 10k entries)
        self._sizes: dict[str, list[float]] = defaultdict(list)
        self._max_history = 10_000

    @property
    def name(self) -> str:
        return "whale"

    def check(self, event: TradeEvent) -> AnomalyEvent | None:
        market = event.market
        trade_usd = event.size * event.price  # approximate USDC value
        sizes = self._sizes[market]

        result = None

        # Need some history to establish a baseline
        if len(sizes) >= 20:
            idx = int(len(sizes) * self._percentile)
            threshold = sizes[min(idx, len(sizes) - 1)]

            if event.size > threshold and trade_usd >= self._min_usd:
                pct_rank = bisect.bisect_left(sizes, event.size) / len(sizes) * 100
                # severity: scales from 0.5 at the threshold to ~1.0 for 10x the threshold
                ratio = event.size / threshold if threshold > 0 else 10.0
                severity = min(1.0, 0.4 + 0.6 * min(ratio / 10.0, 1.0))

                result = AnomalyEvent(
                    detector_type=self.name,
                    market_condition_id=market,
                    asset_id=event.asset_id,
                    severity=severity,
                    summary=(
                        f"Whale trade: size={event.size:.2f} (~${trade_usd:,.0f}) "
                        f"is {pct_rank:.1f}th percentile for this market "
                        f"(threshold={threshold:.2f})"
                    ),
                    details={
                        "trade_size": event.size,
                        "trade_usd_approx": trade_usd,
                        "percentile_rank": pct_rank,
                        "threshold_size": threshold,
                        "market_trade_count": len(sizes),
                    },
                    triggering_trades=[event.raw],
                )

        # Update history (keep it bounded)
        bisect.insort(sizes, event.size)
        if len(sizes) > self._max_history:
            sizes.pop(0)  # remove smallest to keep list bounded

        return result

    def reset(self) -> None:
        self._sizes.clear()
