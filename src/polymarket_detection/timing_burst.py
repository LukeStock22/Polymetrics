"""Detect bursts of trading activity near a market's resolution time.

Insiders often trade aggressively in the minutes/hours before a market
resolves, since they have information about the outcome.  This detector
tracks trade frequency per market and fires when:
  1. The market is within a configurable horizon of its end_date
  2. Trade frequency in the last N seconds exceeds z * baseline
"""

from __future__ import annotations

import math
import time
from collections import defaultdict, deque
from datetime import datetime, timezone
from typing import TYPE_CHECKING

from polymarket_detection.base import AnomalyEvent, Detector

if TYPE_CHECKING:
    from polymarket_streaming.config import DetectorConfig
    from polymarket_streaming.message_handler import TradeEvent


class TimingBurstDetector(Detector):
    """Flag bursts of activity on markets approaching resolution."""

    def __init__(self, config: DetectorConfig) -> None:
        self._window_sec = config.timing_burst_window_sec
        self._horizon_sec = config.timing_resolution_horizon_sec
        self._z_thresh = config.timing_z_threshold
        # market -> deque of trade timestamps (monotonic)
        self._recent: dict[str, deque[float]] = defaultdict(deque)
        # market -> list of historical window counts for baseline
        self._baselines: dict[str, list[int]] = defaultdict(list)
        self._last_baseline_snap: dict[str, float] = {}
        # market_condition_id -> end_date ISO string (set externally)
        self._end_dates: dict[str, str] = {}
        self._max_baseline_points = 500

    @property
    def name(self) -> str:
        return "timing_burst"

    def set_end_dates(self, end_dates: dict[str, str]) -> None:
        self._end_dates = end_dates

    def check(self, event: TradeEvent) -> AnomalyEvent | None:
        now = time.monotonic()
        now_utc = datetime.now(timezone.utc)
        market = event.market

        # Track the trade
        window = self._recent[market]
        window.append(now)
        cutoff = now - self._window_sec
        while window and window[0] < cutoff:
            window.popleft()

        current_count = len(window)

        # Periodically snapshot the window count as a baseline data point
        last_snap = self._last_baseline_snap.get(market, 0)
        if now - last_snap >= self._window_sec:
            baseline = self._baselines[market]
            baseline.append(current_count)
            if len(baseline) > self._max_baseline_points:
                baseline.pop(0)
            self._last_baseline_snap[market] = now

        # Only fire if the market is near its resolution time
        end_date_str = self._end_dates.get(market)
        if not end_date_str:
            return None  # unknown end date, skip

        try:
            end_dt = datetime.fromisoformat(end_date_str.replace("Z", "+00:00"))
            if end_dt.tzinfo is None:
                end_dt = end_dt.replace(tzinfo=timezone.utc)
        except (ValueError, TypeError):
            return None

        secs_until_resolution = (end_dt - now_utc).total_seconds()
        if secs_until_resolution < 0 or secs_until_resolution > self._horizon_sec:
            return None  # not within the critical window

        # Check if current burst exceeds baseline
        baseline = self._baselines.get(market, [])
        if len(baseline) < 3:
            return None

        mean = sum(baseline) / len(baseline)
        variance = sum((v - mean) ** 2 for v in baseline) / len(baseline)
        std = math.sqrt(variance) if variance > 0 else 0.0

        if std == 0:
            return None

        z_score = (current_count - mean) / std
        if z_score < self._z_thresh:
            return None

        mins_until = secs_until_resolution / 60
        severity = min(1.0, 0.5 + 0.5 * min(z_score / 6.0, 1.0))
        # Boost severity the closer we are to resolution
        if mins_until < 10:
            severity = min(1.0, severity + 0.15)

        return AnomalyEvent(
            detector_type=self.name,
            market_condition_id=market,
            asset_id=event.asset_id,
            severity=severity,
            summary=(
                f"Timing burst: {current_count} trades in last {self._window_sec:.0f}s "
                f"(z={z_score:.2f}) with resolution in {mins_until:.0f} min"
            ),
            details={
                "current_count": current_count,
                "baseline_mean": mean,
                "baseline_std": std,
                "z_score": z_score,
                "minutes_until_resolution": mins_until,
                "window_sec": self._window_sec,
            },
            triggering_trades=[event.raw],
        )

    def reset(self) -> None:
        self._recent.clear()
        self._baselines.clear()
        self._last_baseline_snap.clear()
