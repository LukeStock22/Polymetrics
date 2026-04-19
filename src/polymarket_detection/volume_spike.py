"""Detect sudden spikes in trade volume for a market.

Maintains a rolling short window (5 min) and a rolling long window
(1 hour) of trade volume per market.  When the short window exceeds
the long window's mean + z * stddev, flags an anomaly.
"""

from __future__ import annotations

import math
import time
from collections import defaultdict, deque
from dataclasses import dataclass
from typing import TYPE_CHECKING

from polymarket_detection.base import AnomalyEvent, Detector

if TYPE_CHECKING:
    from polymarket_streaming.config import DetectorConfig
    from polymarket_streaming.message_handler import TradeEvent


@dataclass
class _VolumeBucket:
    """A time-bucketed volume accumulator."""
    timestamp: float     # bucket start (monotonic seconds)
    trade_count: int = 0
    total_size: float = 0.0


class VolumeSpikeDetector(Detector):
    """Flag markets where short-term trade activity spikes vs the baseline."""

    def __init__(self, config: DetectorConfig) -> None:
        self._short_sec = config.volume_short_window_sec
        self._long_sec = config.volume_long_window_sec
        self._z_thresh = config.volume_z_threshold
        # market_condition_id -> deque of _VolumeBucket
        self._short: dict[str, deque[_VolumeBucket]] = defaultdict(deque)
        self._long: dict[str, deque[_VolumeBucket]] = defaultdict(deque)

    @property
    def name(self) -> str:
        return "volume_spike"

    def check(self, event: TradeEvent) -> AnomalyEvent | None:
        now = time.monotonic()
        market = event.market

        # Add to both windows
        self._push(self._short[market], now, event.size, self._short_sec)
        self._push(self._long[market], now, event.size, self._long_sec)

        short_vol = self._window_volume(self._short[market])
        long_buckets = list(self._long[market])

        if len(long_buckets) < 3:
            return None  # not enough data yet

        # Compute rolling stats from the long window, bucketed into
        # short-window-sized chunks for comparability.
        bucket_volumes = self._resample(long_buckets, self._short_sec)
        if len(bucket_volumes) < 2:
            return None

        mean = sum(bucket_volumes) / len(bucket_volumes)
        variance = sum((v - mean) ** 2 for v in bucket_volumes) / len(bucket_volumes)
        std = math.sqrt(variance) if variance > 0 else 0.0

        if std == 0:
            return None

        z_score = (short_vol - mean) / std
        if z_score < self._z_thresh:
            return None

        # Normalize severity: z=3 → 0.5, z=6 → 0.8, z=10 → ~0.95
        severity = min(1.0, 1.0 - 1.0 / (1.0 + z_score / 5.0))

        return AnomalyEvent(
            detector_type=self.name,
            market_condition_id=market,
            asset_id=event.asset_id,
            severity=severity,
            summary=(
                f"Volume spike: {short_vol:.1f} in last {self._short_sec:.0f}s "
                f"vs baseline mean {mean:.1f} (z={z_score:.2f})"
            ),
            details={
                "short_window_volume": short_vol,
                "baseline_mean": mean,
                "baseline_std": std,
                "z_score": z_score,
                "short_window_sec": self._short_sec,
            },
            triggering_trades=[event.raw],
        )

    def reset(self) -> None:
        self._short.clear()
        self._long.clear()

    # ---- helpers ----

    @staticmethod
    def _push(window: deque, now: float, size: float, max_age: float) -> None:
        window.append(_VolumeBucket(timestamp=now, trade_count=1, total_size=size))
        cutoff = now - max_age
        while window and window[0].timestamp < cutoff:
            window.popleft()

    @staticmethod
    def _window_volume(window: deque) -> float:
        return sum(b.total_size for b in window)

    @staticmethod
    def _resample(buckets: list[_VolumeBucket], interval: float) -> list[float]:
        """Re-bucket individual trade events into fixed-interval volume sums."""
        if not buckets:
            return []
        t_start = buckets[0].timestamp
        result: list[float] = []
        chunk = 0.0
        boundary = t_start + interval
        for b in buckets:
            if b.timestamp >= boundary:
                result.append(chunk)
                chunk = 0.0
                boundary = b.timestamp + interval
            chunk += b.total_size
        result.append(chunk)
        return result
