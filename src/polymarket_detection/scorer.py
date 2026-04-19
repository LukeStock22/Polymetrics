"""Combines all detectors and orchestrates scoring for each trade event."""

from __future__ import annotations

from typing import TYPE_CHECKING

from polymarket_detection.base import AnomalyEvent, Detector
from polymarket_detection.price_impact import PriceImpactDetector
from polymarket_detection.timing_burst import TimingBurstDetector
from polymarket_detection.volume_spike import VolumeSpikeDetector
from polymarket_detection.whale_detector import WhaleDetector

if TYPE_CHECKING:
    from polymarket_streaming.config import DetectorConfig
    from polymarket_streaming.message_handler import BookEvent, TradeEvent


class AnomalyScorer:
    """Runs all detectors against each trade event and returns anomalies."""

    def __init__(self, config: DetectorConfig) -> None:
        self._timing = TimingBurstDetector(config)
        self._detectors: list[Detector] = [
            VolumeSpikeDetector(config),
            WhaleDetector(config),
            PriceImpactDetector(config),
            self._timing,
        ]

    def set_market_end_dates(self, end_dates: dict[str, str]) -> None:
        self._timing.set_end_dates(end_dates)

    def score(self, event: TradeEvent) -> list[AnomalyEvent]:
        """Run all detectors on a trade event. Returns list of anomalies (may be empty)."""
        anomalies = []
        for detector in self._detectors:
            result = detector.check(event)
            if result is not None:
                anomalies.append(result)
        return anomalies

    def update_book(self, event: BookEvent) -> None:
        """Forward book updates to detectors that use orderbook state."""
        for detector in self._detectors:
            detector.update_book(event)

    def reset_all(self) -> None:
        for detector in self._detectors:
            detector.reset()
