"""Base class for all anomaly detectors."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from polymarket_streaming.message_handler import BookEvent, TradeEvent


@dataclass
class AnomalyEvent:
    """A single anomaly flagged by a detector."""
    detector_type: str
    market_condition_id: str
    asset_id: str
    severity: float              # 0.0 – 1.0
    summary: str                 # one-line human-readable description
    details: dict = field(default_factory=dict)
    triggering_trades: list[dict] = field(default_factory=list)


class Detector(ABC):
    """Interface that every detector must implement."""

    @property
    @abstractmethod
    def name(self) -> str:
        """Short identifier used in DETECTED_ANOMALIES.detector_type."""

    @abstractmethod
    def check(self, event: TradeEvent) -> AnomalyEvent | None:
        """Evaluate a single trade event. Return an AnomalyEvent or None."""

    def update_book(self, event: BookEvent) -> None:
        """Optional: update internal state from a book snapshot."""

    def reset(self) -> None:
        """Clear all internal state."""
