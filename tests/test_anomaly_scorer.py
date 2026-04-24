from __future__ import annotations

from polymarket_detection.base import AnomalyEvent
import polymarket_detection.scorer as scorer_module
from polymarket_streaming.config import DetectorConfig
from polymarket_streaming.message_handler import BookEvent, TradeEvent


class FakeDetector:
    def __init__(self, name: str, should_fire: bool) -> None:
        self._name = name
        self._should_fire = should_fire
        self.book_updates = 0
        self.reset_calls = 0

    @property
    def name(self) -> str:
        return self._name

    def check(self, event: TradeEvent):
        if not self._should_fire:
            return None
        return AnomalyEvent(
            detector_type=self._name,
            market_condition_id=event.market,
            asset_id=event.asset_id,
            severity=0.9,
            summary=f"{self._name} fired",
        )

    def update_book(self, event: BookEvent) -> None:
        self.book_updates += 1

    def reset(self) -> None:
        self.reset_calls += 1


class FakeTimingDetector(FakeDetector):
    def __init__(self) -> None:
        super().__init__("timing_burst", should_fire=False)
        self.end_dates = None

    def set_end_dates(self, end_dates: dict[str, str]) -> None:
        self.end_dates = end_dates


def make_trade_event() -> TradeEvent:
    from datetime import datetime, timezone

    return TradeEvent(
        asset_id="asset-1",
        market="market-1",
        price=0.5,
        size=10.0,
        side="BUY",
        fee_rate_bps=0.0,
        timestamp=datetime(2026, 4, 20, 12, 30, tzinfo=timezone.utc),
        raw={},
    )


def make_book_event() -> BookEvent:
    from datetime import datetime, timezone

    return BookEvent(
        asset_id="asset-1",
        market="market-1",
        bids=[[0.49, 5]],
        asks=[[0.51, 7]],
        best_bid=0.49,
        best_ask=0.51,
        spread=0.02,
        timestamp=datetime(2026, 4, 20, 12, 31, tzinfo=timezone.utc),
        raw={},
    )


def test_anomaly_scorer_collects_anomalies_and_forwards_calls(monkeypatch) -> None:
    created = {}

    def make_factory(name: str, should_fire: bool):
        def factory(config):
            detector = FakeDetector(name, should_fire)
            created[name] = detector
            return detector

        return factory

    def make_timing_factory(config):
        detector = FakeTimingDetector()
        created["timing_burst"] = detector
        return detector

    monkeypatch.setattr(scorer_module, "VolumeSpikeDetector", make_factory("volume_spike", True))
    monkeypatch.setattr(scorer_module, "WhaleDetector", make_factory("whale", False))
    monkeypatch.setattr(scorer_module, "PriceImpactDetector", make_factory("price_impact", True))
    monkeypatch.setattr(scorer_module, "TimingBurstDetector", make_timing_factory)

    scorer = scorer_module.AnomalyScorer(DetectorConfig())
    scorer.set_market_end_dates({"market-1": "2026-04-20T13:00:00Z"})

    anomalies = scorer.score(make_trade_event())
    scorer.update_book(make_book_event())
    scorer.reset_all()

    assert [anomaly.detector_type for anomaly in anomalies] == [
        "volume_spike",
        "price_impact",
    ]
    assert created["timing_burst"].end_dates == {"market-1": "2026-04-20T13:00:00Z"}
    assert all(detector.book_updates == 1 for detector in created.values())
    assert all(detector.reset_calls == 1 for detector in created.values())
