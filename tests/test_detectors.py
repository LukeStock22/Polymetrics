from __future__ import annotations

from datetime import datetime, timedelta, timezone

import polymarket_detection.timing_burst as timing_burst_module
import polymarket_detection.volume_spike as volume_spike_module
from polymarket_detection.price_impact import PriceImpactDetector
from polymarket_detection.timing_burst import TimingBurstDetector
from polymarket_detection.volume_spike import VolumeSpikeDetector
from polymarket_detection.whale_detector import WhaleDetector
from polymarket_streaming.config import DetectorConfig
from polymarket_streaming.message_handler import BookEvent, TradeEvent


def make_trade_event(*, size: float, price: float = 1.0, market: str = "market-1") -> TradeEvent:
    return TradeEvent(
        asset_id="asset-1",
        market=market,
        price=price,
        size=size,
        side="BUY",
        fee_rate_bps=0.0,
        timestamp=datetime(2026, 4, 20, 12, 30, tzinfo=timezone.utc),
        raw={"price": price, "size": size, "market": market},
    )


def make_book_event(*, best_bid: float, best_ask: float) -> BookEvent:
    return BookEvent(
        asset_id="asset-1",
        market="market-1",
        bids=[[best_bid, 10]],
        asks=[[best_ask, 10]],
        best_bid=best_bid,
        best_ask=best_ask,
        spread=best_ask - best_bid,
        timestamp=datetime(2026, 4, 20, 12, 31, tzinfo=timezone.utc),
        raw={},
    )


def test_whale_detector_flags_large_trade_after_baseline_is_established() -> None:
    detector = WhaleDetector(
        DetectorConfig(whale_percentile=99.0, whale_min_usd=100.0)
    )

    for size in range(1, 21):
        assert detector.check(make_trade_event(size=float(size), price=10.0)) is None

    anomaly = detector.check(make_trade_event(size=50.0, price=10.0))

    assert anomaly is not None
    assert anomaly.detector_type == "whale"
    assert anomaly.market_condition_id == "market-1"
    assert anomaly.details["threshold_size"] == 20.0
    assert anomaly.details["trade_usd_approx"] == 500.0


def test_volume_spike_detector_flags_spike_vs_long_window_baseline(monkeypatch) -> None:
    times = iter([i * 10.0 for i in range(21)])
    monkeypatch.setattr(volume_spike_module.time, "monotonic", lambda: next(times))

    detector = VolumeSpikeDetector(
        DetectorConfig(
            volume_short_window_sec=10.0,
            volume_long_window_sec=1_000.0,
            volume_z_threshold=3.0,
        )
    )

    for _ in range(20):
        assert detector.check(make_trade_event(size=1.0)) is None

    anomaly = detector.check(make_trade_event(size=100.0))

    assert anomaly is not None
    assert anomaly.detector_type == "volume_spike"
    assert anomaly.details["short_window_volume"] >= 100.0
    assert anomaly.details["z_score"] >= 3.0


def test_price_impact_detector_uses_book_state_and_prior_trade_price() -> None:
    detector = PriceImpactDetector(
        DetectorConfig(price_impact_threshold=0.5)
    )
    detector.update_book(make_book_event(best_bid=0.45, best_ask=0.55))

    assert detector.check(make_trade_event(size=5.0, price=0.50)) is None

    anomaly = detector.check(make_trade_event(size=5.0, price=0.62))

    assert anomaly is not None
    assert anomaly.detector_type == "price_impact"
    assert anomaly.details["prev_price"] == 0.50
    assert anomaly.details["new_price"] == 0.62
    assert anomaly.details["impact_ratio"] > 1.0


def test_timing_burst_detector_requires_resolution_window_and_baseline(monkeypatch) -> None:
    times = iter([0.0, 11.0, 25.0, 35.0, 35.0, 35.0, 35.0])
    monkeypatch.setattr(timing_burst_module.time, "monotonic", lambda: next(times))

    detector = TimingBurstDetector(
        DetectorConfig(
            timing_burst_window_sec=10.0,
            timing_resolution_horizon_sec=3_600.0,
            timing_z_threshold=3.0,
        )
    )
    detector.set_end_dates(
        {
            "market-1": (
                datetime.now(timezone.utc) + timedelta(minutes=5)
            ).isoformat()
        }
    )

    for _ in range(6):
        detector.check(make_trade_event(size=1.0))

    anomaly = detector.check(make_trade_event(size=1.0))

    assert anomaly is not None
    assert anomaly.detector_type == "timing_burst"
    assert anomaly.details["current_count"] >= 4
    assert anomaly.details["minutes_until_resolution"] <= 10
