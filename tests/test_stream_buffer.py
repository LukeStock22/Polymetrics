from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

import polymarket_streaming.buffer as buffer_module
from polymarket_streaming.buffer import StreamBuffer
from polymarket_streaming.message_handler import BookEvent, TradeEvent


def make_trade_event(*, price: float = 0.5, size: float = 10.0, side: str = "BUY") -> TradeEvent:
    return TradeEvent(
        asset_id="asset-1",
        market="market-1",
        price=price,
        size=size,
        side=side,
        fee_rate_bps=5.0,
        timestamp=datetime(2026, 4, 20, 12, 30, tzinfo=timezone.utc),
        raw={"kind": "trade", "price": price, "size": size},
    )


def make_book_event() -> BookEvent:
    return BookEvent(
        asset_id="asset-1",
        market="market-1",
        bids=[[0.49, 10], [0.48, 8]],
        asks=[[0.51, 12], [0.52, 9]],
        best_bid=0.49,
        best_ask=0.51,
        spread=0.02,
        timestamp=datetime(2026, 4, 20, 12, 31, tzinfo=timezone.utc),
        raw={"kind": "book"},
    )


def make_config(**overrides):
    base = {
        "flush_interval_sec": 30.0,
        "flush_max_rows": 3,
    }
    base.update(overrides)
    return SimpleNamespace(**base)


def test_should_flush_when_row_limit_or_time_threshold_is_reached(monkeypatch) -> None:
    times = iter([100.0, 105.0, 141.0])
    monkeypatch.setattr(buffer_module.time, "monotonic", lambda: next(times))

    buf = StreamBuffer(make_config(flush_interval_sec=30.0, flush_max_rows=3))
    assert buf.should_flush() is False

    buf.add_trade(make_trade_event())
    buf.add_book(make_book_event())
    buf.add_anomaly("whale", "market-1", "asset-1", 0.9, {"z": 5}, [{"id": 1}])
    assert buf.total_rows == 3
    assert buf.should_flush() is True


def test_flush_writes_all_row_types_and_clears_buffer(monkeypatch) -> None:
    times = iter([200.0, 210.0, 220.0, 230.0])
    monkeypatch.setattr(buffer_module.time, "monotonic", lambda: next(times))

    class RecordingLoader:
        def __init__(self) -> None:
            self.trades = None
            self.books = None
            self.anomalies = None

        def insert_trades(self, rows):
            self.trades = rows

        def insert_book_snapshots(self, rows):
            self.books = rows

        def insert_anomalies(self, rows):
            self.anomalies = rows

    buf = StreamBuffer(make_config())
    buf.add_trade(make_trade_event(price=0.7, size=4))
    buf.add_book(make_book_event())
    buf.add_anomaly("price_impact", "market-1", "asset-1", 0.8, {"impact": 2.5}, [{"id": 2}])

    loader = RecordingLoader()
    buf.flush(loader)

    assert buf.total_rows == 0
    assert loader.trades[0]["market_condition_id"] == "market-1"
    assert loader.books[0]["best_bid"] == 0.49
    assert '"impact": 2.5' in loader.anomalies[0]["details"]


def test_flush_requeues_rows_when_loader_raises(monkeypatch) -> None:
    times = iter([300.0, 310.0, 320.0])
    monkeypatch.setattr(buffer_module.time, "monotonic", lambda: next(times))

    class FailingLoader:
        def insert_trades(self, rows):
            raise RuntimeError("boom")

        def insert_book_snapshots(self, rows):
            raise AssertionError("should not be reached")

        def insert_anomalies(self, rows):
            raise AssertionError("should not be reached")

    buf = StreamBuffer(make_config())
    buf.add_trade(make_trade_event())

    buf.flush(FailingLoader())

    assert buf.total_rows == 1
    requeued = buf._trades[0]
    assert requeued.asset_id == "asset-1"
    assert requeued.market_condition_id == "market-1"


def test_flush_is_noop_when_buffer_is_empty(monkeypatch) -> None:
    times = iter([400.0, 410.0, 420.0])
    monkeypatch.setattr(buffer_module.time, "monotonic", lambda: next(times))

    class Loader:
        def insert_trades(self, rows):
            raise AssertionError("should not be called")

        def insert_book_snapshots(self, rows):
            raise AssertionError("should not be called")

        def insert_anomalies(self, rows):
            raise AssertionError("should not be called")

    buf = StreamBuffer(make_config())
    buf.flush(Loader())

    assert buf.total_rows == 0
