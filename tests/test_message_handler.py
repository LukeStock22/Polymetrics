from __future__ import annotations

from datetime import datetime, timezone

import pytest

from polymarket_streaming.message_handler import (
    BestBidAskEvent,
    BookEvent,
    MarketResolvedEvent,
    PriceChangeEvent,
    TradeEvent,
    _extract_price,
    _parse_ts,
    parse_message,
)


def test_parse_ts_handles_epoch_seconds_millis_and_iso_strings() -> None:
    ts_seconds = _parse_ts(1_714_675_200)
    ts_millis = _parse_ts(1_714_675_200_000)
    ts_iso = _parse_ts("2026-04-20T12:30:00Z")

    assert ts_seconds == datetime.fromtimestamp(1_714_675_200, tz=timezone.utc)
    assert ts_millis == datetime.fromtimestamp(1_714_675_200, tz=timezone.utc)
    assert ts_iso == datetime(2026, 4, 20, 12, 30, tzinfo=timezone.utc)


def test_extract_price_supports_dicts_lists_and_missing_values() -> None:
    assert _extract_price({"price": "0.45"}) == 0.45
    assert _extract_price({"p": "0.55"}) == 0.55
    assert _extract_price([0.65, 10]) == 0.65
    assert _extract_price([]) is None
    assert _extract_price(None) is None


def test_parse_message_handles_batched_trade_and_book_events() -> None:
    events = parse_message(
        """
        [
          {
            "event_type": "last_trade_price",
            "asset_id": "asset-1",
            "market": "market-1",
            "price": 0.61,
            "size": 12,
            "side": "buy",
            "fee_rate_bps": 15,
            "timestamp": "2026-04-20T12:30:00Z"
          },
          {
            "event_type": "book",
            "asset_id": "asset-1",
            "condition_id": "market-1",
            "bids": [[0.60, 5]],
            "asks": [[0.62, 8]],
            "timestamp": 1714675200
          }
        ]
        """
    )

    assert len(events) == 2
    trade_event, book_event = events
    assert isinstance(trade_event, TradeEvent)
    assert trade_event.market == "market-1"
    assert trade_event.side == "BUY"
    assert trade_event.price == 0.61

    assert isinstance(book_event, BookEvent)
    assert book_event.market == "market-1"
    assert book_event.best_bid == 0.60
    assert book_event.best_ask == 0.62
    assert book_event.spread == pytest.approx(0.02)


def test_parse_message_handles_specialized_event_types_and_skips_unknowns() -> None:
    bba = parse_message(
        '{"event_type":"best_bid_ask","asset_id":"a1","market":"m1","best_bid":0.4,"best_ask":0.6}'
    )[0]
    resolved = parse_message(
        '{"event_type":"market_resolved","winning_asset_id":"a1","condition_id":"m1","winning_outcome":"YES"}'
    )[0]
    price_change = parse_message(
        '{"event_type":"price_change","market":"m1","timestamp":"2026-04-20T12:30:00Z","changes":[{"asset_id":"a1","price":0.7,"size":3,"side":"sell"}]}'
    )[0]

    assert isinstance(bba, BestBidAskEvent)
    assert bba.spread == pytest.approx(0.2)

    assert isinstance(resolved, MarketResolvedEvent)
    assert resolved.asset_id == "a1"
    assert resolved.market == "m1"
    assert resolved.winning_outcome == "YES"

    assert isinstance(price_change, PriceChangeEvent)
    assert price_change.asset_id == "a1"
    assert price_change.market == "m1"
    assert price_change.side == "SELL"

    assert parse_message('{"event_type":"heartbeat"}') == []


def test_parse_message_returns_empty_for_invalid_json() -> None:
    assert parse_message("{not valid json}") == []
