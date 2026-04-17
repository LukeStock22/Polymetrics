"""Parse raw websocket JSON into typed event objects."""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional

log = logging.getLogger(__name__)


# --------------- event dataclasses ---------------

@dataclass
class TradeEvent:
    """A last_trade_price message from the market channel."""
    asset_id: str
    market: str          # condition_id
    price: float
    size: float
    side: str            # BUY or SELL
    fee_rate_bps: float
    timestamp: datetime
    raw: dict = field(repr=False, default_factory=dict)


@dataclass
class BookEvent:
    """A full orderbook snapshot from the market channel."""
    asset_id: str
    market: str
    bids: list           # list of [price, size] pairs
    asks: list
    best_bid: Optional[float]
    best_ask: Optional[float]
    spread: Optional[float]
    timestamp: datetime
    raw: dict = field(repr=False, default_factory=dict)


@dataclass
class BestBidAskEvent:
    """A best_bid_ask update (requires custom_feature_enabled)."""
    asset_id: str
    market: str
    best_bid: float
    best_ask: float
    spread: float
    timestamp: datetime
    raw: dict = field(repr=False, default_factory=dict)


@dataclass
class MarketResolvedEvent:
    """A market_resolved event (requires custom_feature_enabled)."""
    asset_id: str
    market: str
    winning_outcome: str
    timestamp: datetime
    raw: dict = field(repr=False, default_factory=dict)


@dataclass
class PriceChangeEvent:
    """A price_change event — an order added/removed from the book."""
    asset_id: str
    market: str
    price: float
    size: float          # 0 means removal
    side: str
    timestamp: datetime
    raw: dict = field(repr=False, default_factory=dict)


# --------------- parser ---------------

def _ts_now() -> datetime:
    return datetime.now(timezone.utc)


def _parse_ts(raw_ts) -> datetime:
    """Parse a timestamp that might be epoch seconds, millis, or ISO string."""
    if raw_ts is None:
        return _ts_now()
    if isinstance(raw_ts, (int, float)):
        # Heuristic: if > 1e12, treat as milliseconds
        if raw_ts > 1e12:
            return datetime.fromtimestamp(raw_ts / 1000, tz=timezone.utc)
        return datetime.fromtimestamp(raw_ts, tz=timezone.utc)
    if isinstance(raw_ts, str):
        try:
            return datetime.fromisoformat(raw_ts.replace("Z", "+00:00"))
        except ValueError:
            return _ts_now()
    return _ts_now()


def parse_message(raw_text: str) -> list:
    """Parse a raw websocket message and return a list of typed events.

    The Polymarket WS may send single JSON objects or batched arrays.
    Returns a list of event dataclasses (may be empty for heartbeats/acks).
    """
    try:
        payload = json.loads(raw_text)
    except json.JSONDecodeError:
        log.warning("Failed to decode WS message: %s", raw_text[:200])
        return []

    # Normalize: if the server sent a single object, wrap it in a list.
    if isinstance(payload, dict):
        messages = [payload]
    elif isinstance(payload, list):
        messages = payload
    else:
        return []

    results = []
    for msg in messages:
        if not isinstance(msg, dict):
            continue
        try:
            event = _parse_single(msg)
            if event is not None:
                results.append(event)
        except Exception:
            log.debug("Skipping unparseable message: %s", str(msg)[:200])
    return results


def _parse_single(msg: dict):
    """Parse one JSON object into a typed event or None."""
    event_type = msg.get("event_type") or msg.get("type") or msg.get("event")

    if event_type == "last_trade_price":
        return _parse_trade(msg)
    if event_type == "book":
        return _parse_book(msg)
    if event_type == "best_bid_ask":
        return _parse_bba(msg)
    if event_type == "market_resolved":
        return _parse_resolved(msg)
    if event_type == "price_change":
        return _parse_price_change(msg)

    # Heartbeat / ack / unknown — silently skip
    return None


def _parse_trade(msg: dict) -> TradeEvent:
    return TradeEvent(
        asset_id=str(msg.get("asset_id", "")),
        market=str(msg.get("market", msg.get("condition_id", ""))),
        price=float(msg.get("price", 0)),
        size=float(msg.get("size", 0)),
        side=str(msg.get("side", "")).upper(),
        fee_rate_bps=float(msg.get("fee_rate_bps", 0)),
        timestamp=_parse_ts(msg.get("timestamp")),
        raw=msg,
    )


def _extract_price(level) -> Optional[float]:
    """Pull a price from a book level that may be a dict or a list."""
    if isinstance(level, dict):
        return float(level.get("price", level.get("p", 0)))
    if isinstance(level, (list, tuple)) and level:
        return float(level[0])
    return None


def _parse_book(msg: dict) -> BookEvent:
    bids = msg.get("bids", [])
    asks = msg.get("asks", [])
    best_bid = _extract_price(bids[0]) if bids else None
    best_ask = _extract_price(asks[0]) if asks else None
    spread = (best_ask - best_bid) if (best_bid is not None and best_ask is not None) else None
    return BookEvent(
        asset_id=str(msg.get("asset_id", "")),
        market=str(msg.get("market", msg.get("condition_id", ""))),
        bids=bids,
        asks=asks,
        best_bid=best_bid,
        best_ask=best_ask,
        spread=spread,
        timestamp=_parse_ts(msg.get("timestamp")),
        raw=msg,
    )


def _parse_bba(msg: dict) -> BestBidAskEvent:
    bb = float(msg.get("best_bid", 0))
    ba = float(msg.get("best_ask", 0))
    return BestBidAskEvent(
        asset_id=str(msg.get("asset_id", "")),
        market=str(msg.get("market", msg.get("condition_id", ""))),
        best_bid=bb,
        best_ask=ba,
        spread=ba - bb,
        timestamp=_parse_ts(msg.get("timestamp")),
        raw=msg,
    )


def _parse_resolved(msg: dict) -> MarketResolvedEvent:
    return MarketResolvedEvent(
        asset_id=str(msg.get("winning_asset_id", msg.get("asset_id", ""))),
        market=str(msg.get("market", msg.get("condition_id", ""))),
        winning_outcome=str(msg.get("winning_outcome", "")),
        timestamp=_parse_ts(msg.get("timestamp")),
        raw=msg,
    )


def _parse_price_change(msg: dict) -> PriceChangeEvent:
    # price_change messages can contain a list of changes;
    # we return only the first one (caller can re-invoke for batches).
    changes = msg.get("price_changes") or msg.get("changes") or [msg]
    c = changes[0] if changes else msg
    return PriceChangeEvent(
        asset_id=str(c.get("asset_id", msg.get("asset_id", ""))),
        market=str(c.get("market", msg.get("market", ""))),
        price=float(c.get("price", 0)),
        size=float(c.get("size", 0)),
        side=str(c.get("side", "")).upper(),
        timestamp=_parse_ts(msg.get("timestamp")),
        raw=msg,
    )
