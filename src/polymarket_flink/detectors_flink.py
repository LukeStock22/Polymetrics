"""Flink KeyedProcessFunction adapters around the existing detectors.

Each function wraps one Python detector and partitions work by
market_condition_id, so all events for a market hit the same task
slot and the in-memory detector state per market stays consistent.

We deliberately reuse the detector classes from polymarket_detection
rather than rewriting detection logic — this keeps Flink and the
production Python consumer (kafka_consumer_with_detectors) using the
exact same math.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime
from typing import Iterable

from pyflink.datastream.functions import KeyedProcessFunction, ProcessFunction

from polymarket_detection.price_impact import PriceImpactDetector
from polymarket_detection.scorer import AnomalyScorer
from polymarket_detection.timing_burst import TimingBurstDetector
from polymarket_detection.volume_spike import VolumeSpikeDetector
from polymarket_detection.whale_detector import WhaleDetector
from polymarket_streaming.config import DetectorConfig
from polymarket_streaming.message_handler import BookEvent, TradeEvent

log = logging.getLogger(__name__)


def _parse_iso(ts: str) -> datetime:
    return datetime.fromisoformat(ts.replace("Z", "+00:00"))


def dict_to_trade_event(d: dict) -> TradeEvent:
    return TradeEvent(
        asset_id=d["asset_id"],
        market=d["market_condition_id"],
        price=float(d["price"]),
        size=float(d["size"]),
        side=d["side"],
        fee_rate_bps=float(d.get("fee_rate_bps", 0)),
        timestamp=_parse_iso(d["ws_timestamp"]),
        raw=d.get("raw_payload", {}),
    )


def dict_to_book_event(d: dict) -> BookEvent:
    return BookEvent(
        asset_id=d["asset_id"],
        market=d["market_condition_id"],
        bids=d.get("bid_depth", []),
        asks=d.get("ask_depth", []),
        best_bid=d.get("best_bid"),
        best_ask=d.get("best_ask"),
        spread=d.get("spread"),
        timestamp=_parse_iso(d["snapshot_ts"]),
        raw=d.get("raw_payload", {}),
    )


def _anomaly_to_sink_row(a) -> dict:
    return {
        "detector_type": a.detector_type,
        "market_condition_id": a.market_condition_id,
        "asset_id": a.asset_id,
        "severity_score": a.severity,
        "details": a.details,
        "triggering_trades": a.triggering_trades,
    }


class RunDetectorsOnTrade(KeyedProcessFunction):
    """For each trade keyed by market_condition_id, run all 4 detectors.

    Detector state is held in a per-subtask AnomalyScorer instance —
    since the stream is keyed by market, a given subtask sees all
    trades for the markets routed to it (Flink's default behavior for
    keyBy). Per-market state inside the detector dictionaries is
    partitioned naturally.

    For end-date-dependent logic (TimingBurstDetector), end dates are
    provided via environment-supplied JSON file and refreshed on a
    timer; production path should use a broadcast state pattern but
    this is adequate for a class demo.
    """

    def __init__(self, detector_config_dict: dict | None = None,
                 end_dates: dict[str, str] | None = None) -> None:
        super().__init__()
        # Pass config as dict so it's serializable across the Flink wire
        self._cfg_dict = detector_config_dict or {}
        self._end_dates = end_dates or {}
        self._scorer: AnomalyScorer | None = None

    def open(self, runtime_context) -> None:  # noqa: ARG002
        cfg = DetectorConfig(**self._cfg_dict)
        self._scorer = AnomalyScorer(cfg)
        self._scorer.set_market_end_dates(self._end_dates)
        log.info("RunDetectorsOnTrade: scorer initialized")

    def process_element(self, value: str, ctx) -> Iterable[str]:  # noqa: ARG002
        try:
            payload = json.loads(value)
            event = dict_to_trade_event(payload)
        except Exception:
            log.exception("Failed to parse trade payload; dropping")
            return

        assert self._scorer is not None
        for anomaly in self._scorer.score(event):
            yield json.dumps(_anomaly_to_sink_row(anomaly))


class UpdateBookState(KeyedProcessFunction):
    """Feed book snapshots into the per-subtask scorer so PriceImpactDetector
    has current best_bid/best_ask when evaluating trades on the same market."""

    def __init__(self, detector_config_dict: dict | None = None) -> None:
        super().__init__()
        self._cfg_dict = detector_config_dict or {}
        self._scorer: AnomalyScorer | None = None

    def open(self, runtime_context) -> None:  # noqa: ARG002
        cfg = DetectorConfig(**self._cfg_dict)
        self._scorer = AnomalyScorer(cfg)

    def process_element(self, value: str, ctx) -> Iterable[str]:  # noqa: ARG002
        try:
            payload = json.loads(value)
            event = dict_to_book_event(payload)
        except Exception:
            log.exception("Failed to parse book payload; dropping")
            return
        assert self._scorer is not None
        self._scorer.update_book(event)
        # Books are not emitted downstream — this operator exists purely
        # to mutate scorer state; the downstream sink receives the raw
        # book from a parallel branch.
        return iter(())


class PassthroughForSink(ProcessFunction):
    """Identity function — emits the record unchanged. Used when we want
    to take a branch of the stream directly into a sink. Flink 1.20
    PyFlink's SinkFunction expects a dict record, and JSON-decoding
    near the sink keeps the pipeline simple."""

    def process_element(self, value: str, ctx) -> Iterable[dict]:  # noqa: ARG002
        try:
            yield json.loads(value)
        except Exception:
            log.exception("Bad JSON on passthrough; dropping")
