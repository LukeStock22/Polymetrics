"""Python Flink SinkFunctions for writing to Snowflake via key-pair auth.

Each subtask keeps its own SnowflakeLoader connection (opened lazily on
the first invoke) and batches inserts. open()/close() are called by
Flink; invoke() is called for every record.
"""

from __future__ import annotations

import json
import logging
import os
import time
from pathlib import Path
from typing import Any

from pyflink.datastream.functions import SinkFunction

log = logging.getLogger(__name__)

_DEFAULT_FLUSH_SEC = float(os.environ.get("SNOWFLAKE_FLUSH_INTERVAL_SEC", "30"))
_DEFAULT_FLUSH_ROWS = int(os.environ.get("SNOWFLAKE_FLUSH_MAX_ROWS", "1000"))


def _load_private_key_der() -> bytes:
    from cryptography.hazmat.primitives import serialization
    path = Path(os.environ.get("SNOWFLAKE_PRIVATE_KEY_PATH", "")).expanduser()
    if not path or not path.exists():
        raise RuntimeError(f"SNOWFLAKE_PRIVATE_KEY_PATH not set or missing: {path}")
    p_key = serialization.load_pem_private_key(path.read_bytes(), password=None)
    return p_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )


def _connect():
    import snowflake.connector
    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        private_key=_load_private_key_der(),
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
        database=os.environ["SNOWFLAKE_DATABASE"],
        role=os.environ["SNOWFLAKE_ROLE"],
    )


class _BatchingSink(SinkFunction):
    """Base class that batches records and flushes on timer or row count."""

    TABLE: str = ""
    INSERT_SQL: str = ""
    COLUMNS: tuple[str, ...] = ()

    def __init__(self, flush_interval_sec: float = _DEFAULT_FLUSH_SEC,
                 flush_max_rows: int = _DEFAULT_FLUSH_ROWS) -> None:
        super().__init__()
        self._flush_interval = flush_interval_sec
        self._flush_max_rows = flush_max_rows
        self._buffer: list[tuple] = []
        self._last_flush = 0.0
        self._conn = None

    def open(self, runtime_context) -> None:  # noqa: ARG002 — Flink contract
        self._conn = _connect()
        self._last_flush = time.monotonic()
        log.info("SnowflakeSink[%s] connected", self.TABLE)

    def invoke(self, record: dict, context) -> None:  # noqa: ARG002 — Flink contract
        self._buffer.append(self._to_row(record))
        if (len(self._buffer) >= self._flush_max_rows
                or time.monotonic() - self._last_flush >= self._flush_interval):
            self._flush()

    def close(self) -> None:
        self._flush()
        if self._conn is not None:
            self._conn.close()
            self._conn = None

    def _flush(self) -> None:
        if not self._buffer or self._conn is None:
            return
        rows = self._buffer
        self._buffer = []
        try:
            cur = self._conn.cursor()
            cur.executemany(self.INSERT_SQL, rows)
            cur.close()
            log.info("SnowflakeSink[%s] flushed %d rows", self.TABLE, len(rows))
            self._last_flush = time.monotonic()
        except Exception:
            log.exception("Flush failed; re-queueing rows")
            self._buffer = rows + self._buffer

    def _to_row(self, record: dict) -> tuple:
        raise NotImplementedError


class TradeSink(_BatchingSink):
    TABLE = "CLOB_TRADES_STREAM"
    INSERT_SQL = """
        INSERT INTO DOG_SCHEMA.CLOB_TRADES_STREAM
            (asset_id, market_condition_id, side, price, size,
             fee_rate_bps, ws_timestamp, raw_payload)
        SELECT column1, column2, column3,
               column4::NUMBER(18,6), column5::NUMBER(38,12),
               column6::NUMBER(18,2), column7::TIMESTAMP_NTZ,
               PARSE_JSON(column8)
        FROM VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """

    def _to_row(self, r: dict) -> tuple:
        return (
            r["asset_id"], r["market_condition_id"], r["side"],
            r["price"], r["size"], r.get("fee_rate_bps", 0),
            r["ws_timestamp"], json.dumps(r.get("raw_payload", {})),
        )


class BookSink(_BatchingSink):
    TABLE = "CLOB_BOOK_SNAPSHOTS"
    INSERT_SQL = """
        INSERT INTO DOG_SCHEMA.CLOB_BOOK_SNAPSHOTS
            (asset_id, market_condition_id, best_bid, best_ask, spread,
             bid_depth, ask_depth, snapshot_ts, raw_payload)
        SELECT column1, column2,
               column3::NUMBER(18,6), column4::NUMBER(18,6), column5::NUMBER(18,6),
               PARSE_JSON(column6), PARSE_JSON(column7),
               column8::TIMESTAMP_NTZ, PARSE_JSON(column9)
        FROM VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    def _to_row(self, r: dict) -> tuple:
        return (
            r["asset_id"], r["market_condition_id"],
            r.get("best_bid"), r.get("best_ask"), r.get("spread"),
            json.dumps(r.get("bid_depth", [])),
            json.dumps(r.get("ask_depth", [])),
            r["snapshot_ts"],
            json.dumps(r.get("raw_payload", {})),
        )


class AnomalySink(_BatchingSink):
    TABLE = "DETECTED_ANOMALIES"
    INSERT_SQL = """
        INSERT INTO DOG_SCHEMA.DETECTED_ANOMALIES
            (detector_type, market_condition_id, asset_id,
             severity_score, details, triggering_trades)
        SELECT column1, column2, column3,
               column4::NUMBER(18,6),
               PARSE_JSON(column5), PARSE_JSON(column6)
        FROM VALUES (%s, %s, %s, %s, %s, %s)
    """

    def _to_row(self, r: dict) -> tuple:
        return (
            r["detector_type"], r["market_condition_id"], r["asset_id"],
            r["severity_score"],
            json.dumps(r.get("details", {})),
            json.dumps(r.get("triggering_trades", [])),
        )
