"""Batch-insert buffered rows into Snowflake.

Runs outside of Airflow, so uses snowflake-connector-python directly
instead of SnowflakeHook.
"""

from __future__ import annotations

import logging
from contextlib import contextmanager
from typing import Any

import snowflake.connector

from polymarket_streaming.config import SnowflakeConfig

log = logging.getLogger(__name__)


class SnowflakeLoader:
    def __init__(self, config: SnowflakeConfig) -> None:
        self._config = config
        self._conn: snowflake.connector.SnowflakeConnection | None = None

    # ---- connection management ----

    def _get_conn(self) -> snowflake.connector.SnowflakeConnection:
        if self._conn is None or self._conn.is_closed():
            connect_args: dict[str, Any] = dict(
                account=self._config.account,
                user=self._config.user,
                database=self._config.database,
                warehouse=self._config.warehouse,
                role=self._config.role,
            )
            if self._config.private_key_path:
                from cryptography.hazmat.primitives import serialization
                from pathlib import Path
                key_bytes = Path(self._config.private_key_path).read_bytes()
                private_key = serialization.load_pem_private_key(key_bytes, password=None)
                connect_args["private_key"] = private_key.private_bytes(
                    encoding=serialization.Encoding.DER,
                    format=serialization.PrivateFormat.PKCS8,
                    encryption_algorithm=serialization.NoEncryption(),
                )
            else:
                connect_args["password"] = self._config.password
                if self._config.passcode:
                    connect_args["passcode"] = self._config.passcode

            self._conn = snowflake.connector.connect(**connect_args)
            log.info("Connected to Snowflake %s.%s", self._config.database, self._config.warehouse)
        return self._conn

    @contextmanager
    def _cursor(self):
        conn = self._get_conn()
        cur = conn.cursor()
        try:
            yield cur
        finally:
            cur.close()

    def close(self) -> None:
        if self._conn and not self._conn.is_closed():
            self._conn.close()

    # ---- batch inserts ----

    def insert_trades(self, rows: list[dict]) -> None:
        sql = """
            INSERT INTO DOG_SCHEMA.CLOB_TRADES_STREAM
                (asset_id, market_condition_id, side, price, size,
                 fee_rate_bps, ws_timestamp, raw_payload)
            SELECT
                column1, column2, column3,
                column4::NUMBER(18,6), column5::NUMBER(38,12),
                column6::NUMBER(18,2), column7::TIMESTAMP_NTZ,
                PARSE_JSON(column8)
            FROM VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """
        data = [
            (r["asset_id"], r["market_condition_id"], r["side"],
             r["price"], r["size"], r["fee_rate_bps"],
             r["ws_timestamp"], r["raw_payload"])
            for r in rows
        ]
        self._executemany(sql, data, "CLOB_TRADES_STREAM")

    def insert_book_snapshots(self, rows: list[dict]) -> None:
        sql = """
            INSERT INTO DOG_SCHEMA.CLOB_BOOK_SNAPSHOTS
                (asset_id, market_condition_id, best_bid, best_ask, spread,
                 bid_depth, ask_depth, snapshot_ts, raw_payload)
            SELECT
                column1, column2,
                column3::NUMBER(18,6), column4::NUMBER(18,6), column5::NUMBER(18,6),
                PARSE_JSON(column6), PARSE_JSON(column7),
                column8::TIMESTAMP_NTZ, PARSE_JSON(column9)
            FROM VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        data = [
            (r["asset_id"], r["market_condition_id"],
             r["best_bid"], r["best_ask"], r["spread"],
             r["bid_depth"], r["ask_depth"],
             r["snapshot_ts"], r["raw_payload"])
            for r in rows
        ]
        self._executemany(sql, data, "CLOB_BOOK_SNAPSHOTS")

    def insert_anomalies(self, rows: list[dict]) -> None:
        sql = """
            INSERT INTO DOG_SCHEMA.DETECTED_ANOMALIES
                (detector_type, market_condition_id, asset_id,
                 severity_score, details, triggering_trades)
            SELECT
                column1, column2, column3,
                column4::NUMBER(18,6),
                PARSE_JSON(column5), PARSE_JSON(column6)
            FROM VALUES (%s, %s, %s, %s, %s, %s)
        """
        data = [
            (r["detector_type"], r["market_condition_id"], r["asset_id"],
             r["severity_score"], r["details"], r["triggering_trades"])
            for r in rows
        ]
        self._executemany(sql, data, "DETECTED_ANOMALIES")

    def _executemany(self, sql: str, data: list[tuple], table: str) -> None:
        with self._cursor() as cur:
            cur.executemany(sql, data)
            log.debug("Inserted %d rows into %s", len(data), table)
