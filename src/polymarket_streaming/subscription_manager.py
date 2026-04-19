"""Manage which Polymarket markets to subscribe to on the websocket.

On startup, queries CURATED.GAMMA_MARKETS for active markets and
extracts their clob_token_ids (asset IDs).  Can refresh periodically
to pick up new markets or drop resolved ones.
"""

from __future__ import annotations

import json
import logging
from typing import Any

import snowflake.connector

from polymarket_streaming.config import StreamConfig

log = logging.getLogger(__name__)


class SubscriptionManager:
    """Resolves active market asset IDs from Snowflake or the Gamma API."""

    def __init__(self, config: StreamConfig) -> None:
        self._config = config
        self._asset_ids: list[str] = []
        # market_condition_id -> end_date for timing detector
        self.market_end_dates: dict[str, str] = {}

    @property
    def asset_ids(self) -> list[str]:
        return self._asset_ids

    def refresh_from_snowflake(self) -> list[str]:
        """Query CURATED.GAMMA_MARKETS for active markets' asset IDs.

        Orders by tracked 24h volume descending and limits to
        ``top_n_markets`` (0 = no limit). Falls back to unordered
        all-active selection if the volume column is absent.
        """
        sf = self._config.snowflake
        connect_args = dict(
            account=sf.account,
            user=sf.user,
            database=sf.database,
            warehouse=sf.warehouse,
            role=sf.role,
        )
        if sf.private_key_path:
            from cryptography.hazmat.primitives import serialization
            from pathlib import Path
            key_bytes = Path(sf.private_key_path).read_bytes()
            private_key = serialization.load_pem_private_key(key_bytes, password=None)
            connect_args["private_key"] = private_key.private_bytes(
                encoding=serialization.Encoding.DER,
                format=serialization.PrivateFormat.PKCS8,
                encryption_algorithm=serialization.NoEncryption(),
            )
        else:
            connect_args["password"] = sf.password

        conn = snowflake.connector.connect(**connect_args)
        try:
            cur = conn.cursor()
            limit_clause = f"LIMIT {int(self._config.top_n_markets)}" if self._config.top_n_markets else ""
            # Try volume-ordered query first, then fall back if column missing.
            try:
                cur.execute(f"""
                    SELECT market_id,
                           clob_token_ids,
                           end_date
                    FROM   PANTHER_DB.CURATED.GAMMA_MARKETS
                    WHERE  active = TRUE
                      AND  closed = FALSE
                      AND  clob_token_ids IS NOT NULL
                    ORDER BY COALESCE(volume_24hr, volume, 0) DESC NULLS LAST
                    {limit_clause}
                """)
            except snowflake.connector.errors.ProgrammingError:
                log.warning("Volume-ordered query failed, falling back to unordered")
                cur.execute(f"""
                    SELECT market_id,
                           clob_token_ids,
                           end_date
                    FROM   PANTHER_DB.CURATED.GAMMA_MARKETS
                    WHERE  active = TRUE
                      AND  closed = FALSE
                      AND  clob_token_ids IS NOT NULL
                    {limit_clause}
                """)
            asset_ids = []
            for row in cur:
                market_id, token_ids_raw, end_date = row
                try:
                    token_ids = json.loads(token_ids_raw) if isinstance(token_ids_raw, str) else token_ids_raw
                    if isinstance(token_ids, list):
                        for tid in token_ids:
                            asset_ids.append(str(tid))
                except (json.JSONDecodeError, TypeError):
                    continue
                if end_date is not None:
                    self.market_end_dates[market_id] = str(end_date)
            cur.close()
        finally:
            conn.close()

        self._asset_ids = asset_ids
        log.info("Loaded %d asset IDs from %d active markets", len(asset_ids), len(asset_ids) // 2)
        return asset_ids

    def refresh_from_api(self) -> list[str]:
        """Fallback: fetch active markets from the Gamma REST API."""
        import urllib.request

        url = f"{self._config.gamma_api_base}/markets?active=true&closed=false&limit=200"
        req = urllib.request.Request(url, headers={"User-Agent": "PolyMetrics/1.0"})
        with urllib.request.urlopen(req) as resp:
            markets = json.loads(resp.read())

        asset_ids = []
        for m in markets:
            token_ids = m.get("clobTokenIds") or m.get("clob_token_ids") or []
            if isinstance(token_ids, str):
                try:
                    token_ids = json.loads(token_ids)
                except json.JSONDecodeError:
                    continue
            for tid in token_ids:
                asset_ids.append(str(tid))

            end_date = m.get("endDate") or m.get("end_date")
            if end_date and m.get("id"):
                self.market_end_dates[m["id"]] = str(end_date)

        self._asset_ids = asset_ids
        log.info("Loaded %d asset IDs from Gamma API", len(asset_ids))
        return asset_ids

    def refresh(self) -> list[str]:
        """Try Snowflake first, fall back to Gamma API."""
        try:
            return self.refresh_from_snowflake()
        except Exception:
            log.warning("Snowflake refresh failed, falling back to Gamma API", exc_info=True)
            return self.refresh_from_api()

    def build_subscribe_message(self) -> str:
        """Return the JSON subscription message for the market channel."""
        ids = self._asset_ids[:self._config.max_assets_per_connection]
        return json.dumps({
            "assets_ids": ids,
            "type": "market",
            "custom_feature_enabled": True,
        })
