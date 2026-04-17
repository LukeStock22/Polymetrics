"""Configuration for the Polymarket streaming pipeline."""

from __future__ import annotations

import os
from dataclasses import dataclass, field


@dataclass(frozen=True)
class SnowflakeConfig:
    account: str = field(default_factory=lambda: os.environ.get("SNOWFLAKE_ACCOUNT", "UNB02139"))
    user: str = field(default_factory=lambda: os.environ.get("SNOWFLAKE_USER", ""))
    password: str = field(default_factory=lambda: os.environ.get("SNOWFLAKE_PASSWORD", ""))
    private_key_path: str = field(default_factory=lambda: os.environ.get("SNOWFLAKE_PRIVATE_KEY_PATH", ""))
    passcode: str = field(default_factory=lambda: os.environ.get("SNOWFLAKE_MFA_PASSCODE", ""))
    database: str = field(default_factory=lambda: os.environ.get("SNOWFLAKE_DATABASE", "DOG_DB"))
    warehouse: str = field(default_factory=lambda: os.environ.get("SNOWFLAKE_WAREHOUSE", "DOG_WH"))
    role: str = field(default_factory=lambda: os.environ.get("SNOWFLAKE_ROLE", "TRAINING_ROLE"))


@dataclass(frozen=True)
class StreamConfig:
    # Websocket
    ws_url: str = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
    ping_interval_sec: float = 10.0
    reconnect_delay_sec: float = 5.0
    max_reconnect_delay_sec: float = 60.0

    # Buffer / flush
    flush_interval_sec: float = float(os.environ.get("STREAM_FLUSH_INTERVAL", "30"))
    flush_max_rows: int = int(os.environ.get("STREAM_FLUSH_MAX_ROWS", "5000"))

    # Subscription
    max_assets_per_connection: int = 200
    subscribe_active_only: bool = True

    # Data API (REST) for enrichment
    data_api_base: str = "https://data-api.polymarket.com"
    gamma_api_base: str = "https://gamma-api.polymarket.com"
    rest_poll_limit: int = int(os.environ.get("REST_POLL_LIMIT", "10000"))

    # Snowflake
    snowflake: SnowflakeConfig = field(default_factory=SnowflakeConfig)


@dataclass(frozen=True)
class DetectorConfig:
    # Volume spike
    volume_short_window_sec: float = float(os.environ.get("DETECTOR_VOL_SHORT_WINDOW", "300"))    # 5 min
    volume_long_window_sec: float = float(os.environ.get("DETECTOR_VOL_LONG_WINDOW", "3600"))     # 1 hour
    volume_z_threshold: float = float(os.environ.get("DETECTOR_VOL_Z_THRESHOLD", "3.0"))

    # Whale
    whale_percentile: float = float(os.environ.get("DETECTOR_WHALE_PERCENTILE", "99.0"))
    whale_min_usd: float = float(os.environ.get("DETECTOR_WHALE_MIN_USD", "10000"))

    # Price impact
    price_impact_threshold: float = float(os.environ.get("DETECTOR_PRICE_IMPACT_THRESHOLD", "0.05"))

    # Timing burst
    timing_burst_window_sec: float = float(os.environ.get("DETECTOR_TIMING_WINDOW", "300"))       # 5 min
    timing_resolution_horizon_sec: float = float(os.environ.get("DETECTOR_TIMING_HORIZON", "3600"))  # 1 hour before resolution
    timing_z_threshold: float = float(os.environ.get("DETECTOR_TIMING_Z_THRESHOLD", "3.0"))
