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
class KafkaConfig:
    bootstrap_servers: str = field(default_factory=lambda: os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"))
    client_id: str = field(default_factory=lambda: os.environ.get("KAFKA_CLIENT_ID", "polymarket-producer"))
    topic_trades: str = field(default_factory=lambda: os.environ.get("KAFKA_TOPIC_TRADES", "polymarket.trades.raw"))
    topic_books: str = field(default_factory=lambda: os.environ.get("KAFKA_TOPIC_BOOKS", "polymarket.books.raw"))
    topic_anomalies: str = field(default_factory=lambda: os.environ.get("KAFKA_TOPIC_ANOMALIES", "polymarket.anomalies"))
    linger_ms: int = int(os.environ.get("KAFKA_LINGER_MS", "50"))
    batch_size: int = int(os.environ.get("KAFKA_BATCH_SIZE", "65536"))
    compression: str = field(default_factory=lambda: os.environ.get("KAFKA_COMPRESSION", "lz4"))
    consumer_group: str = field(default_factory=lambda: os.environ.get("KAFKA_CONSUMER_GROUP", "polymetrics-snowflake-sink"))


@dataclass(frozen=True)
class StreamConfig:
    # Websocket
    ws_url: str = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
    ping_interval_sec: float = 10.0
    reconnect_delay_sec: float = 5.0
    max_reconnect_delay_sec: float = 60.0

    # Buffer / flush (Snowflake direct path only)
    flush_interval_sec: float = float(os.environ.get("STREAM_FLUSH_INTERVAL", "30"))
    flush_max_rows: int = int(os.environ.get("STREAM_FLUSH_MAX_ROWS", "5000"))

    # Subscription — top-N markets by 24h volume (0 = all active)
    max_assets_per_connection: int = int(os.environ.get("STREAM_MAX_ASSETS", "400"))
    top_n_markets: int = int(os.environ.get("STREAM_TOP_N_MARKETS", "200"))
    subscribe_active_only: bool = True

    # Data API (REST) for enrichment
    data_api_base: str = "https://data-api.polymarket.com"
    gamma_api_base: str = "https://gamma-api.polymarket.com"
    rest_poll_limit: int = int(os.environ.get("REST_POLL_LIMIT", "10000"))

    # Snowflake
    snowflake: SnowflakeConfig = field(default_factory=SnowflakeConfig)

    # Kafka
    kafka: KafkaConfig = field(default_factory=KafkaConfig)


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
