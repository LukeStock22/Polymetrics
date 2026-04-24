from __future__ import annotations

import importlib

import polymarket_streaming.config as config_module


def test_config_classes_pick_up_environment_overrides(monkeypatch) -> None:
    monkeypatch.setenv("SNOWFLAKE_ACCOUNT", "ACCOUNT_X")
    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
    monkeypatch.setenv("KAFKA_LINGER_MS", "125")
    monkeypatch.setenv("STREAM_FLUSH_MAX_ROWS", "999")
    monkeypatch.setenv("DETECTOR_WHALE_PERCENTILE", "97.5")

    cfg = importlib.reload(config_module)

    assert cfg.SnowflakeConfig().account == "ACCOUNT_X"
    assert cfg.KafkaConfig().bootstrap_servers == "kafka:29092"
    assert cfg.KafkaConfig().linger_ms == 125
    assert cfg.StreamConfig().flush_max_rows == 999
    assert cfg.DetectorConfig().whale_percentile == 97.5
