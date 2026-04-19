-- ============================================================
-- 02_anomaly_tables.sql
-- Tables for anomaly detection results and daily aggregations.
-- Depends on: 00_bootstrap (RAW + CURATED schemas)
-- ============================================================

-- ----- raw anomaly events from the streaming detectors -----
CREATE TABLE IF NOT EXISTS DOG_SCHEMA.DETECTED_ANOMALIES (
    anomaly_id              STRING          DEFAULT UUID_STRING(),
    detector_type           STRING          NOT NULL,
    market_condition_id     STRING          NOT NULL,
    asset_id                STRING,
    severity_score          NUMBER(18,6),
    details                 VARIANT,
    triggering_trades       VARIANT,
    detected_at             TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP()
);

-- ----- daily per-market stats (built by order_flow_daily_agg DAG) -----
CREATE TABLE IF NOT EXISTS DOG_TRANSFORM.MARKET_DAILY_STATS (
    market_condition_id     STRING          NOT NULL,
    stat_date               DATE            NOT NULL,
    trade_count             NUMBER(18,0),
    buy_count               NUMBER(18,0),
    sell_count              NUMBER(18,0),
    total_volume            NUMBER(38,12),
    total_usdc_volume       NUMBER(38,12),
    unique_wallets          NUMBER(18,0),
    avg_trade_size          NUMBER(38,12),
    median_trade_size       NUMBER(38,12),
    max_trade_size          NUMBER(38,12),
    vwap                    NUMBER(18,6),
    price_open              NUMBER(18,6),
    price_close             NUMBER(18,6),
    price_high              NUMBER(18,6),
    price_low               NUMBER(18,6),
    anomaly_count           NUMBER(18,0)    DEFAULT 0,
    computed_at             TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (market_condition_id, stat_date)
);

-- ----- daily per-wallet stats (built by order_flow_daily_agg DAG) -----
CREATE TABLE IF NOT EXISTS DOG_TRANSFORM.WALLET_DAILY_STATS (
    proxy_wallet            STRING          NOT NULL,
    stat_date               DATE            NOT NULL,
    trade_count             NUMBER(18,0),
    distinct_markets        NUMBER(18,0),
    total_volume            NUMBER(38,12),
    total_usdc_volume       NUMBER(38,12),
    buy_volume              NUMBER(38,12),
    sell_volume             NUMBER(38,12),
    avg_trade_size          NUMBER(38,12),
    max_trade_size          NUMBER(38,12),
    anomaly_involvement     NUMBER(18,0)    DEFAULT 0,
    computed_at             TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (proxy_wallet, stat_date)
);
