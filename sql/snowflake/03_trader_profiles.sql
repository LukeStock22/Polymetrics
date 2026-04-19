-- ============================================================
-- 03_trader_profiles.sql
-- Trader risk profiles and anomaly attribution.
-- Built nightly by the anomaly_attribution Airflow DAG.
-- Depends on: 01_streaming_tables, 02_anomaly_tables
-- ============================================================

-- ----- links a detected anomaly to the wallet(s) involved -----
CREATE TABLE IF NOT EXISTS DOG_TRANSFORM.ANOMALY_ATTRIBUTION (
    attribution_id          STRING          DEFAULT UUID_STRING(),
    anomaly_id              STRING          NOT NULL,
    proxy_wallet            STRING          NOT NULL,
    market_condition_id     STRING          NOT NULL,
    trade_timestamp         TIMESTAMP_NTZ,
    trade_side              STRING,
    trade_size              NUMBER(38,12),
    trade_price             NUMBER(18,6),
    transaction_hash        STRING,
    attributed_at           TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP(),
    UNIQUE (anomaly_id, proxy_wallet, transaction_hash)
);

-- ----- composite risk profile per wallet -----
CREATE TABLE IF NOT EXISTS DOG_TRANSFORM.TRADER_RISK_PROFILES (
    proxy_wallet                    STRING          PRIMARY KEY,
    leaderboard_rank                NUMBER(10,0),
    total_trades                    NUMBER(18,0),
    total_volume                    NUMBER(38,12),
    distinct_markets                NUMBER(18,0),
    avg_trade_size                  NUMBER(38,12),
    max_single_trade                NUMBER(38,12),
    win_rate                        NUMBER(18,6),
    avg_seconds_before_resolution   NUMBER(18,2),
    anomaly_involvement_count       NUMBER(18,0)    DEFAULT 0,
    anomaly_involvement_rate        NUMBER(18,6)    DEFAULT 0,
    top_detector_type               STRING,
    risk_score                      NUMBER(18,6)    DEFAULT 0,
    first_seen                      TIMESTAMP_NTZ,
    last_seen                       TIMESTAMP_NTZ,
    updated_at                      TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP()
);
