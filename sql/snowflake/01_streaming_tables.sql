-- ============================================================
-- 01_streaming_tables.sql
-- Tables for real-time market data streamed from Polymarket
-- CLOB websocket and Data API REST endpoints.
-- Depends on: 00_bootstrap_polymarket.sql (creates RAW schema)
-- ============================================================

-- ----- trades captured from the market channel websocket -----
-- Each row is a single last_trade_price event from the WS.
-- wallet_address is NULL at insert time; enriched later by
-- joining against DATA_API_TRADES on (conditionId, timestamp,
-- size, price).
CREATE TABLE IF NOT EXISTS DOG_SCHEMA.CLOB_TRADES_STREAM (
    stream_id               STRING          DEFAULT UUID_STRING(),
    asset_id                STRING          NOT NULL,
    market_condition_id     STRING          NOT NULL,
    side                    STRING,
    price                   NUMBER(18,6),
    size                    NUMBER(38,12),
    fee_rate_bps            NUMBER(18,2),
    ws_timestamp            TIMESTAMP_NTZ,
    wallet_address          STRING,
    enriched_at             TIMESTAMP_NTZ,
    raw_payload             VARIANT,
    streamed_at             TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP()
);

-- ----- orderbook snapshots from the market channel websocket -----
-- Periodic book events give a full snapshot of bid/ask depth.
CREATE TABLE IF NOT EXISTS DOG_SCHEMA.CLOB_BOOK_SNAPSHOTS (
    snapshot_id             STRING          DEFAULT UUID_STRING(),
    asset_id                STRING          NOT NULL,
    market_condition_id     STRING          NOT NULL,
    best_bid                NUMBER(18,6),
    best_ask                NUMBER(18,6),
    spread                  NUMBER(18,6),
    bid_depth               VARIANT,
    ask_depth               VARIANT,
    snapshot_ts             TIMESTAMP_NTZ,
    raw_payload             VARIANT,
    streamed_at             TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP()
);

-- ----- trades pulled from the Data API REST endpoint -----
-- These include the proxyWallet (trader identity) that the
-- websocket does not provide.  Loaded by the order_flow_enrichment
-- Airflow DAG on a short schedule (every 15-30 min).
CREATE TABLE IF NOT EXISTS DOG_SCHEMA.DATA_API_TRADES (
    proxy_wallet            STRING          NOT NULL,
    condition_id            STRING          NOT NULL,
    asset                   STRING,
    side                    STRING,
    outcome                 STRING,
    outcome_index           NUMBER(2,0),
    price                   NUMBER(18,6),
    size                    NUMBER(38,12),
    title                   STRING,
    slug                    STRING,
    event_slug              STRING,
    transaction_hash        STRING,
    trade_timestamp         TIMESTAMP_NTZ,
    raw_payload             VARIANT,
    loaded_at               TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP(),
    UNIQUE (transaction_hash, proxy_wallet, condition_id)
);

-- ----- leaderboard user snapshots -----
-- Periodic snapshot of Polymarket leaderboard rankings.
-- Used to identify known whales and calibrate detection thresholds.
CREATE TABLE IF NOT EXISTS DOG_SCHEMA.LEADERBOARD_USERS (
    proxy_wallet            STRING          NOT NULL,
    rank                    NUMBER(10,0),
    user_name               STRING,
    x_username              STRING,
    verified_badge          BOOLEAN,
    volume                  NUMBER(38,12),
    pnl                     NUMBER(38,12),
    profile_image           STRING,
    snapshot_date           DATE            DEFAULT CURRENT_DATE(),
    loaded_at               TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP(),
    UNIQUE (proxy_wallet, snapshot_date)
);
