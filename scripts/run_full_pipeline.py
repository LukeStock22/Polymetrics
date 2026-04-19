"""Run the full analytics pipeline against Will's trade data.

1. Load trades from COYOTE_DB.PUBLIC.CURATED_POLYMARKET_USER_ACTIVITY
2. Run SQL-based anomaly detection (whale, volume spike)
3. Compute daily market/wallet aggregations
4. Build trader risk profiles

Usage:
    python scripts/run_full_pipeline.py                # default ~2M rows
    python scripts/run_full_pipeline.py --rows 5000000 # custom row count

Required env vars:
    SNOWFLAKE_USER, SNOWFLAKE_PASSWORD
"""

from __future__ import annotations

import argparse
import os
import sys
import time
from pathlib import Path

import snowflake.connector

DB = "DOG_DB"
RAW = "DOG_SCHEMA"
CUR = "DOG_TRANSFORM"
WILL_TABLE = "COYOTE_DB.PUBLIC.CURATED_POLYMARKET_USER_ACTIVITY"


def get_connection() -> snowflake.connector.SnowflakeConnection:
    user = os.environ.get("SNOWFLAKE_USER", "")
    password = os.environ.get("SNOWFLAKE_PASSWORD", "")
    if not user or not password:
        print("ERROR: SNOWFLAKE_USER and SNOWFLAKE_PASSWORD required.", file=sys.stderr)
        sys.exit(1)

    passcode = os.environ.get("SNOWFLAKE_MFA_PASSCODE", "")
    if not passcode:
        passcode = input("Enter Duo TOTP code: ").strip()

    conn = snowflake.connector.connect(
        account=os.environ.get("SNOWFLAKE_ACCOUNT", "UNB02139"),
        user=user,
        password=password,
        passcode=passcode,
        database=DB,
        warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE", "DOG_WH"),
        role=os.environ.get("SNOWFLAKE_ROLE", "TRAINING_ROLE"),
    )
    print(f"Connected to {DB} as {user}")
    return conn


def step(conn, name, sql):
    """Run a SQL step, print timing."""
    print(f"\n{'─'*60}")
    print(f"  {name}")
    print(f"{'─'*60}")
    t0 = time.time()
    cur = conn.cursor()
    try:
        cur.execute(sql)
        row = cur.fetchone()
        elapsed = time.time() - t0
        if row:
            print(f"  Result: {row}")
        print(f"  Done in {elapsed:.1f}s")
        return row
    finally:
        cur.close()


def run_pipeline(conn, max_rows: int) -> None:
    start = time.time()

    # ── Step 1: Load trades from Will's table ──
    step(conn, f"Loading up to {max_rows:,} trades from Will's COYOTE_DB", f"""
        INSERT INTO {DB}.{RAW}.DATA_API_TRADES
            (proxy_wallet, condition_id, asset, side, outcome,
             outcome_index, price, size, transaction_hash, trade_timestamp)
        SELECT
            src.PROXYWALLET,
            src.CONDITIONID,
            src.ASSET,
            src.SIDE,
            src.OUTCOME,
            src.OUTCOMEINDEX::NUMBER(2,0),
            src.PRICE::NUMBER(18,6),
            src.SIZE::NUMBER(38,12),
            src.TRANSACTIONHASH,
            TO_TIMESTAMP(src.TIMESTAMP)
        FROM {WILL_TABLE} src
        WHERE src.TYPE = 'TRADE'
          AND src.TRANSACTIONHASH IS NOT NULL
          AND NOT EXISTS (
              SELECT 1 FROM {DB}.{RAW}.DATA_API_TRADES existing
              WHERE existing.transaction_hash = src.TRANSACTIONHASH
                AND existing.proxy_wallet = src.PROXYWALLET
          )
        LIMIT {max_rows}
    """)

    step(conn, "Row count in DATA_API_TRADES",
         f"SELECT COUNT(*) FROM {DB}.{RAW}.DATA_API_TRADES")

    # ── Step 2: SQL-based anomaly detection ──

    # Whale detector: trades above 99th percentile per market AND > $10K USDC
    step(conn, "Anomaly detection: whale trades (99th pctile, >$10K)", f"""
        INSERT INTO {DB}.{RAW}.DETECTED_ANOMALIES
            (detector_type, market_condition_id, asset_id, severity_score, details)
        SELECT
            'whale',
            t.condition_id,
            t.asset,
            LEAST(1.0, 0.4 + 0.6 * LEAST(t.size / NULLIF(pctl.p99, 0) / 10.0, 1.0)),
            OBJECT_CONSTRUCT(
                'trade_size', t.size,
                'trade_usd_approx', t.size * t.price,
                'threshold_p99', pctl.p99,
                'proxy_wallet', t.proxy_wallet,
                'transaction_hash', t.transaction_hash
            )
        FROM {DB}.{RAW}.DATA_API_TRADES t
        JOIN (
            SELECT condition_id,
                   PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY size) AS p99
            FROM {DB}.{RAW}.DATA_API_TRADES
            GROUP BY condition_id
            HAVING COUNT(*) >= 20
        ) pctl ON t.condition_id = pctl.condition_id
        WHERE t.size > pctl.p99
          AND t.size * t.price >= 10000
    """)

    # Volume spike detector: markets where hourly volume > mean + 3*stddev
    step(conn, "Anomaly detection: volume spikes (z > 3.0)", f"""
        INSERT INTO {DB}.{RAW}.DETECTED_ANOMALIES
            (detector_type, market_condition_id, severity_score, details)
        WITH hourly AS (
            SELECT condition_id,
                   DATE_TRUNC('hour', trade_timestamp) AS hour_ts,
                   SUM(size) AS hour_volume,
                   COUNT(*) AS hour_trades
            FROM {DB}.{RAW}.DATA_API_TRADES
            GROUP BY condition_id, DATE_TRUNC('hour', trade_timestamp)
        ),
        baselines AS (
            SELECT condition_id,
                   AVG(hour_volume) AS mean_vol,
                   STDDEV(hour_volume) AS std_vol
            FROM hourly
            GROUP BY condition_id
            HAVING COUNT(*) >= 3 AND STDDEV(hour_volume) > 0
        )
        SELECT
            'volume_spike',
            h.condition_id,
            LEAST(1.0, 1.0 - 1.0 / (1.0 + (h.hour_volume - b.mean_vol) / b.std_vol / 5.0)),
            OBJECT_CONSTRUCT(
                'hour', h.hour_ts,
                'hour_volume', h.hour_volume,
                'hour_trades', h.hour_trades,
                'baseline_mean', b.mean_vol,
                'baseline_std', b.std_vol,
                'z_score', (h.hour_volume - b.mean_vol) / b.std_vol
            )
        FROM hourly h
        JOIN baselines b ON h.condition_id = b.condition_id
        WHERE (h.hour_volume - b.mean_vol) / b.std_vol >= 3.0
    """)

    step(conn, "Anomaly count",
         f"SELECT COUNT(*) FROM {DB}.{RAW}.DETECTED_ANOMALIES")

    # ── Step 3: Daily aggregations (from order_flow_daily_agg DAG) ──

    step(conn, "Market daily stats (MERGE)", f"""
        MERGE INTO {DB}.{CUR}.MARKET_DAILY_STATS tgt
        USING (
            SELECT
                condition_id                         AS market_condition_id,
                trade_timestamp::DATE                 AS stat_date,
                COUNT(*)                              AS trade_count,
                COUNT_IF(side = 'BUY')                AS buy_count,
                COUNT_IF(side = 'SELL')               AS sell_count,
                SUM(size)                             AS total_volume,
                SUM(size * price)                     AS total_usdc_volume,
                COUNT(DISTINCT proxy_wallet)          AS unique_wallets,
                AVG(size)                             AS avg_trade_size,
                MEDIAN(size)                          AS median_trade_size,
                MAX(size)                             AS max_trade_size,
                SUM(size * price) / NULLIF(SUM(size), 0) AS vwap,
                MIN_BY(price, trade_timestamp)        AS price_open,
                MAX_BY(price, trade_timestamp)        AS price_close,
                MAX(price)                            AS price_high,
                MIN(price)                            AS price_low
            FROM {DB}.{RAW}.DATA_API_TRADES
            GROUP BY condition_id, trade_timestamp::DATE
        ) src
        ON  tgt.market_condition_id = src.market_condition_id
        AND tgt.stat_date = src.stat_date
        WHEN MATCHED THEN UPDATE SET
            trade_count = src.trade_count, buy_count = src.buy_count,
            sell_count = src.sell_count, total_volume = src.total_volume,
            total_usdc_volume = src.total_usdc_volume, unique_wallets = src.unique_wallets,
            avg_trade_size = src.avg_trade_size, median_trade_size = src.median_trade_size,
            max_trade_size = src.max_trade_size, vwap = src.vwap,
            price_open = src.price_open, price_close = src.price_close,
            price_high = src.price_high, price_low = src.price_low,
            computed_at = CURRENT_TIMESTAMP()
        WHEN NOT MATCHED THEN INSERT (
            market_condition_id, stat_date, trade_count, buy_count, sell_count,
            total_volume, total_usdc_volume, unique_wallets, avg_trade_size,
            median_trade_size, max_trade_size, vwap, price_open, price_close,
            price_high, price_low
        ) VALUES (
            src.market_condition_id, src.stat_date, src.trade_count,
            src.buy_count, src.sell_count, src.total_volume,
            src.total_usdc_volume, src.unique_wallets, src.avg_trade_size,
            src.median_trade_size, src.max_trade_size, src.vwap,
            src.price_open, src.price_close, src.price_high, src.price_low
        )
    """)

    step(conn, "Wallet daily stats (MERGE)", f"""
        MERGE INTO {DB}.{CUR}.WALLET_DAILY_STATS tgt
        USING (
            SELECT
                proxy_wallet,
                trade_timestamp::DATE                AS stat_date,
                COUNT(*)                             AS trade_count,
                COUNT(DISTINCT condition_id)          AS distinct_markets,
                SUM(size)                            AS total_volume,
                SUM(size * price)                    AS total_usdc_volume,
                SUM(CASE WHEN side = 'BUY' THEN size ELSE 0 END)  AS buy_volume,
                SUM(CASE WHEN side = 'SELL' THEN size ELSE 0 END) AS sell_volume,
                AVG(size)                            AS avg_trade_size,
                MAX(size)                            AS max_trade_size
            FROM {DB}.{RAW}.DATA_API_TRADES
            GROUP BY proxy_wallet, trade_timestamp::DATE
        ) src
        ON  tgt.proxy_wallet = src.proxy_wallet
        AND tgt.stat_date = src.stat_date
        WHEN MATCHED THEN UPDATE SET
            trade_count = src.trade_count, distinct_markets = src.distinct_markets,
            total_volume = src.total_volume, total_usdc_volume = src.total_usdc_volume,
            buy_volume = src.buy_volume, sell_volume = src.sell_volume,
            avg_trade_size = src.avg_trade_size, max_trade_size = src.max_trade_size,
            computed_at = CURRENT_TIMESTAMP()
        WHEN NOT MATCHED THEN INSERT (
            proxy_wallet, stat_date, trade_count, distinct_markets,
            total_volume, total_usdc_volume, buy_volume, sell_volume,
            avg_trade_size, max_trade_size
        ) VALUES (
            src.proxy_wallet, src.stat_date, src.trade_count,
            src.distinct_markets, src.total_volume, src.total_usdc_volume,
            src.buy_volume, src.sell_volume, src.avg_trade_size,
            src.max_trade_size
        )
    """)

    # ── Step 4: Roll up anomaly counts into daily stats ──
    step(conn, "Roll up anomaly counts into market stats", f"""
        UPDATE {DB}.{CUR}.MARKET_DAILY_STATS m
        SET    m.anomaly_count = sub.cnt
        FROM (
            SELECT market_condition_id,
                   detected_at::DATE AS stat_date,
                   COUNT(*)          AS cnt
            FROM   {DB}.{RAW}.DETECTED_ANOMALIES
            GROUP BY market_condition_id, detected_at::DATE
        ) sub
        WHERE m.market_condition_id = sub.market_condition_id
          AND m.stat_date = sub.stat_date
    """)

    # ── Step 5: Anomaly attribution ──
    step(conn, "Anomaly attribution (link anomalies → wallets)", f"""
        MERGE INTO {DB}.{CUR}.ANOMALY_ATTRIBUTION tgt
        USING (
            SELECT
                a.anomaly_id,
                t.proxy_wallet,
                a.market_condition_id,
                t.trade_timestamp,
                t.side            AS trade_side,
                t.size            AS trade_size,
                t.price           AS trade_price,
                t.transaction_hash
            FROM {DB}.{RAW}.DETECTED_ANOMALIES a
            JOIN {DB}.{RAW}.DATA_API_TRADES t
              ON a.market_condition_id = t.condition_id
             AND ABS(DATEDIFF('second', a.detected_at, t.trade_timestamp)) <= 30
        ) src
        ON  tgt.anomaly_id = src.anomaly_id
        AND tgt.proxy_wallet = src.proxy_wallet
        AND tgt.transaction_hash = src.transaction_hash
        WHEN NOT MATCHED THEN INSERT (
            anomaly_id, proxy_wallet, market_condition_id,
            trade_timestamp, trade_side, trade_size, trade_price,
            transaction_hash
        ) VALUES (
            src.anomaly_id, src.proxy_wallet, src.market_condition_id,
            src.trade_timestamp, src.trade_side, src.trade_size,
            src.trade_price, src.transaction_hash
        )
    """)

    # ── Step 6: Trader risk profiles ──
    step(conn, "Trader risk profiles (MERGE)", f"""
        MERGE INTO {DB}.{CUR}.TRADER_RISK_PROFILES tgt
        USING (
            WITH trade_stats AS (
                SELECT
                    proxy_wallet,
                    COUNT(*)                        AS total_trades,
                    SUM(size * price)               AS total_volume,
                    COUNT(DISTINCT condition_id)     AS distinct_markets,
                    AVG(size)                        AS avg_trade_size,
                    MAX(size)                        AS max_single_trade,
                    MIN(trade_timestamp)             AS first_seen,
                    MAX(trade_timestamp)             AS last_seen
                FROM {DB}.{RAW}.DATA_API_TRADES
                GROUP BY proxy_wallet
            ),
            anomaly_stats AS (
                SELECT
                    proxy_wallet,
                    COUNT(DISTINCT anomaly_id)       AS anomaly_count
                FROM {DB}.{CUR}.ANOMALY_ATTRIBUTION
                GROUP BY proxy_wallet
            ),
            leaderboard AS (
                SELECT proxy_wallet, rank
                FROM {DB}.{RAW}.LEADERBOARD_USERS
                QUALIFY ROW_NUMBER() OVER (
                    PARTITION BY proxy_wallet ORDER BY snapshot_date DESC
                ) = 1
            )
            SELECT
                ts.proxy_wallet,
                lb.rank                              AS leaderboard_rank,
                ts.total_trades,
                ts.total_volume,
                ts.distinct_markets,
                ts.avg_trade_size,
                ts.max_single_trade,
                COALESCE(ans.anomaly_count, 0)       AS anomaly_involvement_count,
                COALESCE(ans.anomaly_count, 0)
                    / NULLIF(ts.total_trades, 0)     AS anomaly_involvement_rate,
                ts.first_seen,
                ts.last_seen
            FROM trade_stats ts
            LEFT JOIN anomaly_stats ans USING (proxy_wallet)
            LEFT JOIN leaderboard lb    USING (proxy_wallet)
        ) src
        ON tgt.proxy_wallet = src.proxy_wallet
        WHEN MATCHED THEN UPDATE SET
            leaderboard_rank = src.leaderboard_rank,
            total_trades = src.total_trades, total_volume = src.total_volume,
            distinct_markets = src.distinct_markets, avg_trade_size = src.avg_trade_size,
            max_single_trade = src.max_single_trade,
            anomaly_involvement_count = src.anomaly_involvement_count,
            anomaly_involvement_rate = src.anomaly_involvement_rate,
            first_seen = src.first_seen, last_seen = src.last_seen,
            updated_at = CURRENT_TIMESTAMP()
        WHEN NOT MATCHED THEN INSERT (
            proxy_wallet, leaderboard_rank, total_trades, total_volume,
            distinct_markets, avg_trade_size, max_single_trade,
            anomaly_involvement_count, anomaly_involvement_rate,
            first_seen, last_seen
        ) VALUES (
            src.proxy_wallet, src.leaderboard_rank, src.total_trades,
            src.total_volume, src.distinct_markets, src.avg_trade_size,
            src.max_single_trade, src.anomaly_involvement_count,
            src.anomaly_involvement_rate, src.first_seen, src.last_seen
        )
    """)

    # ── Final summary ──
    print(f"\n{'='*60}")
    print("  PIPELINE COMPLETE")
    print(f"{'='*60}")
    elapsed = time.time() - start
    print(f"  Total time: {elapsed:.0f}s ({elapsed/60:.1f} min)")

    for label, sql in [
        ("Trades", f"SELECT COUNT(*) FROM {DB}.{RAW}.DATA_API_TRADES"),
        ("Anomalies detected", f"SELECT COUNT(*) FROM {DB}.{RAW}.DETECTED_ANOMALIES"),
        ("Market daily stats", f"SELECT COUNT(*) FROM {DB}.{CUR}.MARKET_DAILY_STATS"),
        ("Wallet daily stats", f"SELECT COUNT(*) FROM {DB}.{CUR}.WALLET_DAILY_STATS"),
        ("Anomaly attributions", f"SELECT COUNT(*) FROM {DB}.{CUR}.ANOMALY_ATTRIBUTION"),
        ("Trader risk profiles", f"SELECT COUNT(*) FROM {DB}.{CUR}.TRADER_RISK_PROFILES"),
    ]:
        cur = conn.cursor()
        cur.execute(sql)
        row = cur.fetchone()
        print(f"  {label}: {row[0]:,}")
        cur.close()
    print(f"{'='*60}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--rows", type=int, default=2_000_000,
                        help="Max rows to load from Will's table (default: 2M)")
    args = parser.parse_args()

    conn = get_connection()
    try:
        run_pipeline(conn, args.rows)
    finally:
        conn.close()
        print("\nConnection closed.")


if __name__ == "__main__":
    main()
