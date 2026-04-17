"""DAG: order_flow_daily_agg

Nightly aggregation of trade data into per-market and per-wallet daily
statistics.  Feeds downstream analytics and anomaly model training.

Runs daily at 06:00 UTC (after midnight ET when Polymarket activity is low).
"""

from __future__ import annotations

import os
import sys
from datetime import datetime
from pathlib import Path

from airflow.decorators import dag, task

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = PROJECT_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

SNOWFLAKE_CONN_ID = os.environ.get("POLYMARKET_SNOWFLAKE_CONN_ID", "Snowflake")
SNOWFLAKE_DATABASE = os.environ.get("POLYMARKET_SNOWFLAKE_DATABASE", "DOG_DB")


def get_snowflake_hook():
    from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
    return SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)


@dag(
    dag_id="order_flow_daily_agg",
    schedule="0 6 * * *",
    start_date=datetime(2026, 4, 1),
    catchup=False,
    default_args={"owner": "polymetrics"},
    tags=["polymarket", "order_flow", "aggregation"],
)
def order_flow_daily_agg():

    @task()
    def aggregate_market_daily_stats() -> None:
        """Compute per-market daily stats from DATA_API_TRADES."""
        hook = get_snowflake_hook()
        hook.run(f"""
            MERGE INTO {SNOWFLAKE_DATABASE}.DOG_TRANSFORM.MARKET_DAILY_STATS tgt
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
                FROM {SNOWFLAKE_DATABASE}.DOG_SCHEMA.DATA_API_TRADES
                WHERE trade_timestamp::DATE >= DATEADD('day', -1, CURRENT_DATE())
                GROUP BY condition_id, trade_timestamp::DATE
            ) src
            ON  tgt.market_condition_id = src.market_condition_id
            AND tgt.stat_date = src.stat_date
            WHEN MATCHED THEN UPDATE SET
                trade_count       = src.trade_count,
                buy_count         = src.buy_count,
                sell_count        = src.sell_count,
                total_volume      = src.total_volume,
                total_usdc_volume = src.total_usdc_volume,
                unique_wallets    = src.unique_wallets,
                avg_trade_size    = src.avg_trade_size,
                median_trade_size = src.median_trade_size,
                max_trade_size    = src.max_trade_size,
                vwap              = src.vwap,
                price_open        = src.price_open,
                price_close       = src.price_close,
                price_high        = src.price_high,
                price_low         = src.price_low,
                computed_at       = CURRENT_TIMESTAMP()
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

    @task()
    def aggregate_wallet_daily_stats() -> None:
        """Compute per-wallet daily stats from DATA_API_TRADES."""
        hook = get_snowflake_hook()
        hook.run(f"""
            MERGE INTO {SNOWFLAKE_DATABASE}.DOG_TRANSFORM.WALLET_DAILY_STATS tgt
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
                FROM {SNOWFLAKE_DATABASE}.DOG_SCHEMA.DATA_API_TRADES
                WHERE trade_timestamp::DATE >= DATEADD('day', -1, CURRENT_DATE())
                GROUP BY proxy_wallet, trade_timestamp::DATE
            ) src
            ON  tgt.proxy_wallet = src.proxy_wallet
            AND tgt.stat_date = src.stat_date
            WHEN MATCHED THEN UPDATE SET
                trade_count      = src.trade_count,
                distinct_markets = src.distinct_markets,
                total_volume     = src.total_volume,
                total_usdc_volume = src.total_usdc_volume,
                buy_volume       = src.buy_volume,
                sell_volume      = src.sell_volume,
                avg_trade_size   = src.avg_trade_size,
                max_trade_size   = src.max_trade_size,
                computed_at      = CURRENT_TIMESTAMP()
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

    @task()
    def count_daily_anomalies() -> None:
        """Roll up anomaly counts into the daily market stats."""
        hook = get_snowflake_hook()
        hook.run(f"""
            UPDATE {SNOWFLAKE_DATABASE}.DOG_TRANSFORM.MARKET_DAILY_STATS m
            SET    m.anomaly_count = sub.cnt
            FROM (
                SELECT market_condition_id,
                       detected_at::DATE AS stat_date,
                       COUNT(*)          AS cnt
                FROM   {SNOWFLAKE_DATABASE}.DOG_SCHEMA.DETECTED_ANOMALIES
                WHERE  detected_at::DATE >= DATEADD('day', -1, CURRENT_DATE())
                GROUP BY market_condition_id, detected_at::DATE
            ) sub
            WHERE m.market_condition_id = sub.market_condition_id
              AND m.stat_date = sub.stat_date
        """)

    market_stats = aggregate_market_daily_stats()
    wallet_stats = aggregate_wallet_daily_stats()
    anomaly_counts = count_daily_anomalies()
    [market_stats, wallet_stats] >> anomaly_counts


order_flow_daily_agg_dag = order_flow_daily_agg()
