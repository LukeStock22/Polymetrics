"""DAG: anomaly_attribution

Nightly job that links detected anomalies to specific wallets by
joining anomaly events with enriched trade data.  Updates the
TRADER_RISK_PROFILES table with cumulative risk metrics.

Runs daily at 08:00 UTC (after model training and enrichment are done).
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
    dag_id="anomaly_attribution",
    schedule="0 8 * * *",
    start_date=datetime(2026, 4, 1),
    catchup=False,
    default_args={"owner": "polymetrics"},
    tags=["polymarket", "anomaly", "attribution"],
)
def anomaly_attribution():

    @task()
    def attribute_anomalies_to_wallets() -> dict:
        """Join detected anomalies with trade data to identify involved wallets."""
        hook = get_snowflake_hook()
        hook.run(f"""
            MERGE INTO {SNOWFLAKE_DATABASE}.DOG_TRANSFORM.ANOMALY_ATTRIBUTION tgt
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
                FROM {SNOWFLAKE_DATABASE}.DOG_SCHEMA.DETECTED_ANOMALIES a
                JOIN {SNOWFLAKE_DATABASE}.DOG_SCHEMA.DATA_API_TRADES t
                  ON a.market_condition_id = t.condition_id
                 AND ABS(DATEDIFF('second', a.detected_at, t.trade_timestamp)) <= 30
                WHERE a.detected_at >= DATEADD('day', -2, CURRENT_TIMESTAMP())
                  AND NOT EXISTS (
                      SELECT 1 FROM {SNOWFLAKE_DATABASE}.DOG_TRANSFORM.ANOMALY_ATTRIBUTION x
                      WHERE x.anomaly_id = a.anomaly_id
                        AND x.proxy_wallet = t.proxy_wallet
                        AND x.transaction_hash = t.transaction_hash
                  )
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

        count = hook.get_first(f"""
            SELECT COUNT(*) FROM {SNOWFLAKE_DATABASE}.DOG_TRANSFORM.ANOMALY_ATTRIBUTION
            WHERE attributed_at >= DATEADD('day', -1, CURRENT_TIMESTAMP())
        """)
        return {"new_attributions": count[0] if count else 0}

    @task()
    def update_trader_risk_profiles() -> dict:
        """Rebuild risk profiles from all available trade and anomaly data."""
        hook = get_snowflake_hook()
        hook.run(f"""
            MERGE INTO {SNOWFLAKE_DATABASE}.DOG_TRANSFORM.TRADER_RISK_PROFILES tgt
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
                    FROM {SNOWFLAKE_DATABASE}.DOG_SCHEMA.DATA_API_TRADES
                    GROUP BY proxy_wallet
                ),
                anomaly_stats AS (
                    SELECT
                        proxy_wallet,
                        COUNT(DISTINCT anomaly_id)       AS anomaly_count
                    FROM {SNOWFLAKE_DATABASE}.DOG_TRANSFORM.ANOMALY_ATTRIBUTION
                    GROUP BY proxy_wallet
                ),
                leaderboard AS (
                    SELECT proxy_wallet, rank
                    FROM {SNOWFLAKE_DATABASE}.DOG_SCHEMA.LEADERBOARD_USERS
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
                leaderboard_rank          = src.leaderboard_rank,
                total_trades              = src.total_trades,
                total_volume              = src.total_volume,
                distinct_markets          = src.distinct_markets,
                avg_trade_size            = src.avg_trade_size,
                max_single_trade          = src.max_single_trade,
                anomaly_involvement_count = src.anomaly_involvement_count,
                anomaly_involvement_rate  = src.anomaly_involvement_rate,
                first_seen                = src.first_seen,
                last_seen                 = src.last_seen,
                updated_at                = CURRENT_TIMESTAMP()
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

        count = hook.get_first(f"""
            SELECT COUNT(*) FROM {SNOWFLAKE_DATABASE}.DOG_TRANSFORM.TRADER_RISK_PROFILES
        """)
        return {"total_profiles": count[0] if count else 0}

    attrib = attribute_anomalies_to_wallets()
    profiles = update_trader_risk_profiles()
    attrib >> profiles


anomaly_attribution_dag = anomaly_attribution()
