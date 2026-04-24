from __future__ import annotations

from collections import defaultdict
from pathlib import Path
import sys
from textwrap import dedent
from typing import Iterable

import pandas as pd
import streamlit as st


APP_DIR = Path(__file__).resolve().parents[1]
if str(APP_DIR) not in sys.path:
    sys.path.insert(0, str(APP_DIR))

from app import get_session


NUMERIC_TYPES = {
    "NUMBER",
    "DECIMAL",
    "NUMERIC",
    "INT",
    "INTEGER",
    "BIGINT",
    "SMALLINT",
    "FLOAT",
    "DOUBLE",
    "DOUBLE PRECISION",
    "REAL",
}
TEMPORAL_TYPES = {"DATE", "TIMESTAMP_LTZ", "TIMESTAMP_NTZ", "TIMESTAMP_TZ", "TIME"}
ANALYTICS_ROW_TIMESTAMP_EXPRESSIONS: dict[str, str] = {
    "DIM_MARKETS": (
        "GREATEST("
        "COALESCE(MARKET_UPDATED_AT, TO_TIMESTAMP_NTZ('1900-01-01 00:00:00')),"
        "COALESCE(MARKET_TRANSFORMED_AT, TO_TIMESTAMP_NTZ('1900-01-01 00:00:00')),"
        "COALESCE(LAST_REFRESHED_AT, TO_TIMESTAMP_NTZ('1900-01-01 00:00:00'))"
        ")"
    ),
    "BRIDGE_MARKET_ASSETS": "LAST_REFRESHED_AT",
    "FACT_DATA_API_TRADES": "COALESCE(SOURCE_LOADED_AT, TRADE_TS, LAST_REFRESHED_AT)",
    "FACT_USER_ACTIVITY_TRADES": "COALESCE(SOURCE_LOADED_AT, TRADE_TS, LAST_REFRESHED_AT)",
    "FACT_CLOB_TRADES": "COALESCE(ENRICHED_AT, STREAMED_AT, TRADE_TS, LAST_REFRESHED_AT)",
    "FACT_BOOK_SNAPSHOTS": "COALESCE(STREAMED_AT, SNAPSHOT_TS, LAST_REFRESHED_AT)",
    "FACT_LEADERBOARD_USER_SNAPSHOTS": (
        "COALESCE(LOADED_AT, TO_TIMESTAMP_NTZ(SNAPSHOT_DATE), LAST_REFRESHED_AT)"
    ),
    "DIM_LEADERBOARD_USERS": (
        "COALESCE(LOADED_AT, TO_TIMESTAMP_NTZ(SNAPSHOT_DATE), LAST_REFRESHED_AT)"
    ),
    "FACT_ANOMALIES": "COALESCE(DETECTED_AT, LAST_REFRESHED_AT)",
    "FACT_ANOMALY_ATTRIBUTION": "COALESCE(ATTRIBUTED_AT, TRADE_TIMESTAMP, LAST_REFRESHED_AT)",
    "FACT_MARKET_DAILY": "COALESCE(LAST_TRADE_TS, FIRST_TRADE_TS, LAST_REFRESHED_AT)",
    "FACT_WALLET_DAILY": "LAST_REFRESHED_AT",
    "DIM_TRADERS": "COALESCE(UPDATED_AT, LAST_SEEN, FIRST_SEEN)",
    "TRACKED_MARKET_VOLUME_DAILY": "COALESCE(LAST_TRADE_TS, FIRST_TRADE_TS, LAST_REFRESHED_AT)",
    "HIGHEST_VOLUME_MARKETS": "LAST_REFRESHED_AT",
    "MARKET_TOP_TRADERS_DAILY": "LAST_REFRESHED_AT",
    "MARKET_TOP_TRADERS_ALL_TIME": "LAST_REFRESHED_AT",
    "MARKET_CONCENTRATION_DAILY": "LAST_REFRESHED_AT",
    "PLATFORM_DAILY_SUMMARY": "LAST_REFRESHED_AT",
    "TRADER_SEGMENT_SNAPSHOT": "LAST_REFRESHED_AT",
    "TRADER_COHORT_MONTHLY": "LAST_REFRESHED_AT",
    "MARKET_THEME_DAILY": "LAST_REFRESHED_AT",
}

ANALYTICS_TABLE_METADATA: dict[str, dict[str, object]] = {
    "DIM_MARKETS": {
        "build_method": "Snowpark DataFrame API",
        "sources": [
            {
                "table_name": "PANTHER_DB.CURATED.GAMMA_MARKETS",
                "role": "Canonical curated market source",
            }
        ],
        "joins": [],
        "build_logic": (
            "Deduplicate curated market rows to the latest snapshot per CONDITION_ID, "
            "then standardize market status, label, liquidity, and volume fields."
        ),
        "query_sql": dedent(
            """
            SELECT *
            FROM (
                SELECT
                    *,
                    ROW_NUMBER() OVER (
                        PARTITION BY CONDITION_ID
                        ORDER BY UPDATED_AT DESC NULLS LAST,
                                 TRANSFORMED_AT DESC NULLS LAST,
                                 MARKET_ID
                    ) AS row_num
                FROM PANTHER_DB.CURATED.GAMMA_MARKETS
                WHERE CONDITION_ID IS NOT NULL
            )
            WHERE row_num = 1
            """
        ).strip(),
    },
    "BRIDGE_MARKET_ASSETS": {
        "build_method": "Snowflake SQL via Snowpark session.sql",
        "sources": [
            {
                "table_name": "PANTHER_DB.ANALYTICS.DIM_MARKETS",
                "role": "Parent market dimension with token and outcome arrays",
            }
        ],
        "joins": [
            {
                "left": "LATERAL FLATTEN(clob_token_ids)",
                "right": "LATERAL FLATTEN(outcomes)",
                "on": "token_flat.index = outcome_flat.index",
            }
        ],
        "build_logic": (
            "Explode market outcome arrays so each market/outcome token becomes one row."
        ),
        "query_sql": dedent(
            """
            SELECT
                market_id,
                condition_id,
                token_flat.index::INTEGER AS outcome_index,
                token_flat.value::STRING AS asset_id,
                outcome_flat.value::STRING AS outcome_name
            FROM PANTHER_DB.ANALYTICS.DIM_MARKETS,
                 LATERAL FLATTEN(input => clob_token_ids) token_flat,
                 LATERAL FLATTEN(input => outcomes) outcome_flat
            WHERE token_flat.index = outcome_flat.index
            """
        ).strip(),
    },
    "FACT_DATA_API_TRADES": {
        "build_method": "Snowpark DataFrame API",
        "sources": [
            {
                "table_name": "DOG_DB.DOG_SCHEMA.DATA_API_TRADES",
                "role": "Supplemental raw trade events from the Polymarket data API",
            },
            {
                "table_name": "PANTHER_DB.ANALYTICS.DIM_MARKETS",
                "role": "Market enrichment for question/label/status",
            },
        ],
        "joins": [
            {
                "left": "DOG_DB.DOG_SCHEMA.DATA_API_TRADES",
                "right": "PANTHER_DB.ANALYTICS.DIM_MARKETS",
                "on": "DATA_API_TRADES.CONDITION_ID = DIM_MARKETS.CONDITION_ID",
            }
        ],
        "build_logic": (
            "Left-join raw DOG trade rows onto the latest market dimension, then cast "
            "types and compute USDC_VOLUME = PRICE * SIZE."
        ),
        "query_sql": dedent(
            """
            SELECT
                TO_DATE(t.TRADE_TIMESTAMP) AS TRADE_DATE,
                TO_TIMESTAMP_NTZ(t.TRADE_TIMESTAMP) AS TRADE_TS,
                d.MARKET_ID,
                t.CONDITION_ID,
                COALESCE(d.MARKET_QUESTION, t.TITLE) AS MARKET_QUESTION,
                COALESCE(d.MARKET_LABEL, t.SLUG, t.TITLE) AS MARKET_LABEL,
                COALESCE(d.ACTIVE, FALSE) AS ACTIVE,
                COALESCE(d.CLOSED, FALSE) AS CLOSED,
                t.PROXY_WALLET,
                t.ASSET AS ASSET_ID,
                t.SIDE AS TRADE_SIDE,
                t.OUTCOME AS OUTCOME_NAME,
                t.OUTCOME_INDEX::INTEGER AS OUTCOME_INDEX,
                t.PRICE::DOUBLE AS PRICE,
                t.SIZE::DOUBLE AS SIZE,
                ROUND(t.PRICE::DOUBLE * t.SIZE::DOUBLE, 2) AS USDC_VOLUME
            FROM DOG_DB.DOG_SCHEMA.DATA_API_TRADES t
            LEFT JOIN PANTHER_DB.ANALYTICS.DIM_MARKETS d
              ON t.CONDITION_ID = d.CONDITION_ID
            """
        ).strip(),
    },
    "FACT_USER_ACTIVITY_TRADES": {
        "build_method": "Snowpark DataFrame API",
        "sources": [
            {
                "table_name": "COYOTE_DB.PUBLIC.CURATED_POLYMARKET_USER_ACTIVITY",
                "role": "Authoritative user transaction source",
            },
            {
                "table_name": "PANTHER_DB.ANALYTICS.DIM_MARKETS",
                "role": "Market enrichment for question/label/status",
            },
        ],
        "joins": [
            {
                "left": "COYOTE_DB.PUBLIC.CURATED_POLYMARKET_USER_ACTIVITY",
                "right": "PANTHER_DB.ANALYTICS.DIM_MARKETS",
                "on": "CURATED_POLYMARKET_USER_ACTIVITY.CONDITIONID = DIM_MARKETS.CONDITION_ID",
            }
        ],
        "build_logic": (
            "Filter COYOTE activity to TRADE rows, left-join onto the latest market dimension, "
            "standardize field names, and use USDCSIZE as the canonical traded-USDC measure."
        ),
        "query_sql": dedent(
            """
            SELECT
                TO_DATE(TO_TIMESTAMP_NTZ(t.TIMESTAMP)) AS TRADE_DATE,
                TO_TIMESTAMP_NTZ(t.TIMESTAMP) AS TRADE_TS,
                d.MARKET_ID,
                t.CONDITIONID AS CONDITION_ID,
                COALESCE(d.MARKET_QUESTION, d.QUESTION_TEXT, t.CONDITIONID) AS MARKET_QUESTION,
                COALESCE(d.MARKET_LABEL, d.MARKET_SLUG, t.CONDITIONID) AS MARKET_LABEL,
                COALESCE(d.ACTIVE, FALSE) AS ACTIVE,
                COALESCE(d.CLOSED, FALSE) AS CLOSED,
                t.PROXYWALLET AS PROXY_WALLET,
                t.ASSET AS ASSET_ID,
                UPPER(t.SIDE) AS TRADE_SIDE,
                t.OUTCOME AS OUTCOME_NAME,
                t.OUTCOMEINDEX::INTEGER AS OUTCOME_INDEX,
                t.PRICE::DOUBLE AS PRICE,
                t.SIZE::DOUBLE AS SIZE,
                COALESCE(t.USDCSIZE::DOUBLE, ROUND(t.PRICE::DOUBLE * t.SIZE::DOUBLE, 2)) AS USDC_VOLUME,
                t.TRANSACTIONHASH AS TRANSACTION_HASH
            FROM COYOTE_DB.PUBLIC.CURATED_POLYMARKET_USER_ACTIVITY t
            LEFT JOIN PANTHER_DB.ANALYTICS.DIM_MARKETS d
              ON t.CONDITIONID = d.CONDITION_ID
            WHERE UPPER(t.TYPE) = 'TRADE'
            """
        ).strip(),
    },
    "FACT_CLOB_TRADES": {
        "build_method": "Snowpark DataFrame API",
        "sources": [
            {
                "table_name": "DOG_DB.DOG_SCHEMA.CLOB_TRADES_STREAM",
                "role": "Raw streamed trade events",
            },
            {
                "table_name": "PANTHER_DB.ANALYTICS.DIM_MARKETS",
                "role": "Market enrichment",
            },
            {
                "table_name": "PANTHER_DB.ANALYTICS.BRIDGE_MARKET_ASSETS",
                "role": "Maps ASSET_ID to outcome metadata",
            },
        ],
        "joins": [
            {
                "left": "DOG_DB.DOG_SCHEMA.CLOB_TRADES_STREAM",
                "right": "PANTHER_DB.ANALYTICS.DIM_MARKETS",
                "on": "MARKET_CONDITION_ID = CONDITION_ID",
            },
            {
                "left": "CLOB trades after market join",
                "right": "PANTHER_DB.ANALYTICS.BRIDGE_MARKET_ASSETS",
                "on": "MARKET_CONDITION_ID = CONDITION_ID AND ASSET_ID = ASSET_ID",
            },
        ],
        "build_logic": (
            "Enrich streamed CLOB trades with market labels and outcome-level asset mappings."
        ),
        "query_sql": dedent(
            """
            SELECT ...
            FROM DOG_DB.DOG_SCHEMA.CLOB_TRADES_STREAM t
            LEFT JOIN PANTHER_DB.ANALYTICS.DIM_MARKETS d
              ON t.MARKET_CONDITION_ID = d.CONDITION_ID
            LEFT JOIN PANTHER_DB.ANALYTICS.BRIDGE_MARKET_ASSETS b
              ON t.MARKET_CONDITION_ID = b.CONDITION_ID
             AND t.ASSET_ID = b.ASSET_ID
            """
        ).strip(),
    },
    "FACT_BOOK_SNAPSHOTS": {
        "build_method": "Snowpark DataFrame API",
        "sources": [
            {
                "table_name": "DOG_DB.DOG_SCHEMA.CLOB_BOOK_SNAPSHOTS",
                "role": "Raw order book snapshots",
            },
            {
                "table_name": "PANTHER_DB.ANALYTICS.DIM_MARKETS",
                "role": "Market enrichment",
            },
            {
                "table_name": "PANTHER_DB.ANALYTICS.BRIDGE_MARKET_ASSETS",
                "role": "Outcome-level asset mapping",
            },
        ],
        "joins": [
            {
                "left": "DOG_DB.DOG_SCHEMA.CLOB_BOOK_SNAPSHOTS",
                "right": "PANTHER_DB.ANALYTICS.DIM_MARKETS",
                "on": "MARKET_CONDITION_ID = CONDITION_ID",
            },
            {
                "left": "Book snapshots after market join",
                "right": "PANTHER_DB.ANALYTICS.BRIDGE_MARKET_ASSETS",
                "on": "MARKET_CONDITION_ID = CONDITION_ID AND ASSET_ID = ASSET_ID",
            },
        ],
        "build_logic": (
            "Attach readable market and outcome fields to raw order book snapshots."
        ),
        "query_sql": dedent(
            """
            SELECT ...
            FROM DOG_DB.DOG_SCHEMA.CLOB_BOOK_SNAPSHOTS s
            LEFT JOIN PANTHER_DB.ANALYTICS.DIM_MARKETS d
              ON s.MARKET_CONDITION_ID = d.CONDITION_ID
            LEFT JOIN PANTHER_DB.ANALYTICS.BRIDGE_MARKET_ASSETS b
              ON s.MARKET_CONDITION_ID = b.CONDITION_ID
             AND s.ASSET_ID = b.ASSET_ID
            """
        ).strip(),
    },
    "FACT_LEADERBOARD_USER_SNAPSHOTS": {
        "build_method": "Snowpark DataFrame API",
        "sources": [
            {
                "table_name": "DOG_DB.DOG_SCHEMA.LEADERBOARD_USERS",
                "role": "Raw leaderboard snapshot rows",
            }
        ],
        "joins": [],
        "build_logic": (
            "Cast and standardize leaderboard snapshot fields at the raw snapshot grain."
        ),
        "query_sql": "SELECT CAST/RENAME fields FROM DOG_DB.DOG_SCHEMA.LEADERBOARD_USERS",
    },
    "DIM_LEADERBOARD_USERS": {
        "build_method": "Snowpark DataFrame API",
        "sources": [
            {
                "table_name": "PANTHER_DB.ANALYTICS.FACT_LEADERBOARD_USER_SNAPSHOTS",
                "role": "Historical leaderboard snapshots",
            }
        ],
        "joins": [],
        "build_logic": (
            "Keep the latest leaderboard snapshot per PROXY_WALLET using SNAPSHOT_DATE "
            "and LOADED_AT ordering."
        ),
        "query_sql": dedent(
            """
            SELECT *
            FROM (
                SELECT
                    *,
                    ROW_NUMBER() OVER (
                        PARTITION BY PROXY_WALLET
                        ORDER BY SNAPSHOT_DATE DESC NULLS LAST, LOADED_AT DESC NULLS LAST
                    ) AS row_num
                FROM PANTHER_DB.ANALYTICS.FACT_LEADERBOARD_USER_SNAPSHOTS
            )
            WHERE row_num = 1
            """
        ).strip(),
    },
    "FACT_ANOMALIES": {
        "build_method": "Snowpark DataFrame API",
        "sources": [
            {
                "table_name": "DOG_DB.DOG_SCHEMA.DETECTED_ANOMALIES",
                "role": "Raw anomaly detections",
            },
            {
                "table_name": "PANTHER_DB.ANALYTICS.DIM_MARKETS",
                "role": "Market enrichment",
            },
            {
                "table_name": "PANTHER_DB.ANALYTICS.BRIDGE_MARKET_ASSETS",
                "role": "Outcome-level asset mapping",
            },
        ],
        "joins": [
            {
                "left": "DOG_DB.DOG_SCHEMA.DETECTED_ANOMALIES",
                "right": "PANTHER_DB.ANALYTICS.DIM_MARKETS",
                "on": "MARKET_CONDITION_ID = CONDITION_ID",
            },
            {
                "left": "Detected anomalies after market join",
                "right": "PANTHER_DB.ANALYTICS.BRIDGE_MARKET_ASSETS",
                "on": "MARKET_CONDITION_ID = CONDITION_ID AND ASSET_ID = ASSET_ID",
            },
        ],
        "build_logic": (
            "Attach market and outcome metadata to anomaly detections."
        ),
        "query_sql": dedent(
            """
            SELECT ...
            FROM DOG_DB.DOG_SCHEMA.DETECTED_ANOMALIES a
            LEFT JOIN PANTHER_DB.ANALYTICS.DIM_MARKETS d
              ON a.MARKET_CONDITION_ID = d.CONDITION_ID
            LEFT JOIN PANTHER_DB.ANALYTICS.BRIDGE_MARKET_ASSETS b
              ON a.MARKET_CONDITION_ID = b.CONDITION_ID
             AND a.ASSET_ID = b.ASSET_ID
            """
        ).strip(),
    },
    "FACT_ANOMALY_ATTRIBUTION": {
        "build_method": "Snowpark DataFrame API",
        "sources": [
            {
                "table_name": "DOG_DB.DOG_TRANSFORM.ANOMALY_ATTRIBUTION",
                "role": "Teammate-provided anomaly attribution rows",
            },
            {
                "table_name": "PANTHER_DB.ANALYTICS.DIM_MARKETS",
                "role": "Market enrichment",
            },
        ],
        "joins": [
            {
                "left": "DOG_DB.DOG_TRANSFORM.ANOMALY_ATTRIBUTION",
                "right": "PANTHER_DB.ANALYTICS.DIM_MARKETS",
                "on": "MARKET_CONDITION_ID = CONDITION_ID",
            }
        ],
        "build_logic": (
            "Attach readable market metadata to anomaly-attribution records."
        ),
        "query_sql": dedent(
            """
            SELECT ...
            FROM DOG_DB.DOG_TRANSFORM.ANOMALY_ATTRIBUTION a
            LEFT JOIN PANTHER_DB.ANALYTICS.DIM_MARKETS d
              ON a.MARKET_CONDITION_ID = d.CONDITION_ID
            """
        ).strip(),
    },
    "FACT_MARKET_DAILY": {
        "build_method": "Snowpark DataFrame API with grouped aggregations and joins",
        "sources": [
            {
                "table_name": "PANTHER_DB.ANALYTICS.FACT_USER_ACTIVITY_TRADES",
                "role": "Primary COYOTE-backed trade fact input",
            },
            {
                "table_name": "PANTHER_DB.ANALYTICS.FACT_ANOMALIES",
                "role": "Daily anomaly counts",
            },
        ],
        "joins": [
            {
                "left": "Grouped trade stats",
                "right": "Open-price subquery",
                "on": "CONDITION_ID and TRADE_DATE",
            },
            {
                "left": "Grouped trade stats",
                "right": "Close-price subquery",
                "on": "CONDITION_ID and TRADE_DATE",
            },
            {
                "left": "Grouped trade stats",
                "right": "Daily anomaly counts",
                "on": "CONDITION_ID and TRADE_DATE",
            },
        ],
        "build_logic": (
            "Aggregate FACT_USER_ACTIVITY_TRADES to the day/market grain, then add open, close, "
            "high, low, VWAP, and anomaly counts."
        ),
        "query_sql": dedent(
            """
            WITH grouped_stats AS (
                SELECT
                    TRADE_DATE,
                    CONDITION_ID,
                    MARKET_ID,
                    MARKET_LABEL,
                    COUNT(*) AS TRADE_COUNT,
                    SUM(USDC_VOLUME) AS TOTAL_USDC_VOLUME,
                    COUNT(DISTINCT PROXY_WALLET) AS UNIQUE_WALLETS,
                    MIN(TRADE_TS) AS FIRST_TRADE_TS,
                    MAX(TRADE_TS) AS LAST_TRADE_TS
                FROM PANTHER_DB.ANALYTICS.FACT_USER_ACTIVITY_TRADES
                GROUP BY TRADE_DATE, CONDITION_ID, MARKET_ID, MARKET_LABEL
            )
            SELECT ...
            FROM grouped_stats g
            LEFT JOIN open_prices o
              ON g.CONDITION_ID = o.CONDITION_ID AND g.TRADE_DATE = o.TRADE_DATE
            LEFT JOIN close_prices c
              ON g.CONDITION_ID = c.CONDITION_ID AND g.TRADE_DATE = c.TRADE_DATE
            LEFT JOIN anomaly_counts a
              ON g.CONDITION_ID = a.CONDITION_ID AND g.TRADE_DATE = a.TRADE_DATE
            """
        ).strip(),
    },
    "FACT_WALLET_DAILY": {
        "build_method": "Snowpark DataFrame API with grouped aggregations and joins",
        "sources": [
            {
                "table_name": "PANTHER_DB.ANALYTICS.FACT_USER_ACTIVITY_TRADES",
                "role": "Primary COYOTE-backed trade fact input",
            },
            {
                "table_name": "PANTHER_DB.ANALYTICS.FACT_ANOMALY_ATTRIBUTION",
                "role": "Per-wallet anomaly attribution input",
            },
        ],
        "joins": [
            {
                "left": "Wallet/day grouped trade stats",
                "right": "Wallet/day anomaly counts",
                "on": "PROXY_WALLET and TRADE_DATE",
            }
        ],
        "build_logic": (
            "Aggregate trading behavior to wallet/day grain and left-join daily anomaly involvement."
        ),
        "query_sql": dedent(
            """
            WITH grouped AS (
                SELECT
                    PROXY_WALLET,
                    TRADE_DATE,
                    COUNT(*) AS TRADE_COUNT,
                    SUM(USDC_VOLUME) AS TOTAL_USDC_VOLUME
                FROM PANTHER_DB.ANALYTICS.FACT_USER_ACTIVITY_TRADES
                GROUP BY PROXY_WALLET, TRADE_DATE
            )
            SELECT ...
            FROM grouped g
            LEFT JOIN anomaly_counts a
              ON g.PROXY_WALLET = a.PROXY_WALLET
             AND g.TRADE_DATE = a.TRADE_DATE
            """
        ).strip(),
    },
    "DIM_TRADERS": {
        "build_method": "Snowpark DataFrame API with grouped aggregations and joins",
        "sources": [
            {
                "table_name": "PANTHER_DB.ANALYTICS.FACT_USER_ACTIVITY_TRADES",
                "role": "Wallet-level COYOTE trade rollup source",
            },
            {
                "table_name": "PANTHER_DB.ANALYTICS.DIM_LEADERBOARD_USERS",
                "role": "Leaderboard enrichment",
            },
            {
                "table_name": "PANTHER_DB.ANALYTICS.FACT_ANOMALY_ATTRIBUTION",
                "role": "Anomaly involvement counts",
            },
            {
                "table_name": "DOG_DB.DOG_TRANSFORM.TRADER_RISK_PROFILES",
                "role": "Teammate-provided risk profile enrichment",
            },
        ],
        "joins": [
            {
                "left": "Trade rollup by PROXY_WALLET",
                "right": "DIM_LEADERBOARD_USERS",
                "on": "PROXY_WALLET = PROXY_WALLET",
            },
            {
                "left": "Trade rollup by PROXY_WALLET",
                "right": "Anomaly counts by PROXY_WALLET",
                "on": "PROXY_WALLET = ANOMALY_PROXY_WALLET",
            },
            {
                "left": "Trade rollup by PROXY_WALLET",
                "right": "DOG_DB.DOG_TRANSFORM.TRADER_RISK_PROFILES",
                "on": "PROXY_WALLET = PROXY_WALLET",
            },
        ],
        "build_logic": (
            "Create one row per trader by combining lifetime trade rollups, leaderboard profile data, "
            "anomaly involvement, and optional risk-profile enrichment."
        ),
        "query_sql": dedent(
            """
            WITH trade_rollup AS (
                SELECT
                    PROXY_WALLET,
                    COUNT(*) AS TOTAL_TRADES,
                    SUM(USDC_VOLUME) AS TOTAL_VOLUME
                FROM PANTHER_DB.ANALYTICS.FACT_USER_ACTIVITY_TRADES
                GROUP BY PROXY_WALLET
            )
            SELECT ...
            FROM trade_rollup t
            LEFT JOIN PANTHER_DB.ANALYTICS.DIM_LEADERBOARD_USERS l
              ON t.PROXY_WALLET = l.PROXY_WALLET
            LEFT JOIN anomaly_by_wallet a
              ON t.PROXY_WALLET = a.ANOMALY_PROXY_WALLET
            LEFT JOIN DOG_DB.DOG_TRANSFORM.TRADER_RISK_PROFILES r
              ON t.PROXY_WALLET = r.PROXY_WALLET
            """
        ).strip(),
    },
    "TRACKED_MARKET_VOLUME_DAILY": {
        "build_method": "Snowpark DataFrame API",
        "sources": [
            {
                "table_name": "PANTHER_DB.ANALYTICS.FACT_MARKET_DAILY",
                "role": "App-facing daily market aggregate source",
            }
        ],
        "joins": [],
        "build_logic": (
            "Project a UI-focused subset of FACT_MARKET_DAILY with renamed columns."
        ),
        "query_sql": dedent(
            """
            SELECT
                STAT_DATE AS TRADE_DATE,
                MARKET_ID,
                CONDITION_ID,
                MARKET_QUESTION,
                MARKET_LABEL,
                ACTIVE,
                CLOSED,
                TRADE_COUNT,
                UNIQUE_WALLETS AS UNIQUE_TRADERS,
                TOTAL_USDC_VOLUME AS TOTAL_VOLUME_USDC,
                FIRST_TRADE_TS,
                LAST_TRADE_TS,
                LAST_REFRESHED_AT
            FROM PANTHER_DB.ANALYTICS.FACT_MARKET_DAILY
            """
        ).strip(),
    },
    "HIGHEST_VOLUME_MARKETS": {
        "build_method": "Snowpark DataFrame API",
        "sources": [
            {
                "table_name": "PANTHER_DB.ANALYTICS.DIM_MARKETS",
                "role": "Latest market snapshot with listed volume",
            }
        ],
        "joins": [],
        "build_logic": (
            "Expose listed volume from DIM_MARKETS as an app-facing ranking table."
        ),
        "query_sql": dedent(
            """
            SELECT
                MARKET_ID,
                CONDITION_ID,
                MARKET_QUESTION,
                MARKET_LABEL,
                ACTIVE,
                CLOSED,
                END_DATE,
                ROUND(LISTED_VOLUME_USDC, 2) AS TOTAL_VOLUME_USDC,
                LAST_REFRESHED_AT
            FROM PANTHER_DB.ANALYTICS.DIM_MARKETS
            """
        ).strip(),
    },
    "MARKET_TOP_TRADERS_DAILY": {
        "build_method": "Snowpark DataFrame API with grouped aggregations",
        "sources": [
            {
                "table_name": "PANTHER_DB.ANALYTICS.FACT_USER_ACTIVITY_TRADES",
                "role": "COYOTE-backed trade fact source for per-day trader rankings",
            }
        ],
        "joins": [],
        "build_logic": (
            "Aggregate FACT_USER_ACTIVITY_TRADES by TRADE_DATE, market, and PROXY_WALLET."
        ),
        "query_sql": dedent(
            """
            SELECT
                TRADE_DATE,
                MARKET_ID,
                CONDITION_ID,
                MARKET_QUESTION,
                MARKET_LABEL,
                PROXY_WALLET,
                ROUND(SUM(USDC_VOLUME), 2) AS TOTAL_VOLUME_USDC,
                COUNT(*) AS TRADE_COUNT,
                ROUND(AVG(USDC_VOLUME), 2) AS AVERAGE_TRADE_USDC,
                ROUND(MAX(USDC_VOLUME), 2) AS LARGEST_TRADE_USDC
            FROM PANTHER_DB.ANALYTICS.FACT_USER_ACTIVITY_TRADES
            GROUP BY TRADE_DATE, MARKET_ID, CONDITION_ID, MARKET_QUESTION, MARKET_LABEL, PROXY_WALLET
            """
        ).strip(),
    },
    "MARKET_TOP_TRADERS_ALL_TIME": {
        "build_method": "Snowpark DataFrame API with grouped aggregations",
        "sources": [
            {
                "table_name": "PANTHER_DB.ANALYTICS.FACT_USER_ACTIVITY_TRADES",
                "role": "COYOTE-backed trade fact source for lifetime trader rankings",
            }
        ],
        "joins": [],
        "build_logic": (
            "Aggregate FACT_USER_ACTIVITY_TRADES by market and PROXY_WALLET across all available history."
        ),
        "query_sql": dedent(
            """
            SELECT
                MARKET_ID,
                CONDITION_ID,
                MARKET_QUESTION,
                MARKET_LABEL,
                PROXY_WALLET,
                ROUND(SUM(USDC_VOLUME), 2) AS TOTAL_VOLUME_USDC,
                COUNT(*) AS TRADE_COUNT,
                ROUND(AVG(USDC_VOLUME), 2) AS AVERAGE_TRADE_USDC,
                ROUND(MAX(USDC_VOLUME), 2) AS LARGEST_TRADE_USDC
            FROM PANTHER_DB.ANALYTICS.FACT_USER_ACTIVITY_TRADES
            GROUP BY MARKET_ID, CONDITION_ID, MARKET_QUESTION, MARKET_LABEL, PROXY_WALLET
            """
        ).strip(),
    },
    "MARKET_CONCENTRATION_DAILY": {
        "build_method": "Snowflake SQL via Snowpark session.sql",
        "sources": [
            {
                "table_name": "PANTHER_DB.ANALYTICS.FACT_MARKET_DAILY",
                "role": "Daily market totals and unique-trader counts",
            },
            {
                "table_name": "PANTHER_DB.ANALYTICS.MARKET_TOP_TRADERS_DAILY",
                "role": "Per-market trader rankings used to compute concentration shares",
            },
        ],
        "joins": [
            {
                "left": "FACT_MARKET_DAILY",
                "right": "Top-trader concentration rollup",
                "on": "STAT_DATE = TRADE_DATE AND CONDITION_ID = CONDITION_ID",
            }
        ],
        "build_logic": (
            "Materialize top-1 and top-5 trader share-of-volume metrics for each "
            "market/day so the app can present a real concentration view instead of a proxy."
        ),
        "query_sql": dedent(
            """
            WITH ranked AS (
                SELECT
                    TRADE_DATE,
                    CONDITION_ID,
                    PROXY_WALLET,
                    TOTAL_VOLUME_USDC,
                    ROW_NUMBER() OVER (
                        PARTITION BY TRADE_DATE, CONDITION_ID
                        ORDER BY TOTAL_VOLUME_USDC DESC, PROXY_WALLET
                    ) AS trader_rank
                FROM PANTHER_DB.ANALYTICS.MARKET_TOP_TRADERS_DAILY
            )
            SELECT ...
            FROM PANTHER_DB.ANALYTICS.FACT_MARKET_DAILY m
            LEFT JOIN concentration c
              ON m.STAT_DATE = c.TRADE_DATE
             AND m.CONDITION_ID = c.CONDITION_ID
            """
        ).strip(),
    },
    "PLATFORM_DAILY_SUMMARY": {
        "build_method": "Snowflake SQL via Snowpark session.sql",
        "sources": [
            {
                "table_name": "PANTHER_DB.ANALYTICS.FACT_MARKET_DAILY",
                "role": "Day-level market totals",
            },
            {
                "table_name": "PANTHER_DB.ANALYTICS.FACT_WALLET_DAILY",
                "role": "Day-level wallet activity counts",
            },
        ],
        "joins": [
            {
                "left": "Market-day rollup",
                "right": "Wallet-day rollup",
                "on": "TRADE_DATE = TRADE_DATE",
            }
        ],
        "build_logic": (
            "Precompute day-level platform summary metrics so the app reads a compact "
            "mart instead of aggregating lower-level daily facts live."
        ),
        "query_sql": dedent(
            """
            WITH market_rollup AS (
                SELECT
                    STAT_DATE AS TRADE_DATE,
                    SUM(TOTAL_USDC_VOLUME) AS TOTAL_VOLUME_USDC,
                    SUM(TRADE_COUNT) AS TOTAL_TRADES
                FROM PANTHER_DB.ANALYTICS.FACT_MARKET_DAILY
                GROUP BY STAT_DATE
            )
            SELECT ...
            FROM market_rollup m
            LEFT JOIN wallet_rollup w
              ON m.TRADE_DATE = w.TRADE_DATE
            """
        ).strip(),
    },
    "TRADER_SEGMENT_SNAPSHOT": {
        "build_method": "Snowflake SQL via Snowpark session.sql",
        "sources": [
            {
                "table_name": "PANTHER_DB.ANALYTICS.DIM_TRADERS",
                "role": "All-time trader profiles and behavior metrics",
            }
        ],
        "joins": [],
        "build_logic": (
            "Assign percentile buckets across trader volume, activity, diversification, "
            "and average trade size to produce a reusable segmentation snapshot."
        ),
        "query_sql": dedent(
            """
            SELECT
                *,
                NTILE(10) OVER (ORDER BY TOTAL_VOLUME DESC NULLS LAST) AS volume_decile,
                NTILE(10) OVER (ORDER BY TOTAL_TRADES DESC NULLS LAST) AS trade_count_decile,
                NTILE(10) OVER (ORDER BY DISTINCT_MARKETS DESC NULLS LAST) AS diversification_decile,
                NTILE(10) OVER (ORDER BY AVG_TRADE_SIZE DESC NULLS LAST) AS avg_trade_size_decile
            FROM PANTHER_DB.ANALYTICS.DIM_TRADERS
            """
        ).strip(),
    },
    "TRADER_COHORT_MONTHLY": {
        "build_method": "Snowflake SQL via Snowpark session.sql",
        "sources": [
            {
                "table_name": "PANTHER_DB.ANALYTICS.FACT_WALLET_DAILY",
                "role": "Daily wallet activity used to build wallet-month summaries",
            },
            {
                "table_name": "PANTHER_DB.ANALYTICS.DIM_TRADERS",
                "role": "Trader first-seen timestamps used for cohort assignment",
            },
        ],
        "joins": [
            {
                "left": "Wallet-month rollup",
                "right": "DIM_TRADERS cohort assignment",
                "on": "PROXY_WALLET = PROXY_WALLET",
            }
        ],
        "build_logic": (
            "Compress wallet activity into cohort-month metrics so the app can present "
            "wallet lifecycle and cohort behavior without scanning raw trade history."
        ),
        "query_sql": dedent(
            """
            WITH cohort_base AS (
                SELECT
                    PROXY_WALLET,
                    DATE_TRUNC('MONTH', FIRST_SEEN) AS cohort_month
                FROM PANTHER_DB.ANALYTICS.DIM_TRADERS
            )
            SELECT ...
            FROM wallet_monthly w
            INNER JOIN cohort_base c
              ON w.PROXY_WALLET = c.PROXY_WALLET
            """
        ).strip(),
    },
    "MARKET_THEME_DAILY": {
        "build_method": "Snowflake SQL via Snowpark session.sql",
        "sources": [
            {
                "table_name": "PANTHER_DB.ANALYTICS.FACT_USER_ACTIVITY_TRADES",
                "role": "Authoritative COYOTE-backed trade history used to aggregate theme-level demand",
            },
            {
                "table_name": "PANTHER_DB.ANALYTICS.DIM_MARKETS",
                "role": "PANTHER market metadata used to normalize each market into a theme",
            },
        ],
        "joins": [
            {
                "left": "FACT_USER_ACTIVITY_TRADES",
                "right": "DIM_MARKETS",
                "on": "CONDITION_ID = CONDITION_ID",
            }
        ],
        "build_logic": (
            "Normalize each market into a bet theme using PANTHER `GROUP_ITEM_TITLE` first, "
            "then event title as a fallback, and aggregate daily volume, participation, "
            "and market counts at the theme/day grain."
        ),
        "query_sql": dedent(
            """
            SELECT
                f.TRADE_DATE,
                COALESCE(
                    d.MARKET_THEME,
                    d.MARKET_GROUP_TITLE,
                    d.EVENT_TITLE,
                    f.MARKET_LABEL,
                    f.MARKET_QUESTION,
                    f.CONDITION_ID
                ) AS MARKET_THEME,
                COUNT(DISTINCT f.CONDITION_ID) AS DISTINCT_MARKETS,
                COUNT(*) AS TRADE_COUNT,
                COUNT(DISTINCT f.PROXY_WALLET) AS UNIQUE_TRADERS,
                SUM(f.USDC_VOLUME) AS TOTAL_VOLUME_USDC
            FROM PANTHER_DB.ANALYTICS.FACT_USER_ACTIVITY_TRADES f
            LEFT JOIN PANTHER_DB.ANALYTICS.DIM_MARKETS d
              ON f.CONDITION_ID = d.CONDITION_ID
            GROUP BY 1, 2
            """
        ).strip(),
    },
}


def quote_identifier(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def parse_fq_name(full_name: str) -> tuple[str, str, str]:
    parts = [part.strip() for part in full_name.strip().split(".")]
    if len(parts) != 3 or any(not part for part in parts):
        raise ValueError(f"Expected DATABASE.SCHEMA.TABLE, got {full_name!r}")
    return parts[0], parts[1], parts[2]


def format_bytes_to_gb(value: float | int | None) -> float | None:
    if value is None:
        return None
    return round(float(value) / (1024**3), 4)


def format_timestamp(value) -> str:
    if pd.isna(value):
        return "N/A"
    if hasattr(value, "strftime"):
        return value.strftime("%Y-%m-%d %H:%M")
    return str(value)


def render_timestamp_metric(column, label: str, value) -> None:
    formatted = format_timestamp(value)
    column.markdown(
        f"""
        <div style="padding-top: 0.25rem;">
          <div style="font-size: 0.95rem; color: rgba(250, 250, 250, 0.95); margin-bottom: 0.4rem;">
            {label}
          </div>
          <div style="font-size: 1.7rem; font-weight: 600; line-height: 1.2; word-break: break-word;">
            {formatted}
          </div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def build_base_table_specs() -> list[dict[str, str]]:
    app_settings = dict(st.secrets["app"]) if "app" in st.secrets else {}
    return [
        {
            "system": "PANTHER",
            "layer": "raw",
            "table_name": "PANTHER_DB.RAW.GAMMA_MARKET_PAGES_STAGE",
            "description": "Landing-stage table for staged Gamma market payloads.",
        },
        {
            "system": "PANTHER",
            "layer": "raw",
            "table_name": "PANTHER_DB.RAW.GAMMA_MARKET_PAGES",
            "description": "Persisted raw Gamma market pages loaded by the Gamma DAGs.",
        },
        {
            "system": "PANTHER",
            "layer": "curated",
            "table_name": app_settings.get(
                "markets_table", "PANTHER_DB.CURATED.GAMMA_MARKETS"
            ),
            "description": "Curated market dimension populated from Gamma market ingestion.",
        },
        {
            "system": "COYOTE",
            "layer": "curated",
            "table_name": app_settings.get(
                "user_activity_table",
                "COYOTE_DB.PUBLIC.CURATED_POLYMARKET_USER_ACTIVITY",
            ),
            "description": "Authoritative user transaction table loaded from the user-activity parquet ingestion.",
        },
        {
            "system": "DOG",
            "layer": "raw",
            "table_name": app_settings.get(
                "dog_data_api_trades_table", "DOG_DB.DOG_SCHEMA.DATA_API_TRADES"
            ),
            "description": "DOG raw trade feed from the Polymarket data API.",
        },
        {
            "system": "DOG",
            "layer": "raw",
            "table_name": app_settings.get(
                "dog_clob_trades_stream_table", "DOG_DB.DOG_SCHEMA.CLOB_TRADES_STREAM"
            ),
            "description": "DOG raw streamed CLOB trade events.",
        },
        {
            "system": "DOG",
            "layer": "raw",
            "table_name": app_settings.get(
                "dog_clob_book_snapshots_table",
                "DOG_DB.DOG_SCHEMA.CLOB_BOOK_SNAPSHOTS",
            ),
            "description": "DOG raw order-book snapshot stream.",
        },
        {
            "system": "DOG",
            "layer": "raw",
            "table_name": app_settings.get(
                "dog_leaderboard_users_table", "DOG_DB.DOG_SCHEMA.LEADERBOARD_USERS"
            ),
            "description": "DOG raw leaderboard snapshot source for trader enrichment.",
        },
        {
            "system": "DOG",
            "layer": "raw",
            "table_name": app_settings.get(
                "dog_detected_anomalies_table", "DOG_DB.DOG_SCHEMA.DETECTED_ANOMALIES"
            ),
            "description": "DOG-derived anomaly detection output at market or asset level.",
        },
        {
            "system": "DOG",
            "layer": "transform",
            "table_name": app_settings.get(
                "dog_market_daily_stats_table",
                "DOG_DB.DOG_TRANSFORM.MARKET_DAILY_STATS",
            ),
            "description": "Teammate-provided DOG daily market rollups.",
        },
        {
            "system": "DOG",
            "layer": "transform",
            "table_name": app_settings.get(
                "dog_wallet_daily_stats_table",
                "DOG_DB.DOG_TRANSFORM.WALLET_DAILY_STATS",
            ),
            "description": "Teammate-provided DOG daily wallet rollups.",
        },
        {
            "system": "DOG",
            "layer": "transform",
            "table_name": app_settings.get(
                "dog_anomaly_attribution_table",
                "DOG_DB.DOG_TRANSFORM.ANOMALY_ATTRIBUTION",
            ),
            "description": "Teammate-provided anomaly-to-trader attribution table.",
        },
        {
            "system": "DOG",
            "layer": "transform",
            "table_name": app_settings.get(
                "dog_trader_risk_profiles_table",
                "DOG_DB.DOG_TRANSFORM.TRADER_RISK_PROFILES",
            ),
            "description": "Teammate-provided trader risk and behavior profiles.",
        },
    ]


@st.cache_data(ttl=300, show_spinner=False)
def fetch_table_inventory_metadata(table_specs: tuple[tuple[str, str, str, str], ...]) -> pd.DataFrame:
    session = get_session()
    records = [
        {
            "system": spec[0],
            "layer": spec[1],
            "table_name": spec[2],
            "description": spec[3],
        }
        for spec in table_specs
    ]

    grouped_specs: dict[str, list[tuple[str, str]]] = defaultdict(list)
    for record in records:
        database, schema, table = parse_fq_name(record["table_name"])
        grouped_specs[database].append((schema, table))

    metadata_frames: list[pd.DataFrame] = []
    for database, schema_tables in grouped_specs.items():
        clauses = []
        for schema, table in schema_tables:
            clauses.append(
                f"(table_schema = '{schema}' AND table_name = '{table}')"
            )
        query = f"""
            SELECT
                table_catalog AS database_name,
                table_schema,
                table_name,
                row_count,
                bytes,
                last_altered
            FROM {database}.information_schema.tables
            WHERE {' OR '.join(clauses)}
        """
        metadata_frames.append(session.sql(query).to_pandas())

    metadata_df = (
        pd.concat(metadata_frames, ignore_index=True)
        if metadata_frames
        else pd.DataFrame(
            columns=[
                "DATABASE_NAME",
                "TABLE_SCHEMA",
                "TABLE_NAME",
                "ROW_COUNT",
                "BYTES",
                "LAST_ALTERED",
            ]
        )
    )

    if not metadata_df.empty:
        metadata_df["table_name"] = (
            metadata_df["DATABASE_NAME"]
            + "."
            + metadata_df["TABLE_SCHEMA"]
            + "."
            + metadata_df["TABLE_NAME"]
        )
        metadata_df = metadata_df.rename(
            columns={
                "ROW_COUNT": "row_count",
                "BYTES": "bytes",
                "LAST_ALTERED": "last_altered",
            }
        )[["table_name", "row_count", "bytes", "last_altered"]]

    inventory_df = pd.DataFrame(records)
    merged = inventory_df.merge(metadata_df, on="table_name", how="left")
    merged["size_gb"] = merged["bytes"].apply(format_bytes_to_gb)
    return merged


@st.cache_data(ttl=300, show_spinner=False)
def fetch_analytics_inventory_metadata() -> pd.DataFrame:
    session = get_session()
    query = """
        SELECT
            table_catalog AS database_name,
            table_schema,
            table_name,
            row_count,
            bytes,
            last_altered
        FROM PANTHER_DB.information_schema.tables
        WHERE table_schema = 'ANALYTICS'
          AND table_name <> 'ANALYTICS_BUILD_STATE'
        ORDER BY table_name
    """
    df = session.sql(query).to_pandas()
    if df.empty:
        return pd.DataFrame(
            columns=[
                "table_name",
                "row_count",
                "size_gb",
                "last_altered",
                "latest_row_timestamp",
                "short_name",
            ]
        )

    df["table_name"] = (
        df["DATABASE_NAME"] + "." + df["TABLE_SCHEMA"] + "." + df["TABLE_NAME"]
    )
    df["size_gb"] = df["BYTES"].apply(format_bytes_to_gb)
    df = df.rename(
        columns={
            "ROW_COUNT": "row_count",
            "LAST_ALTERED": "last_altered",
        }
    )
    inventory_df = df[
        ["table_name", "row_count", "size_gb", "last_altered", "TABLE_NAME"]
    ].rename(columns={"TABLE_NAME": "short_name"})
    row_freshness_df = fetch_analytics_row_freshness(tuple(inventory_df["table_name"].tolist()))
    return inventory_df.merge(row_freshness_df, on="table_name", how="left")


@st.cache_data(ttl=300, show_spinner=False)
def fetch_analytics_row_freshness(
    table_names: tuple[str, ...],
) -> pd.DataFrame:
    if not table_names:
        return pd.DataFrame(columns=["table_name", "latest_row_timestamp"])

    session = get_session()
    subqueries: list[str] = []
    for full_name in table_names:
        _, _, short_name = parse_fq_name(full_name)
        timestamp_expression = ANALYTICS_ROW_TIMESTAMP_EXPRESSIONS.get(short_name)
        if not timestamp_expression:
            subqueries.append(
                f"SELECT '{full_name}' AS table_name, NULL::TIMESTAMP_NTZ AS latest_row_timestamp"
            )
            continue
        subqueries.append(
            f"""
            SELECT
                '{full_name}' AS table_name,
                MAX({timestamp_expression}) AS latest_row_timestamp
            FROM {full_name}
            """.strip()
        )

    return session.sql("\nUNION ALL\n".join(subqueries)).to_pandas().rename(
        columns={"TABLE_NAME": "table_name", "LATEST_ROW_TIMESTAMP": "latest_row_timestamp"}
    )


@st.cache_data(ttl=300, show_spinner=False)
def fetch_analytics_build_state() -> pd.DataFrame:
    session = get_session()
    query = """
        SELECT
            pipeline_name,
            last_successful_coyote_loaded_at,
            last_build_mode,
            updated_at
        FROM PANTHER_DB.ANALYTICS.ANALYTICS_BUILD_STATE
        WHERE pipeline_name = 'analytics_layer_v2'
    """
    return session.sql(query).to_pandas().rename(
        columns={
            "PIPELINE_NAME": "pipeline_name",
            "LAST_SUCCESSFUL_COYOTE_LOADED_AT": "last_successful_coyote_loaded_at",
            "LAST_BUILD_MODE": "last_build_mode",
            "UPDATED_AT": "updated_at",
        }
    )


@st.cache_data(ttl=300, show_spinner=False)
def fetch_table_columns(full_name: str) -> pd.DataFrame:
    session = get_session()
    database, schema, table = parse_fq_name(full_name)
    query = f"""
        SELECT
            ordinal_position,
            column_name,
            data_type
        FROM {database}.information_schema.columns
        WHERE table_schema = '{schema}'
          AND table_name = '{table}'
        ORDER BY ordinal_position
    """
    return session.sql(query).to_pandas().rename(
        columns={
            "ORDINAL_POSITION": "position",
            "COLUMN_NAME": "column_name",
            "DATA_TYPE": "data_type",
        }
    )


@st.cache_data(ttl=300, show_spinner=False)
def fetch_example_row(full_name: str) -> pd.DataFrame:
    session = get_session()
    return session.table(full_name).limit(1).to_pandas()


@st.cache_data(ttl=300, show_spinner=False)
def fetch_table_summary_stats(full_name: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    columns_df = fetch_table_columns(full_name)
    numeric_columns = columns_df[
        columns_df["data_type"].isin(NUMERIC_TYPES)
    ]["column_name"].tolist()[:6]
    temporal_columns = columns_df[
        columns_df["data_type"].isin(TEMPORAL_TYPES)
    ]["column_name"].tolist()[:4]

    if not numeric_columns and not temporal_columns:
        return pd.DataFrame(), pd.DataFrame()

    session = get_session()
    select_parts = ["COUNT(*) AS row_count"]
    for column in numeric_columns:
        quoted = quote_identifier(column)
        alias = column.lower()
        select_parts.extend(
            [
                f"COUNT({quoted}) AS {alias}__non_null",
                f"MIN({quoted}) AS {alias}__min",
                f"MAX({quoted}) AS {alias}__max",
                f"AVG({quoted}) AS {alias}__avg",
            ]
        )
    for column in temporal_columns:
        quoted = quote_identifier(column)
        alias = column.lower()
        select_parts.extend(
            [
                f"COUNT({quoted}) AS {alias}__non_null",
                f"MIN({quoted}) AS {alias}__min",
                f"MAX({quoted}) AS {alias}__max",
            ]
        )

    summary_query = f"""
        SELECT
            {', '.join(select_parts)}
        FROM {full_name}
    """
    summary_row_raw = session.sql(summary_query).to_pandas().iloc[0].to_dict()
    summary_row = {
        str(key).lower(): value for key, value in summary_row_raw.items()
    }

    numeric_records = []
    for column in numeric_columns:
        alias = column.lower()
        numeric_records.append(
            {
                "column_name": column,
                "non_null_count": summary_row.get(f"{alias}__non_null"),
                "min": summary_row.get(f"{alias}__min"),
                "max": summary_row.get(f"{alias}__max"),
                "avg": summary_row.get(f"{alias}__avg"),
            }
        )

    temporal_records = []
    for column in temporal_columns:
        alias = column.lower()
        temporal_records.append(
            {
                "column_name": column,
                "non_null_count": summary_row.get(f"{alias}__non_null"),
                "min": format_timestamp(summary_row.get(f"{alias}__min")),
                "max": format_timestamp(summary_row.get(f"{alias}__max")),
            }
        )

    return pd.DataFrame(numeric_records), pd.DataFrame(temporal_records)


def fetch_analytics_lineage(table_name: str) -> dict[str, object] | None:
    short_name = table_name.split(".")[-1]
    return ANALYTICS_TABLE_METADATA.get(short_name)


def selected_row_or_default(
    df: pd.DataFrame, selection, session_key: str
) -> pd.Series | None:
    if df.empty:
        return None

    selected_rows = selection.selection.rows
    selected = None
    if selected_rows:
        selected = df.iloc[selected_rows[0]]
        st.session_state[session_key] = selected["table_name"]
    elif session_key in st.session_state:
        matches = df[df["table_name"] == st.session_state[session_key]]
        if not matches.empty:
            selected = matches.iloc[0]

    if selected is None:
        selected = df.iloc[0]
        st.session_state[session_key] = selected["table_name"]

    return selected


def render_table_details(selected: pd.Series, include_summary_stats: bool) -> None:
    full_name = selected["table_name"]
    st.subheader(full_name)

    metric_col_1, metric_col_2, metric_col_3 = st.columns(3)
    metric_col_1.metric("Rows", f"{int(selected['row_count']):,}" if pd.notna(selected["row_count"]) else "N/A")
    metric_col_2.metric(
        "Size (GB)",
        f"{float(selected['size_gb']):,.4f}" if pd.notna(selected["size_gb"]) else "N/A",
    )
    render_timestamp_metric(
        metric_col_3,
        "Latest row timestamp" if include_summary_stats else "Last updated",
        selected["latest_row_timestamp"] if include_summary_stats else selected["last_altered"],
    )

    columns_df = fetch_table_columns(full_name)
    sample_df = fetch_example_row(full_name)

    detail_tab_1, detail_tab_2, detail_tab_3 = st.tabs(
        ["Columns", "Example Row", "Summary Stats"]
    )
    with detail_tab_1:
        st.dataframe(columns_df, hide_index=True, width="stretch")
    with detail_tab_2:
        if sample_df.empty:
            st.info("This table currently has no rows.")
        else:
            st.dataframe(sample_df, hide_index=True, width="stretch")
    with detail_tab_3:
        if not include_summary_stats:
            st.caption("Summary statistics are shown for analytics-layer tables only.")
        else:
            numeric_stats, temporal_stats = fetch_table_summary_stats(full_name)
            if numeric_stats.empty and temporal_stats.empty:
                st.info("No numeric or temporal columns were available for summary statistics.")
            else:
                if not numeric_stats.empty:
                    st.write("Numeric columns")
                    st.dataframe(numeric_stats, hide_index=True, width="stretch")
                if not temporal_stats.empty:
                    st.write("Date and timestamp columns")
                    st.dataframe(temporal_stats, hide_index=True, width="stretch")

    if include_summary_stats:
        lineage = fetch_analytics_lineage(full_name)
        st.divider()
        st.subheader("Build Lineage")
        if lineage is None:
            st.info("No lineage metadata is available yet for this analytics table.")
            return

        st.caption(str(lineage["build_logic"]))
        st.write(f"Build method: `{lineage['build_method']}`")

        source_df = pd.DataFrame(lineage["sources"])
        st.write("Source tables")
        st.dataframe(source_df, hide_index=True, width="stretch")

        joins = lineage["joins"]
        st.write("Join conditions")
        if joins:
            st.dataframe(pd.DataFrame(joins), hide_index=True, width="stretch")
        else:
            st.caption("No joins. This table is built from a single source or projection.")

        st.write("Query shape")
        st.code(str(lineage["query_sql"]), language="sql")


def main() -> None:
    st.title("Data Availability")
    st.caption(
        "Developer-focused inventory of source tables and analytics-layer tables in Snowflake."
    )

    refresh_col, summary_col = st.columns([1, 3])
    with refresh_col:
        if st.button("Refresh metadata"):
            st.cache_data.clear()
            st.rerun()
    with summary_col:
        st.caption(
            "Base-table timestamps come from Snowflake table metadata. "
            "Analytics-table freshness uses per-table row timestamps plus the analytics build state."
        )

    base_specs = build_base_table_specs()
    base_specs_tuple = tuple(
        (spec["system"], spec["layer"], spec["table_name"], spec["description"])
        for spec in base_specs
    )
    base_tables_df = fetch_table_inventory_metadata(base_specs_tuple)
    analytics_tables_df = fetch_analytics_inventory_metadata()
    analytics_build_state_df = fetch_analytics_build_state()

    base_tab, analytics_tab = st.tabs(["Base Tables", "Analytics Layer"])

    with base_tab:
        st.write(
            "These are the upstream project tables across PANTHER, COYOTE, and DOG that feed the analytics layer."
        )
        base_selection = st.dataframe(
            base_tables_df[
                [
                    "system",
                    "layer",
                    "table_name",
                    "row_count",
                    "last_altered",
                    "size_gb",
                    "description",
                ]
            ],
            hide_index=True,
            on_select="rerun",
            selection_mode="single-row",
            width="stretch",
            column_config={
                "system": st.column_config.TextColumn("System"),
                "layer": st.column_config.TextColumn("Layer"),
                "table_name": st.column_config.TextColumn("Table"),
                "row_count": st.column_config.NumberColumn("Rows", format="%d"),
                "last_altered": st.column_config.DatetimeColumn("Last updated"),
                "size_gb": st.column_config.NumberColumn("Size (GB)", format="%.4f"),
                "description": st.column_config.TextColumn("Purpose"),
            },
            key="base_table_inventory",
        )
        selected_base = selected_row_or_default(
            base_tables_df, base_selection, "selected_base_table"
        )
        if selected_base is not None:
            render_table_details(selected_base, include_summary_stats=False)

    with analytics_tab:
        st.write(
            "These are the tables materialized in `PANTHER_DB.ANALYTICS` by the Snowpark analytics build."
        )
        if not analytics_build_state_df.empty:
            build_state = analytics_build_state_df.iloc[0]
            st.caption(
                "Last successful analytics build run: "
                f"`{format_timestamp(build_state['updated_at'])}` "
                f"(mode: `{build_state['last_build_mode']}`, "
                f"COYOTE watermark: `{format_timestamp(build_state['last_successful_coyote_loaded_at'])}`)"
            )
        if not analytics_tables_df.empty:
            metric_col_1, metric_col_2, metric_col_3, metric_col_4 = st.columns(4)
            metric_col_1.metric("Analytics tables", f"{len(analytics_tables_df):,}")
            metric_col_2.metric(
                "Total rows (metadata)",
                f"{int(analytics_tables_df['row_count'].fillna(0).sum()):,}",
            )
            metric_col_3.metric(
                "Total size (GB)",
                f"{float(analytics_tables_df['size_gb'].fillna(0).sum()):,.4f}",
            )
            render_timestamp_metric(
                metric_col_4,
                "Most recent row timestamp",
                analytics_tables_df["latest_row_timestamp"].max(),
            )

        analytics_selection = st.dataframe(
            analytics_tables_df[
                ["short_name", "table_name", "row_count", "latest_row_timestamp", "size_gb"]
            ],
            hide_index=True,
            on_select="rerun",
            selection_mode="single-row",
            width="stretch",
            column_config={
                "short_name": st.column_config.TextColumn("Analytics table"),
                "table_name": st.column_config.TextColumn("Full name"),
                "row_count": st.column_config.NumberColumn("Rows", format="%d"),
                "latest_row_timestamp": st.column_config.DatetimeColumn(
                    "Latest row timestamp"
                ),
                "size_gb": st.column_config.NumberColumn("Size (GB)", format="%.4f"),
            },
            key="analytics_table_inventory",
        )
        selected_analytics = selected_row_or_default(
            analytics_tables_df, analytics_selection, "selected_analytics_table"
        )
        if selected_analytics is not None:
            render_table_details(selected_analytics, include_summary_stats=True)


if __name__ == "__main__":
    main()
