from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path
import time
import tomllib

from snowflake.snowpark import Session
from snowflake.snowpark.functions import (
    avg,
    coalesce,
    col,
    count,
    count_distinct,
    current_timestamp,
    lit,
    max as max_,
    min as min_,
    round as round_,
    row_number,
    sql_expr,
    sum as sum_,
    to_date,
    to_timestamp_ntz,
    upper,
    when,
)
from snowflake.snowpark.window import Window


@dataclass(frozen=True)
class AnalyticsConfig:
    user_activity_table: str
    markets_table: str
    dog_data_api_trades_table: str
    dog_clob_trades_stream_table: str
    dog_clob_book_snapshots_table: str
    dog_leaderboard_users_table: str
    dog_detected_anomalies_table: str
    dog_market_daily_stats_table: str
    dog_wallet_daily_stats_table: str
    dog_anomaly_attribution_table: str
    dog_trader_risk_profiles_table: str
    tracked_market_volume_daily_table: str
    highest_volume_markets_table: str
    market_top_traders_daily_table: str
    market_top_traders_all_time_table: str
    market_concentration_daily_table: str
    platform_daily_summary_table: str
    trader_segment_snapshot_table: str
    trader_cohort_monthly_table: str
    market_theme_daily_table: str


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build the warehouse analytics layer used by the Streamlit app."
    )
    parser.add_argument(
        "--config-path",
        default=".streamlit/secrets.toml",
        help="Path to a TOML file with [snowflake] and [app] sections.",
    )
    parser.add_argument(
        "--mode",
        choices=["full", "incremental"],
        default="incremental",
        help="Use full rebuilds or incremental updates driven by COYOTE _LOADED_AT.",
    )
    return parser.parse_args()


def load_config(path: str) -> tuple[dict, AnalyticsConfig]:
    config_path = Path(path)
    with config_path.open("rb") as file:
        data = tomllib.load(file)

    snowflake_config = dict(data["snowflake"])
    app_config = dict(data.get("app", {}))

    analytics_config = AnalyticsConfig(
        user_activity_table=app_config.get(
            "user_activity_table",
            "COYOTE_DB.PUBLIC.CURATED_POLYMARKET_USER_ACTIVITY",
        ),
        markets_table=app_config.get(
            "markets_table", "PANTHER_DB.CURATED.GAMMA_MARKETS"
        ),
        dog_data_api_trades_table=app_config.get(
            "dog_data_api_trades_table", "DOG_DB.DOG_SCHEMA.DATA_API_TRADES"
        ),
        dog_clob_trades_stream_table=app_config.get(
            "dog_clob_trades_stream_table", "DOG_DB.DOG_SCHEMA.CLOB_TRADES_STREAM"
        ),
        dog_clob_book_snapshots_table=app_config.get(
            "dog_clob_book_snapshots_table",
            "DOG_DB.DOG_SCHEMA.CLOB_BOOK_SNAPSHOTS",
        ),
        dog_leaderboard_users_table=app_config.get(
            "dog_leaderboard_users_table", "DOG_DB.DOG_SCHEMA.LEADERBOARD_USERS"
        ),
        dog_detected_anomalies_table=app_config.get(
            "dog_detected_anomalies_table", "DOG_DB.DOG_SCHEMA.DETECTED_ANOMALIES"
        ),
        dog_market_daily_stats_table=app_config.get(
            "dog_market_daily_stats_table", "DOG_DB.DOG_TRANSFORM.MARKET_DAILY_STATS"
        ),
        dog_wallet_daily_stats_table=app_config.get(
            "dog_wallet_daily_stats_table", "DOG_DB.DOG_TRANSFORM.WALLET_DAILY_STATS"
        ),
        dog_anomaly_attribution_table=app_config.get(
            "dog_anomaly_attribution_table", "DOG_DB.DOG_TRANSFORM.ANOMALY_ATTRIBUTION"
        ),
        dog_trader_risk_profiles_table=app_config.get(
            "dog_trader_risk_profiles_table", "DOG_DB.DOG_TRANSFORM.TRADER_RISK_PROFILES"
        ),
        tracked_market_volume_daily_table=app_config.get(
            "tracked_market_volume_daily_table",
            "PANTHER_DB.ANALYTICS.TRACKED_MARKET_VOLUME_DAILY",
        ),
        highest_volume_markets_table=app_config.get(
            "highest_volume_markets_table",
            "PANTHER_DB.ANALYTICS.HIGHEST_VOLUME_MARKETS",
        ),
        market_top_traders_daily_table=app_config.get(
            "market_top_traders_daily_table",
            "PANTHER_DB.ANALYTICS.MARKET_TOP_TRADERS_DAILY",
        ),
        market_top_traders_all_time_table=app_config.get(
            "market_top_traders_all_time_table",
            "PANTHER_DB.ANALYTICS.MARKET_TOP_TRADERS_ALL_TIME",
        ),
        market_concentration_daily_table=app_config.get(
            "market_concentration_daily_table",
            "PANTHER_DB.ANALYTICS.MARKET_CONCENTRATION_DAILY",
        ),
        platform_daily_summary_table=app_config.get(
            "platform_daily_summary_table",
            "PANTHER_DB.ANALYTICS.PLATFORM_DAILY_SUMMARY",
        ),
        trader_segment_snapshot_table=app_config.get(
            "trader_segment_snapshot_table",
            "PANTHER_DB.ANALYTICS.TRADER_SEGMENT_SNAPSHOT",
        ),
        trader_cohort_monthly_table=app_config.get(
            "trader_cohort_monthly_table",
            "PANTHER_DB.ANALYTICS.TRADER_COHORT_MONTHLY",
        ),
        market_theme_daily_table=app_config.get(
            "market_theme_daily_table",
            "PANTHER_DB.ANALYTICS.MARKET_THEME_DAILY",
        ),
    )

    return snowflake_config, analytics_config


def schema_name_for(table_name: str) -> str:
    parts = table_name.split(".")
    if len(parts) != 3:
        raise ValueError(
            f"Expected a fully qualified table name, got {table_name!r}."
        )
    return ".".join(parts[:2])


def table_name(schema_name: str, object_name: str) -> str:
    return f"{schema_name}.{object_name}"


FACT_MARKET_DAILY_COLUMNS = [
    "STAT_DATE",
    "MARKET_ID",
    "CONDITION_ID",
    "MARKET_QUESTION",
    "MARKET_LABEL",
    "ACTIVE",
    "CLOSED",
    "TRADE_COUNT",
    "BUY_COUNT",
    "SELL_COUNT",
    "TOTAL_VOLUME",
    "TOTAL_USDC_VOLUME",
    "UNIQUE_WALLETS",
    "AVG_TRADE_SIZE",
    "MAX_TRADE_SIZE",
    "VWAP",
    "FIRST_TRADE_TS",
    "LAST_TRADE_TS",
    "PRICE_OPEN",
    "PRICE_CLOSE",
    "PRICE_HIGH",
    "PRICE_LOW",
    "ANOMALY_COUNT",
    "LAST_REFRESHED_AT",
]

FACT_WALLET_DAILY_COLUMNS = [
    "PROXY_WALLET",
    "STAT_DATE",
    "TRADE_COUNT",
    "DISTINCT_MARKETS",
    "TOTAL_VOLUME",
    "TOTAL_USDC_VOLUME",
    "BUY_VOLUME",
    "SELL_VOLUME",
    "AVG_TRADE_SIZE",
    "MAX_TRADE_SIZE",
    "ANOMALY_INVOLVEMENT",
    "LAST_REFRESHED_AT",
]

DIM_TRADERS_COLUMNS = [
    "PROXY_WALLET",
    "LEADERBOARD_RANK",
    "USER_NAME",
    "X_USERNAME",
    "VERIFIED_BADGE",
    "LEADERBOARD_VOLUME",
    "LEADERBOARD_PNL",
    "TOTAL_TRADES",
    "TOTAL_VOLUME",
    "DISTINCT_MARKETS",
    "AVG_TRADE_SIZE",
    "MAX_SINGLE_TRADE",
    "WIN_RATE",
    "AVG_SECONDS_BEFORE_RESOLUTION",
    "ANOMALY_INVOLVEMENT_COUNT",
    "ANOMALY_INVOLVEMENT_RATE",
    "TOP_DETECTOR_TYPE",
    "RISK_SCORE",
    "FIRST_SEEN",
    "LAST_SEEN",
    "UPDATED_AT",
]

TRACKED_MARKET_VOLUME_DAILY_COLUMNS = [
    "TRADE_DATE",
    "MARKET_ID",
    "CONDITION_ID",
    "MARKET_QUESTION",
    "MARKET_LABEL",
    "ACTIVE",
    "CLOSED",
    "TRADE_COUNT",
    "UNIQUE_TRADERS",
    "TOTAL_VOLUME_USDC",
    "FIRST_TRADE_TS",
    "LAST_TRADE_TS",
    "LAST_REFRESHED_AT",
]

MARKET_TOP_TRADERS_DAILY_COLUMNS = [
    "TRADE_DATE",
    "MARKET_ID",
    "CONDITION_ID",
    "MARKET_QUESTION",
    "MARKET_LABEL",
    "PROXY_WALLET",
    "TOTAL_VOLUME_USDC",
    "TRADE_COUNT",
    "AVERAGE_TRADE_USDC",
    "LARGEST_TRADE_USDC",
    "LAST_REFRESHED_AT",
]

MARKET_TOP_TRADERS_ALL_TIME_COLUMNS = [
    "MARKET_ID",
    "CONDITION_ID",
    "MARKET_QUESTION",
    "MARKET_LABEL",
    "PROXY_WALLET",
    "TOTAL_VOLUME_USDC",
    "TRADE_COUNT",
    "AVERAGE_TRADE_USDC",
    "LARGEST_TRADE_USDC",
    "LAST_REFRESHED_AT",
]

MARKET_THEME_DAILY_COLUMNS = [
    "TRADE_DATE",
    "MARKET_THEME",
    "MARKET_THEME_SOURCE",
    "MARKET_GROUP_TITLE",
    "EVENT_TITLE",
    "EVENT_SLUG",
    "DISTINCT_MARKETS",
    "TRADE_COUNT",
    "UNIQUE_TRADERS",
    "TOTAL_VOLUME_USDC",
    "AVG_TRADE_SIZE",
    "MAX_TRADE_SIZE",
    "FIRST_TRADE_TS",
    "LAST_TRADE_TS",
    "LAST_REFRESHED_AT",
]


def parse_table_name(table_name_str: str) -> tuple[str, str, str]:
    parts = table_name_str.split(".")
    if len(parts) != 3:
        raise ValueError(
            f"Expected fully qualified DATABASE.SCHEMA.TABLE, got {table_name_str!r}."
        )
    return parts[0], parts[1], parts[2]


def fetch_table_row_count_metadata(session: Session, table_name_str: str) -> int | None:
    database, schema, table = parse_table_name(table_name_str)
    metadata_query = f"""
        SELECT row_count
        FROM {database}.information_schema.tables
        WHERE table_schema = '{schema}'
          AND table_name = '{table}'
    """
    rows = session.sql(metadata_query).collect()
    if not rows:
        return None
    return rows[0]["ROW_COUNT"]


def fetch_table_columns_metadata(session: Session, table_name_str: str) -> list[str]:
    database, schema, table = parse_table_name(table_name_str)
    query = f"""
        SELECT column_name
        FROM {database}.information_schema.columns
        WHERE table_schema = '{schema}'
          AND table_name = '{table}'
        ORDER BY ordinal_position
    """
    return [row["COLUMN_NAME"] for row in session.sql(query).collect()]


def format_percent(numerator: int, denominator: int) -> str:
    if denominator <= 0:
        return "N/A"
    return f"{(numerator / denominator) * 100:,.4f}%"


def print_source_inventory(session: Session, config: AnalyticsConfig) -> None:
    source_tables = [
        ("PANTHER markets", config.markets_table),
        ("COYOTE user activity", config.user_activity_table),
        ("DOG data API trades", config.dog_data_api_trades_table),
        ("DOG CLOB trades", config.dog_clob_trades_stream_table),
        ("DOG book snapshots", config.dog_clob_book_snapshots_table),
        ("DOG leaderboard users", config.dog_leaderboard_users_table),
        ("DOG detected anomalies", config.dog_detected_anomalies_table),
        ("DOG anomaly attribution", config.dog_anomaly_attribution_table),
        ("DOG trader risk profiles", config.dog_trader_risk_profiles_table),
    ]

    print("[build] Source table row counts from Snowflake metadata:", flush=True)
    for label, full_name in source_tables:
        row_count = fetch_table_row_count_metadata(session, full_name)
        row_count_text = f"{row_count:,}" if row_count is not None else "unavailable"
        print(f"[build]   {label}: {full_name} -> rows={row_count_text}", flush=True)
    print(
        "[build] Note: analytics uses only rows where COYOTE.TYPE = 'TRADE' from the "
        "user activity table.",
        flush=True,
    )


def table_exists(session: Session, table_name_str: str) -> bool:
    return fetch_table_row_count_metadata(session, table_name_str) is not None


def temporary_table_name(analytics_schema: str, prefix: str) -> str:
    return table_name(analytics_schema, f"TMP_{prefix}_{int(time.time_ns())}")


def quote_sql_identifier(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def normalize_column_name(name: str) -> str:
    if name.startswith('"') and name.endswith('"') and len(name) >= 2:
        return name[1:-1].replace('""', '"')
    return name


def normalize_dataframe_columns(df):
    return df.select(
        *[
            df[column_name].alias(normalize_column_name(column_name))
            for column_name in df.columns
        ]
    )


def assert_required_columns_exist(
    available_columns: list[str], required_columns: list[str], object_label: str
) -> None:
    missing = [column for column in required_columns if column not in available_columns]
    if missing:
        raise ValueError(
            f"{object_label} is missing required columns: {', '.join(missing)}. "
            f"Available columns: {', '.join(available_columns)}"
        )


def table_has_required_columns(
    session: Session, table_name_str: str, required_columns: list[str]
) -> bool:
    if not table_exists(session, table_name_str):
        return False
    available_columns = fetch_table_columns_metadata(session, table_name_str)
    return all(column in available_columns for column in required_columns)


def table_has_exact_columns(
    session: Session, table_name_str: str, expected_columns: list[str]
) -> bool:
    if not table_exists(session, table_name_str):
        return False
    available_columns = fetch_table_columns_metadata(session, table_name_str)
    return available_columns == expected_columns


def write_temp_table(df, table_name_str: str) -> None:
    df.write.mode("overwrite").save_as_table(table_name_str, table_type="temporary")


def filter_df_by_keys(base_df, keys_df, base_column: str, key_column: str | None = None):
    key_alias = "__FILTER_KEY"
    renamed_keys = keys_df.select(col(key_column or base_column).alias(key_alias)).distinct()
    matched = base_df.join(
        renamed_keys,
        base_df[base_column] == renamed_keys[key_alias],
        how="inner",
    )
    return matched.select(*[base_df[column] for column in base_df.columns])


def ensure_build_state_table(session: Session, analytics_schema: str) -> str:
    state_table_name = table_name(analytics_schema, "ANALYTICS_BUILD_STATE")
    session.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {state_table_name} (
            PIPELINE_NAME STRING,
            LAST_SUCCESSFUL_COYOTE_LOADED_AT TIMESTAMP_NTZ,
            LAST_BUILD_MODE STRING,
            UPDATED_AT TIMESTAMP_NTZ
        )
        """
    ).collect()
    return state_table_name


def get_last_coyote_watermark(session: Session, state_table_name: str) -> object | None:
    rows = session.sql(
        f"""
        SELECT LAST_SUCCESSFUL_COYOTE_LOADED_AT
        FROM {state_table_name}
        WHERE PIPELINE_NAME = 'analytics_layer_v2'
        """
    ).collect()
    if not rows:
        return None
    return rows[0]["LAST_SUCCESSFUL_COYOTE_LOADED_AT"]


def get_existing_fact_watermark(session: Session, fact_table_name: str):
    if not table_exists(session, fact_table_name):
        return None
    row = (
        session.table(fact_table_name)
        .agg(max_("SOURCE_LOADED_AT").alias("MAX_SOURCE_LOADED_AT"))
        .collect()[0]
    )
    return row["MAX_SOURCE_LOADED_AT"]


def update_build_state(
    session: Session,
    state_table_name: str,
    last_successful_coyote_loaded_at,
    build_mode: str,
) -> None:
    watermark_sql = (
        "NULL"
        if last_successful_coyote_loaded_at is None
        else f"TO_TIMESTAMP_NTZ('{last_successful_coyote_loaded_at.isoformat(sep=' ')}')"
    )
    session.sql(
        f"""
        MERGE INTO {state_table_name} target
        USING (
            SELECT
                'analytics_layer_v2' AS PIPELINE_NAME,
                {watermark_sql} AS LAST_SUCCESSFUL_COYOTE_LOADED_AT,
                '{build_mode}' AS LAST_BUILD_MODE,
                CURRENT_TIMESTAMP() AS UPDATED_AT
        ) source
        ON target.PIPELINE_NAME = source.PIPELINE_NAME
        WHEN MATCHED THEN UPDATE SET
            LAST_SUCCESSFUL_COYOTE_LOADED_AT = source.LAST_SUCCESSFUL_COYOTE_LOADED_AT,
            LAST_BUILD_MODE = source.LAST_BUILD_MODE,
            UPDATED_AT = source.UPDATED_AT
        WHEN NOT MATCHED THEN INSERT (
            PIPELINE_NAME,
            LAST_SUCCESSFUL_COYOTE_LOADED_AT,
            LAST_BUILD_MODE,
            UPDATED_AT
        ) VALUES (
            source.PIPELINE_NAME,
            source.LAST_SUCCESSFUL_COYOTE_LOADED_AT,
            source.LAST_BUILD_MODE,
            source.UPDATED_AT
        )
        """
    ).collect()


def merge_rows_by_keys(
    session: Session,
    analytics_schema: str,
    target_table_name: str,
    source_df,
    key_columns: list[str],
) -> None:
    started_at = time.perf_counter()
    print(
        f"[build] Incremental merge into {target_table_name} on keys "
        f"{', '.join(key_columns)} ...",
        flush=True,
    )
    normalized_df = normalize_dataframe_columns(source_df)
    normalized_columns = [normalize_column_name(column) for column in normalized_df.columns]
    assert_required_columns_exist(
        normalized_columns,
        key_columns,
        f"incremental source for {target_table_name}",
    )
    if table_exists(session, target_table_name):
        assert_required_columns_exist(
            fetch_table_columns_metadata(session, target_table_name),
            key_columns,
            f"target table {target_table_name}",
        )
    temp_table = temporary_table_name(analytics_schema, "MERGE_ROWS")
    write_temp_table(normalized_df, temp_table)
    quoted_source_columns = [quote_sql_identifier(column) for column in normalized_columns]
    column_list = ", ".join(quoted_source_columns)
    source_column_list = ", ".join(
        f"source.{quote_sql_identifier(column)}" for column in normalized_columns
    )
    merge_condition = " AND ".join(
        (
            f"target.{quote_sql_identifier(column)} = "
            f"source.{quote_sql_identifier(column)}"
        )
        for column in key_columns
    )
    session.sql(
        f"""
        MERGE INTO {target_table_name} target
        USING {temp_table} source
        ON {merge_condition}
        WHEN NOT MATCHED THEN INSERT ({column_list})
        VALUES ({source_column_list})
        """
    ).collect()
    elapsed = time.perf_counter() - started_at
    print(
        f"[build] Finished incremental merge into {target_table_name} in {elapsed:,.1f}s",
        flush=True,
    )


def replace_rows_by_keys(
    session: Session,
    analytics_schema: str,
    target_table_name: str,
    source_df,
    key_columns: list[str],
) -> None:
    started_at = time.perf_counter()
    print(
        f"[build] Incremental replace in {target_table_name} on keys "
        f"{', '.join(key_columns)} ...",
        flush=True,
    )
    normalized_df = normalize_dataframe_columns(source_df)
    normalized_columns = [normalize_column_name(column) for column in normalized_df.columns]
    assert_required_columns_exist(
        normalized_columns,
        key_columns,
        f"incremental source for {target_table_name}",
    )
    if table_exists(session, target_table_name):
        assert_required_columns_exist(
            fetch_table_columns_metadata(session, target_table_name),
            key_columns,
            f"target table {target_table_name}",
        )
    temp_table = temporary_table_name(analytics_schema, "REPLACE_ROWS")
    write_temp_table(normalized_df, temp_table)
    quoted_source_columns = [quote_sql_identifier(column) for column in normalized_columns]
    column_list = ", ".join(quoted_source_columns)
    source_column_list = ", ".join(
        f"source.{quote_sql_identifier(column)}" for column in normalized_columns
    )
    key_list = ", ".join(quote_sql_identifier(column) for column in key_columns)
    delete_condition = " AND ".join(
        (
            f"target.{quote_sql_identifier(column)} = "
            f"impacted.{quote_sql_identifier(column)}"
        )
        for column in key_columns
    )
    session.sql(
        f"""
        DELETE FROM {target_table_name} target
        USING (SELECT DISTINCT {key_list} FROM {temp_table}) impacted
        WHERE {delete_condition}
        """
    ).collect()
    session.sql(
        f"""
        INSERT INTO {target_table_name} ({column_list})
        SELECT {source_column_list}
        FROM {temp_table} source
        """
    ).collect()
    elapsed = time.perf_counter() - started_at
    print(
        f"[build] Finished incremental replace in {target_table_name} in {elapsed:,.1f}s",
        flush=True,
    )


def write_table(df, table_name_str: str) -> None:
    started_at = time.perf_counter()
    print(f"[build] Writing {table_name_str} ...", flush=True)
    df.write.mode("overwrite").save_as_table(table_name_str, table_type="transient")
    elapsed = time.perf_counter() - started_at
    print(f"[build] Finished {table_name_str} in {elapsed:,.1f}s", flush=True)


def build_latest_markets_df(session: Session, config: AnalyticsConfig):
    markets = session.table(config.markets_table)
    latest_market_window = Window.partition_by("CONDITION_ID").order_by(
        col("UPDATED_AT").desc_nulls_last(),
        col("TRANSFORMED_AT").desc_nulls_last(),
        col("MARKET_ID"),
    )

    return (
        markets.filter(col("CONDITION_ID").is_not_null())
        .with_column("ROW_NUM", row_number().over(latest_market_window))
        .filter(col("ROW_NUM") == 1)
        .select(
            col("MARKET_ID").alias("MARKET_ID"),
            col("CONDITION_ID").alias("CONDITION_ID"),
            coalesce(col("QUESTION"), col("SLUG"), col("MARKET_ID")).alias(
                "MARKET_QUESTION"
            ),
            coalesce(col("SLUG"), col("QUESTION"), col("MARKET_ID")).alias(
                "MARKET_LABEL"
            ),
            col("SLUG").alias("MARKET_SLUG"),
            col("QUESTION").alias("QUESTION_TEXT"),
            col("GROUP_ITEM_TITLE").alias("MARKET_GROUP_TITLE"),
            sql_expr("events[0]:title::STRING").alias("EVENT_TITLE"),
            sql_expr("events[0]:slug::STRING").alias("EVENT_SLUG"),
            coalesce(
                col("GROUP_ITEM_TITLE"),
                sql_expr("events[0]:title::STRING"),
                col("QUESTION"),
                col("SLUG"),
                col("MARKET_ID"),
            ).alias("MARKET_THEME"),
            when(col("GROUP_ITEM_TITLE").is_not_null(), lit("GROUP_ITEM_TITLE"))
            .when(sql_expr("events[0]:title::STRING").is_not_null(), lit("EVENT_TITLE"))
            .otherwise(lit("MARKET_LABEL_FALLBACK"))
            .alias("MARKET_THEME_SOURCE"),
            col("START_DATE").alias("START_DATE"),
            col("END_DATE").alias("END_DATE"),
            coalesce(col("ACTIVE"), lit(False)).alias("ACTIVE"),
            coalesce(col("CLOSED"), lit(False)).alias("CLOSED"),
            coalesce(col("BEST_BID"), lit(0)).alias("BEST_BID"),
            coalesce(col("BEST_ASK"), lit(0)).alias("BEST_ASK"),
            coalesce(col("SPREAD"), lit(0)).alias("SPREAD"),
            coalesce(col("LAST_TRADE_PRICE"), lit(0)).alias("LAST_TRADE_PRICE"),
            coalesce(col("LIQUIDITY_NUM"), col("LIQUIDITY"), col("LIQUIDITY_CLOB"), lit(0)).alias(
                "LIQUIDITY_USDC"
            ),
            coalesce(col("VOLUME_24HR"), col("VOLUME_24HR_CLOB"), lit(0)).alias(
                "VOLUME_24HR_USDC"
            ),
            coalesce(col("VOLUME_NUM"), col("VOLUME"), col("VOLUME_CLOB"), lit(0)).alias(
                "LISTED_VOLUME_USDC"
            ),
            col("OUTCOMES").alias("OUTCOMES"),
            col("CLOB_TOKEN_IDS").alias("CLOB_TOKEN_IDS"),
            col("UPDATED_AT").alias("MARKET_UPDATED_AT"),
            col("TRANSFORMED_AT").alias("MARKET_TRANSFORMED_AT"),
        )
    )


def build_market_assets_df(
    session: Session, config: AnalyticsConfig, analytics_schema: str
):
    dim_markets_name = table_name(analytics_schema, "DIM_MARKETS")
    query = f"""
        SELECT
            market_id,
            condition_id,
            token_flat.index::INTEGER AS outcome_index,
            token_flat.value::STRING AS asset_id,
            outcome_flat.value::STRING AS outcome_name
        FROM {dim_markets_name},
             LATERAL FLATTEN(input => clob_token_ids) token_flat,
             LATERAL FLATTEN(input => outcomes) outcome_flat
        WHERE token_flat.index = outcome_flat.index
    """
    return session.sql(query)


def build_data_api_trades_df(session: Session, config: AnalyticsConfig, dim_markets):
    trades = session.table(config.dog_data_api_trades_table)
    enriched_trades = trades.join(
        dim_markets,
        trades["CONDITION_ID"] == dim_markets["CONDITION_ID"],
        how="left",
    )

    return enriched_trades.select(
        to_date(col("TRADE_TIMESTAMP")).alias("TRADE_DATE"),
        to_timestamp_ntz(col("TRADE_TIMESTAMP")).alias("TRADE_TS"),
        coalesce(dim_markets["MARKET_ID"], lit(None)).alias("MARKET_ID"),
        trades["CONDITION_ID"].alias("CONDITION_ID"),
        coalesce(dim_markets["MARKET_QUESTION"], trades["TITLE"]).alias("MARKET_QUESTION"),
        coalesce(dim_markets["MARKET_LABEL"], trades["SLUG"], trades["TITLE"]).alias(
            "MARKET_LABEL"
        ),
        coalesce(dim_markets["ACTIVE"], lit(False)).alias("ACTIVE"),
        coalesce(dim_markets["CLOSED"], lit(False)).alias("CLOSED"),
        trades["PROXY_WALLET"].alias("PROXY_WALLET"),
        trades["ASSET"].alias("ASSET_ID"),
        trades["SIDE"].alias("TRADE_SIDE"),
        trades["OUTCOME"].alias("OUTCOME_NAME"),
        trades["OUTCOME_INDEX"].cast("INTEGER").alias("OUTCOME_INDEX"),
        trades["PRICE"].cast("DOUBLE").alias("PRICE"),
        trades["SIZE"].cast("DOUBLE").alias("SIZE"),
        round_(trades["PRICE"].cast("DOUBLE") * trades["SIZE"].cast("DOUBLE"), 2).alias(
            "USDC_VOLUME"
        ),
        trades["TRANSACTION_HASH"].alias("TRANSACTION_HASH"),
        trades["TITLE"].alias("SOURCE_TITLE"),
        trades["SLUG"].alias("SOURCE_SLUG"),
        trades["EVENT_SLUG"].alias("EVENT_SLUG"),
        trades["LOADED_AT"].alias("SOURCE_LOADED_AT"),
        lit("DOG_DATA_API").alias("SOURCE_SYSTEM"),
    )


def build_user_activity_trades_df(
    session: Session,
    config: AnalyticsConfig,
    dim_markets,
    loaded_at_or_after=None,
):
    activity = session.table(config.user_activity_table)
    trade_activity = activity.filter(upper(col("TYPE")) == lit("TRADE"))
    if loaded_at_or_after is not None:
        trade_activity = trade_activity.filter(
            col("_LOADED_AT") >= lit(loaded_at_or_after)
        )
    enriched_trades = trade_activity.join(
        dim_markets,
        trade_activity["CONDITIONID"] == dim_markets["CONDITION_ID"],
        how="left",
    )

    return enriched_trades.select(
        to_date(to_timestamp_ntz(trade_activity["TIMESTAMP"])).alias("TRADE_DATE"),
        to_timestamp_ntz(trade_activity["TIMESTAMP"]).alias("TRADE_TS"),
        coalesce(dim_markets["MARKET_ID"], lit(None)).alias("MARKET_ID"),
        trade_activity["CONDITIONID"].alias("CONDITION_ID"),
        coalesce(
            dim_markets["MARKET_QUESTION"],
            dim_markets["QUESTION_TEXT"],
            trade_activity["CONDITIONID"],
        ).alias("MARKET_QUESTION"),
        coalesce(
            dim_markets["MARKET_LABEL"],
            dim_markets["MARKET_SLUG"],
            trade_activity["CONDITIONID"],
        ).alias("MARKET_LABEL"),
        coalesce(dim_markets["ACTIVE"], lit(False)).alias("ACTIVE"),
        coalesce(dim_markets["CLOSED"], lit(False)).alias("CLOSED"),
        trade_activity["PROXYWALLET"].alias("PROXY_WALLET"),
        trade_activity["ASSET"].alias("ASSET_ID"),
        upper(trade_activity["SIDE"]).alias("TRADE_SIDE"),
        trade_activity["OUTCOME"].alias("OUTCOME_NAME"),
        trade_activity["OUTCOMEINDEX"].cast("INTEGER").alias("OUTCOME_INDEX"),
        trade_activity["PRICE"].cast("DOUBLE").alias("PRICE"),
        trade_activity["SIZE"].cast("DOUBLE").alias("SIZE"),
        coalesce(
            trade_activity["USDCSIZE"].cast("DOUBLE"),
            round_(
                trade_activity["PRICE"].cast("DOUBLE")
                * trade_activity["SIZE"].cast("DOUBLE"),
                2,
            ),
        ).alias("USDC_VOLUME"),
        trade_activity["TRANSACTIONHASH"].alias("TRANSACTION_HASH"),
        trade_activity["TYPE"].alias("ACTIVITY_TYPE"),
        trade_activity["_LOADED_AT"].alias("SOURCE_LOADED_AT"),
        lit("COYOTE_USER_ACTIVITY").alias("SOURCE_SYSTEM"),
    )


def build_clob_trades_df(
    session: Session,
    config: AnalyticsConfig,
    dim_markets,
    market_assets,
):
    trades = session.table(config.dog_clob_trades_stream_table)
    market_enriched = trades.join(
        dim_markets,
        trades["MARKET_CONDITION_ID"] == dim_markets["CONDITION_ID"],
        how="left",
    )
    fully_enriched = market_enriched.join(
        market_assets,
        (
            market_enriched["MARKET_CONDITION_ID"] == market_assets["CONDITION_ID"]
        )
        & (market_enriched["ASSET_ID"] == market_assets["ASSET_ID"]),
        how="left",
    )

    return fully_enriched.select(
        trades["STREAM_ID"].alias("STREAM_ID"),
        trades["MARKET_CONDITION_ID"].alias("CONDITION_ID"),
        dim_markets["MARKET_ID"].alias("MARKET_ID"),
        coalesce(dim_markets["MARKET_QUESTION"], lit(None)).alias("MARKET_QUESTION"),
        coalesce(dim_markets["MARKET_LABEL"], lit(None)).alias("MARKET_LABEL"),
        trades["ASSET_ID"].alias("ASSET_ID"),
        market_assets["OUTCOME_INDEX"].alias("OUTCOME_INDEX"),
        market_assets["OUTCOME_NAME"].alias("OUTCOME_NAME"),
        trades["WALLET_ADDRESS"].alias("PROXY_WALLET"),
        trades["SIDE"].alias("TRADE_SIDE"),
        trades["PRICE"].cast("DOUBLE").alias("PRICE"),
        trades["SIZE"].cast("DOUBLE").alias("SIZE"),
        round_(trades["PRICE"].cast("DOUBLE") * trades["SIZE"].cast("DOUBLE"), 2).alias(
            "USDC_VOLUME"
        ),
        trades["FEE_RATE_BPS"].cast("DOUBLE").alias("FEE_RATE_BPS"),
        trades["WS_TIMESTAMP"].alias("TRADE_TS"),
        to_date(trades["WS_TIMESTAMP"]).alias("TRADE_DATE"),
        trades["ENRICHED_AT"].alias("ENRICHED_AT"),
        trades["STREAMED_AT"].alias("STREAMED_AT"),
    )


def build_book_snapshots_df(
    session: Session,
    config: AnalyticsConfig,
    dim_markets,
    market_assets,
):
    snapshots = session.table(config.dog_clob_book_snapshots_table)
    market_enriched = snapshots.join(
        dim_markets,
        snapshots["MARKET_CONDITION_ID"] == dim_markets["CONDITION_ID"],
        how="left",
    )
    fully_enriched = market_enriched.join(
        market_assets,
        (
            market_enriched["MARKET_CONDITION_ID"] == market_assets["CONDITION_ID"]
        )
        & (market_enriched["ASSET_ID"] == market_assets["ASSET_ID"]),
        how="left",
    )

    return fully_enriched.select(
        snapshots["SNAPSHOT_ID"].alias("SNAPSHOT_ID"),
        snapshots["MARKET_CONDITION_ID"].alias("CONDITION_ID"),
        dim_markets["MARKET_ID"].alias("MARKET_ID"),
        coalesce(dim_markets["MARKET_QUESTION"], lit(None)).alias("MARKET_QUESTION"),
        coalesce(dim_markets["MARKET_LABEL"], lit(None)).alias("MARKET_LABEL"),
        snapshots["ASSET_ID"].alias("ASSET_ID"),
        market_assets["OUTCOME_INDEX"].alias("OUTCOME_INDEX"),
        market_assets["OUTCOME_NAME"].alias("OUTCOME_NAME"),
        snapshots["BEST_BID"].cast("DOUBLE").alias("BEST_BID"),
        snapshots["BEST_ASK"].cast("DOUBLE").alias("BEST_ASK"),
        snapshots["SPREAD"].cast("DOUBLE").alias("SPREAD"),
        snapshots["SNAPSHOT_TS"].alias("SNAPSHOT_TS"),
        to_date(snapshots["SNAPSHOT_TS"]).alias("SNAPSHOT_DATE"),
        snapshots["STREAMED_AT"].alias("STREAMED_AT"),
    )


def build_leaderboard_snapshots_df(session: Session, config: AnalyticsConfig):
    leaderboard = session.table(config.dog_leaderboard_users_table)
    return leaderboard.select(
        col("PROXY_WALLET").alias("PROXY_WALLET"),
        col("RANK").cast("INTEGER").alias("LEADERBOARD_RANK"),
        col("USER_NAME").alias("USER_NAME"),
        col("X_USERNAME").alias("X_USERNAME"),
        col("VERIFIED_BADGE").alias("VERIFIED_BADGE"),
        col("VOLUME").cast("DOUBLE").alias("LEADERBOARD_VOLUME"),
        col("PNL").cast("DOUBLE").alias("LEADERBOARD_PNL"),
        col("PROFILE_IMAGE").alias("PROFILE_IMAGE"),
        col("SNAPSHOT_DATE").alias("SNAPSHOT_DATE"),
        col("LOADED_AT").alias("LOADED_AT"),
    )


def build_leaderboard_current_df(leaderboard_snapshots):
    latest_leaderboard_window = Window.partition_by("PROXY_WALLET").order_by(
        col("SNAPSHOT_DATE").desc_nulls_last(),
        col("LOADED_AT").desc_nulls_last(),
    )

    return (
        leaderboard_snapshots.with_column(
            "ROW_NUM", row_number().over(latest_leaderboard_window)
        )
        .filter(col("ROW_NUM") == 1)
        .drop("ROW_NUM")
    )


def build_anomalies_df(
    session: Session,
    config: AnalyticsConfig,
    dim_markets,
    market_assets,
):
    anomalies = session.table(config.dog_detected_anomalies_table)
    market_enriched = anomalies.join(
        dim_markets,
        anomalies["MARKET_CONDITION_ID"] == dim_markets["CONDITION_ID"],
        how="left",
    )
    fully_enriched = market_enriched.join(
        market_assets,
        (
            anomalies["MARKET_CONDITION_ID"] == market_assets["CONDITION_ID"]
        )
        & (anomalies["ASSET_ID"] == market_assets["ASSET_ID"]),
        how="left",
    )

    return fully_enriched.select(
        anomalies["ANOMALY_ID"].alias("ANOMALY_ID"),
        anomalies["DETECTOR_TYPE"].alias("DETECTOR_TYPE"),
        anomalies["MARKET_CONDITION_ID"].alias("CONDITION_ID"),
        dim_markets["MARKET_ID"].alias("MARKET_ID"),
        coalesce(dim_markets["MARKET_QUESTION"], lit(None)).alias("MARKET_QUESTION"),
        coalesce(dim_markets["MARKET_LABEL"], lit(None)).alias("MARKET_LABEL"),
        anomalies["ASSET_ID"].alias("ASSET_ID"),
        market_assets["OUTCOME_INDEX"].alias("OUTCOME_INDEX"),
        market_assets["OUTCOME_NAME"].alias("OUTCOME_NAME"),
        anomalies["SEVERITY_SCORE"].cast("DOUBLE").alias("SEVERITY_SCORE"),
        anomalies["DETECTED_AT"].alias("DETECTED_AT"),
        to_date(anomalies["DETECTED_AT"]).alias("DETECTED_DATE"),
    )


def build_anomaly_attribution_df(
    session: Session,
    config: AnalyticsConfig,
    dim_markets,
):
    attribution = session.table(config.dog_anomaly_attribution_table)
    enriched = attribution.join(
        dim_markets,
        attribution["MARKET_CONDITION_ID"] == dim_markets["CONDITION_ID"],
        how="left",
    )

    return enriched.select(
        attribution["ATTRIBUTION_ID"].alias("ATTRIBUTION_ID"),
        attribution["ANOMALY_ID"].alias("ANOMALY_ID"),
        attribution["PROXY_WALLET"].alias("PROXY_WALLET"),
        attribution["MARKET_CONDITION_ID"].alias("CONDITION_ID"),
        dim_markets["MARKET_ID"].alias("MARKET_ID"),
        coalesce(dim_markets["MARKET_QUESTION"], lit(None)).alias("MARKET_QUESTION"),
        coalesce(dim_markets["MARKET_LABEL"], lit(None)).alias("MARKET_LABEL"),
        attribution["TRADE_TIMESTAMP"].alias("TRADE_TIMESTAMP"),
        to_date(attribution["TRADE_TIMESTAMP"]).alias("TRADE_DATE"),
        attribution["TRADE_SIDE"].alias("TRADE_SIDE"),
        attribution["TRADE_SIZE"].cast("DOUBLE").alias("TRADE_SIZE"),
        attribution["TRADE_PRICE"].cast("DOUBLE").alias("TRADE_PRICE"),
        attribution["TRANSACTION_HASH"].alias("TRANSACTION_HASH"),
        attribution["ATTRIBUTED_AT"].alias("ATTRIBUTED_AT"),
    )


def build_market_daily_df(fact_trades, fact_anomalies, dim_markets):
    grouped_stats = (
        fact_trades.group_by(
            "TRADE_DATE",
            "MARKET_ID",
            "CONDITION_ID",
            "MARKET_QUESTION",
            "MARKET_LABEL",
            "ACTIVE",
            "CLOSED",
        )
        .agg(
            count(lit(1)).alias("TRADE_COUNT"),
            sum_(when(col("TRADE_SIDE") == lit("BUY"), lit(1)).otherwise(lit(0))).alias(
                "BUY_COUNT"
            ),
            sum_(when(col("TRADE_SIDE") == lit("SELL"), lit(1)).otherwise(lit(0))).alias(
                "SELL_COUNT"
            ),
            round_(sum_("SIZE"), 6).alias("TOTAL_VOLUME"),
            round_(sum_("USDC_VOLUME"), 2).alias("TOTAL_USDC_VOLUME"),
            count_distinct("PROXY_WALLET").alias("UNIQUE_WALLETS"),
            round_(avg("SIZE"), 6).alias("AVG_TRADE_SIZE"),
            round_(max_("SIZE"), 6).alias("MAX_TRADE_SIZE"),
            round_(
                sum_(col("PRICE") * col("SIZE")) / sum_(col("SIZE")),
                6,
            ).alias("VWAP"),
            min_("TRADE_TS").alias("FIRST_TRADE_TS"),
            max_("TRADE_TS").alias("LAST_TRADE_TS"),
            round_(max_("PRICE"), 6).alias("PRICE_HIGH"),
            round_(min_("PRICE"), 6).alias("PRICE_LOW"),
        )
    )

    open_window = Window.partition_by("CONDITION_ID", "TRADE_DATE").order_by(
        col("TRADE_TS").asc_nulls_last(), col("TRANSACTION_HASH")
    )
    close_window = Window.partition_by("CONDITION_ID", "TRADE_DATE").order_by(
        col("TRADE_TS").desc_nulls_last(), col("TRANSACTION_HASH").desc_nulls_last()
    )

    open_prices = (
        fact_trades.with_column("OPEN_RN", row_number().over(open_window))
        .filter(col("OPEN_RN") == 1)
        .select(
            col("CONDITION_ID").alias("OPEN_CONDITION_ID"),
            col("TRADE_DATE").alias("OPEN_TRADE_DATE"),
            round_(col("PRICE"), 6).alias("PRICE_OPEN"),
        )
    )
    close_prices = (
        fact_trades.with_column("CLOSE_RN", row_number().over(close_window))
        .filter(col("CLOSE_RN") == 1)
        .select(
            col("CONDITION_ID").alias("CLOSE_CONDITION_ID"),
            col("TRADE_DATE").alias("CLOSE_TRADE_DATE"),
            round_(col("PRICE"), 6).alias("PRICE_CLOSE"),
        )
    )

    anomaly_counts = (
        fact_anomalies.group_by("CONDITION_ID", "DETECTED_DATE")
        .agg(count(lit(1)).alias("ANOMALY_COUNT"))
        .select(
            col("CONDITION_ID").alias("ANOMALY_CONDITION_ID"),
            col("DETECTED_DATE").alias("TRADE_DATE"),
            col("ANOMALY_COUNT"),
        )
    )

    enriched = (
        grouped_stats.join(
            open_prices,
            (grouped_stats["CONDITION_ID"] == open_prices["OPEN_CONDITION_ID"])
            & (grouped_stats["TRADE_DATE"] == open_prices["OPEN_TRADE_DATE"]),
            how="left",
        )
        .join(
            close_prices,
            (grouped_stats["CONDITION_ID"] == close_prices["CLOSE_CONDITION_ID"])
            & (grouped_stats["TRADE_DATE"] == close_prices["CLOSE_TRADE_DATE"]),
            how="left",
        )
        .join(
            anomaly_counts,
            (grouped_stats["CONDITION_ID"] == anomaly_counts["ANOMALY_CONDITION_ID"])
            & (grouped_stats["TRADE_DATE"] == anomaly_counts["TRADE_DATE"]),
            how="left",
        )
    )

    return enriched.select(
        grouped_stats["TRADE_DATE"].alias("STAT_DATE"),
        grouped_stats["MARKET_ID"].alias("MARKET_ID"),
        grouped_stats["CONDITION_ID"].alias("CONDITION_ID"),
        grouped_stats["MARKET_QUESTION"].alias("MARKET_QUESTION"),
        grouped_stats["MARKET_LABEL"].alias("MARKET_LABEL"),
        grouped_stats["ACTIVE"].alias("ACTIVE"),
        grouped_stats["CLOSED"].alias("CLOSED"),
        grouped_stats["TRADE_COUNT"].alias("TRADE_COUNT"),
        grouped_stats["BUY_COUNT"].alias("BUY_COUNT"),
        grouped_stats["SELL_COUNT"].alias("SELL_COUNT"),
        grouped_stats["TOTAL_VOLUME"].alias("TOTAL_VOLUME"),
        grouped_stats["TOTAL_USDC_VOLUME"].alias("TOTAL_USDC_VOLUME"),
        grouped_stats["UNIQUE_WALLETS"].alias("UNIQUE_WALLETS"),
        grouped_stats["AVG_TRADE_SIZE"].alias("AVG_TRADE_SIZE"),
        grouped_stats["MAX_TRADE_SIZE"].alias("MAX_TRADE_SIZE"),
        grouped_stats["VWAP"].alias("VWAP"),
        grouped_stats["FIRST_TRADE_TS"].alias("FIRST_TRADE_TS"),
        grouped_stats["LAST_TRADE_TS"].alias("LAST_TRADE_TS"),
        open_prices["PRICE_OPEN"].alias("PRICE_OPEN"),
        close_prices["PRICE_CLOSE"].alias("PRICE_CLOSE"),
        grouped_stats["PRICE_HIGH"].alias("PRICE_HIGH"),
        grouped_stats["PRICE_LOW"].alias("PRICE_LOW"),
        coalesce(anomaly_counts["ANOMALY_COUNT"], lit(0)).alias("ANOMALY_COUNT"),
        current_timestamp().alias("LAST_REFRESHED_AT"),
    )


def build_wallet_daily_df(fact_trades, anomaly_attribution):
    anomaly_counts = (
        anomaly_attribution.group_by("PROXY_WALLET", "TRADE_DATE")
        .agg(count(lit(1)).alias("ANOMALY_INVOLVEMENT"))
        .select(
            col("PROXY_WALLET").alias("ANOMALY_PROXY_WALLET"),
            col("TRADE_DATE"),
            col("ANOMALY_INVOLVEMENT"),
        )
    )

    grouped = fact_trades.group_by("PROXY_WALLET", "TRADE_DATE").agg(
        count(lit(1)).alias("TRADE_COUNT"),
        count_distinct("CONDITION_ID").alias("DISTINCT_MARKETS"),
        round_(sum_("SIZE"), 6).alias("TOTAL_VOLUME"),
        round_(sum_("USDC_VOLUME"), 2).alias("TOTAL_USDC_VOLUME"),
        round_(
            sum_(when(col("TRADE_SIDE") == lit("BUY"), col("USDC_VOLUME")).otherwise(lit(0))),
            2,
        ).alias("BUY_VOLUME"),
        round_(
            sum_(when(col("TRADE_SIDE") == lit("SELL"), col("USDC_VOLUME")).otherwise(lit(0))),
            2,
        ).alias("SELL_VOLUME"),
        round_(avg("SIZE"), 6).alias("AVG_TRADE_SIZE"),
        round_(max_("SIZE"), 6).alias("MAX_TRADE_SIZE"),
    )

    enriched = grouped.join(
        anomaly_counts,
        (grouped["PROXY_WALLET"] == anomaly_counts["ANOMALY_PROXY_WALLET"])
        & (grouped["TRADE_DATE"] == anomaly_counts["TRADE_DATE"]),
        how="left",
    )

    return enriched.select(
        grouped["PROXY_WALLET"].alias("PROXY_WALLET"),
        grouped["TRADE_DATE"].alias("STAT_DATE"),
        grouped["TRADE_COUNT"].alias("TRADE_COUNT"),
        grouped["DISTINCT_MARKETS"].alias("DISTINCT_MARKETS"),
        grouped["TOTAL_VOLUME"].alias("TOTAL_VOLUME"),
        grouped["TOTAL_USDC_VOLUME"].alias("TOTAL_USDC_VOLUME"),
        grouped["BUY_VOLUME"].alias("BUY_VOLUME"),
        grouped["SELL_VOLUME"].alias("SELL_VOLUME"),
        grouped["AVG_TRADE_SIZE"].alias("AVG_TRADE_SIZE"),
        grouped["MAX_TRADE_SIZE"].alias("MAX_TRADE_SIZE"),
        coalesce(anomaly_counts["ANOMALY_INVOLVEMENT"], lit(0)).alias(
            "ANOMALY_INVOLVEMENT"
        ),
        current_timestamp().alias("LAST_REFRESHED_AT"),
    )


def build_trader_profiles_df(
    fact_trades,
    leaderboard_current,
    anomaly_attribution,
    source_risk_profiles,
):
    anomaly_by_wallet = (
        anomaly_attribution.group_by("PROXY_WALLET")
        .agg(count(lit(1)).alias("ANOMALY_INVOLVEMENT_COUNT"))
        .select(
            col("PROXY_WALLET").alias("ANOMALY_PROXY_WALLET"),
            col("ANOMALY_INVOLVEMENT_COUNT"),
        )
    )

    trade_rollup = fact_trades.group_by("PROXY_WALLET").agg(
        count(lit(1)).alias("TOTAL_TRADES"),
        round_(sum_("USDC_VOLUME"), 2).alias("TOTAL_VOLUME"),
        count_distinct("CONDITION_ID").alias("DISTINCT_MARKETS"),
        round_(avg("USDC_VOLUME"), 2).alias("AVG_TRADE_SIZE"),
        round_(max_("USDC_VOLUME"), 2).alias("MAX_SINGLE_TRADE"),
        min_("TRADE_TS").alias("FIRST_SEEN"),
        max_("TRADE_TS").alias("LAST_SEEN"),
    )

    enriched = (
        trade_rollup.join(
            leaderboard_current,
            trade_rollup["PROXY_WALLET"] == leaderboard_current["PROXY_WALLET"],
            how="left",
        )
        .join(
            anomaly_by_wallet,
            trade_rollup["PROXY_WALLET"] == anomaly_by_wallet["ANOMALY_PROXY_WALLET"],
            how="left",
        )
        .join(
            source_risk_profiles,
            trade_rollup["PROXY_WALLET"] == source_risk_profiles["PROXY_WALLET"],
            how="left",
        )
    )

    return enriched.select(
        trade_rollup["PROXY_WALLET"].alias("PROXY_WALLET"),
        leaderboard_current["LEADERBOARD_RANK"].alias("LEADERBOARD_RANK"),
        leaderboard_current["USER_NAME"].alias("USER_NAME"),
        leaderboard_current["X_USERNAME"].alias("X_USERNAME"),
        leaderboard_current["VERIFIED_BADGE"].alias("VERIFIED_BADGE"),
        leaderboard_current["LEADERBOARD_VOLUME"].alias("LEADERBOARD_VOLUME"),
        leaderboard_current["LEADERBOARD_PNL"].alias("LEADERBOARD_PNL"),
        trade_rollup["TOTAL_TRADES"].alias("TOTAL_TRADES"),
        trade_rollup["TOTAL_VOLUME"].alias("TOTAL_VOLUME"),
        trade_rollup["DISTINCT_MARKETS"].alias("DISTINCT_MARKETS"),
        trade_rollup["AVG_TRADE_SIZE"].alias("AVG_TRADE_SIZE"),
        trade_rollup["MAX_SINGLE_TRADE"].alias("MAX_SINGLE_TRADE"),
        source_risk_profiles["WIN_RATE"].alias("WIN_RATE"),
        source_risk_profiles["AVG_SECONDS_BEFORE_RESOLUTION"].alias(
            "AVG_SECONDS_BEFORE_RESOLUTION"
        ),
        coalesce(anomaly_by_wallet["ANOMALY_INVOLVEMENT_COUNT"], lit(0)).alias(
            "ANOMALY_INVOLVEMENT_COUNT"
        ),
        source_risk_profiles["ANOMALY_INVOLVEMENT_RATE"].alias(
            "ANOMALY_INVOLVEMENT_RATE"
        ),
        source_risk_profiles["TOP_DETECTOR_TYPE"].alias("TOP_DETECTOR_TYPE"),
        source_risk_profiles["RISK_SCORE"].alias("RISK_SCORE"),
        trade_rollup["FIRST_SEEN"].alias("FIRST_SEEN"),
        trade_rollup["LAST_SEEN"].alias("LAST_SEEN"),
        current_timestamp().alias("UPDATED_AT"),
    )


def build_market_concentration_daily_df(
    session: Session,
    fact_market_daily_name: str,
    market_top_traders_daily_table: str,
):
    query = f"""
        WITH ranked AS (
            SELECT
                trade_date,
                market_id,
                condition_id,
                market_question,
                market_label,
                proxy_wallet,
                total_volume_usdc,
                ROW_NUMBER() OVER (
                    PARTITION BY trade_date, condition_id
                    ORDER BY total_volume_usdc DESC, proxy_wallet
                ) AS trader_rank
            FROM {market_top_traders_daily_table}
        ),
        concentration AS (
            SELECT
                trade_date,
                market_id,
                condition_id,
                MAX(market_question) AS market_question,
                MAX(market_label) AS market_label,
                SUM(CASE WHEN trader_rank = 1 THEN total_volume_usdc ELSE 0 END) AS top_1_volume_usdc,
                SUM(CASE WHEN trader_rank <= 5 THEN total_volume_usdc ELSE 0 END) AS top_5_volume_usdc,
                COUNT(*) AS ranked_traders
            FROM ranked
            GROUP BY trade_date, market_id, condition_id
        )
        SELECT
            m.stat_date AS trade_date,
            m.market_id,
            m.condition_id,
            m.market_question,
            m.market_label,
            m.active,
            m.closed,
            m.trade_count,
            m.unique_wallets AS unique_traders,
            m.total_usdc_volume,
            ROUND(COALESCE(c.top_1_volume_usdc, 0), 2) AS top_1_volume_usdc,
            ROUND(COALESCE(c.top_5_volume_usdc, 0), 2) AS top_5_volume_usdc,
            ROUND(
                COALESCE(c.top_1_volume_usdc, 0) / NULLIF(m.total_usdc_volume, 0),
                6
            ) AS top_1_share_volume,
            ROUND(
                COALESCE(c.top_5_volume_usdc, 0) / NULLIF(m.total_usdc_volume, 0),
                6
            ) AS top_5_share_volume,
            ROUND(m.total_usdc_volume / NULLIF(m.unique_wallets, 0), 2) AS volume_per_trader_usdc,
            COALESCE(c.ranked_traders, 0) AS ranked_traders,
            m.first_trade_ts,
            m.last_trade_ts,
            CURRENT_TIMESTAMP() AS LAST_REFRESHED_AT
        FROM {fact_market_daily_name} m
        LEFT JOIN concentration c
          ON m.stat_date = c.trade_date
         AND m.condition_id = c.condition_id
    """
    return session.sql(query)


def build_platform_daily_summary_df(
    session: Session,
    fact_market_daily_name: str,
    fact_wallet_daily_name: str,
):
    query = f"""
        WITH market_rollup AS (
            SELECT
                stat_date AS trade_date,
                ROUND(SUM(total_usdc_volume), 2) AS total_volume_usdc,
                SUM(trade_count) AS total_trades,
                COUNT(*) AS markets_with_trades
            FROM {fact_market_daily_name}
            GROUP BY stat_date
        ),
        wallet_rollup AS (
            SELECT
                stat_date AS trade_date,
                COUNT(*) AS active_traders
            FROM {fact_wallet_daily_name}
            GROUP BY stat_date
        )
        SELECT
            m.trade_date,
            m.total_volume_usdc,
            m.total_trades,
            m.markets_with_trades,
            COALESCE(w.active_traders, 0) AS active_traders,
            ROUND(m.total_volume_usdc / NULLIF(w.active_traders, 0), 2) AS avg_volume_per_active_trader,
            ROUND(m.total_trades / NULLIF(w.active_traders, 0), 2) AS avg_trades_per_active_trader,
            CURRENT_TIMESTAMP() AS LAST_REFRESHED_AT
        FROM market_rollup m
        LEFT JOIN wallet_rollup w
          ON m.trade_date = w.trade_date
    """
    return session.sql(query)


def build_trader_segment_snapshot_df(session: Session, dim_traders_name: str):
    query = f"""
        WITH ranked AS (
            SELECT
                d.*,
                NTILE(10) OVER (ORDER BY total_volume DESC NULLS LAST) AS volume_decile,
                NTILE(10) OVER (ORDER BY total_trades DESC NULLS LAST) AS trade_count_decile,
                NTILE(10) OVER (ORDER BY distinct_markets DESC NULLS LAST) AS diversification_decile,
                NTILE(10) OVER (ORDER BY avg_trade_size DESC NULLS LAST) AS avg_trade_size_decile
            FROM {dim_traders_name} d
        )
        SELECT
            proxy_wallet,
            user_name,
            x_username,
            leaderboard_rank,
            total_trades,
            total_volume,
            distinct_markets,
            avg_trade_size,
            max_single_trade,
            anomaly_involvement_count,
            first_seen,
            last_seen,
            volume_decile,
            trade_count_decile,
            diversification_decile,
            avg_trade_size_decile,
            CASE
                WHEN volume_decile = 1 THEN 'Top decile by total volume'
                WHEN volume_decile <= 3 THEN 'Upper volume tier'
                WHEN volume_decile <= 7 THEN 'Middle volume tier'
                ELSE 'Lower volume tier'
            END AS volume_segment,
            CASE
                WHEN trade_count_decile = 1 THEN 'Top decile by trade count'
                WHEN trade_count_decile <= 3 THEN 'High activity'
                WHEN trade_count_decile <= 7 THEN 'Medium activity'
                ELSE 'Low activity'
            END AS activity_segment,
            CASE
                WHEN diversification_decile = 1 THEN 'Top decile by diversification'
                WHEN diversification_decile <= 3 THEN 'Highly diversified'
                WHEN diversification_decile <= 7 THEN 'Moderately diversified'
                ELSE 'Concentrated'
            END AS diversification_segment,
            CASE
                WHEN avg_trade_size_decile = 1 THEN 'Top decile by average trade size'
                WHEN avg_trade_size_decile <= 3 THEN 'Large-size trader'
                WHEN avg_trade_size_decile <= 7 THEN 'Mid-size trader'
                ELSE 'Small-size trader'
            END AS avg_trade_size_segment,
            CURRENT_TIMESTAMP() AS LAST_REFRESHED_AT
        FROM ranked
    """
    return session.sql(query)


def build_trader_cohort_monthly_df(
    session: Session,
    fact_wallet_daily_name: str,
    dim_traders_name: str,
):
    query = f"""
        WITH cohort_base AS (
            SELECT
                proxy_wallet,
                DATE_TRUNC('MONTH', first_seen) AS cohort_month
            FROM {dim_traders_name}
            WHERE first_seen IS NOT NULL
        ),
        wallet_monthly AS (
            SELECT
                proxy_wallet,
                DATE_TRUNC('MONTH', stat_date) AS activity_month,
                ROUND(SUM(total_usdc_volume), 2) AS total_volume_usdc,
                SUM(trade_count) AS total_trades,
                COUNT(*) AS active_days,
                MAX(last_refreshed_at) AS source_last_refreshed_at
            FROM {fact_wallet_daily_name}
            GROUP BY proxy_wallet, DATE_TRUNC('MONTH', stat_date)
        ),
        joined AS (
            SELECT
                c.cohort_month,
                w.activity_month,
                DATEDIFF('MONTH', c.cohort_month, w.activity_month) AS months_since_first_seen,
                w.proxy_wallet,
                w.total_volume_usdc,
                w.total_trades,
                w.active_days,
                w.source_last_refreshed_at
            FROM wallet_monthly w
            INNER JOIN cohort_base c
                ON w.proxy_wallet = c.proxy_wallet
            WHERE w.activity_month >= c.cohort_month
        )
        SELECT
            cohort_month,
            activity_month,
            months_since_first_seen,
            COUNT(*) AS active_traders,
            ROUND(SUM(total_volume_usdc), 2) AS total_volume_usdc,
            SUM(total_trades) AS total_trades,
            ROUND(AVG(total_volume_usdc), 2) AS avg_volume_per_active_trader,
            ROUND(AVG(total_trades), 2) AS avg_trades_per_active_trader,
            ROUND(AVG(active_days), 2) AS avg_active_days,
            MAX(source_last_refreshed_at) AS SOURCE_LAST_REFRESHED_AT,
            CURRENT_TIMESTAMP() AS LAST_REFRESHED_AT
        FROM joined
        GROUP BY cohort_month, activity_month, months_since_first_seen
    """
    return session.sql(query)


def build_market_theme_daily_df(
    session: Session,
    fact_user_activity_trades_name: str,
    dim_markets_name: str,
):
    query = f"""
        SELECT
            f.trade_date,
            COALESCE(
                d.market_theme,
                d.market_group_title,
                d.event_title,
                f.market_label,
                f.market_question,
                f.condition_id
            ) AS market_theme,
            COALESCE(d.market_theme_source, 'FACT_FALLBACK') AS market_theme_source,
            MAX(d.market_group_title) AS market_group_title,
            MAX(d.event_title) AS event_title,
            MAX(d.event_slug) AS event_slug,
            COUNT(DISTINCT f.condition_id) AS distinct_markets,
            COUNT(*) AS trade_count,
            COUNT(DISTINCT f.proxy_wallet) AS unique_traders,
            ROUND(SUM(f.usdc_volume), 2) AS total_volume_usdc,
            ROUND(AVG(f.usdc_volume), 2) AS avg_trade_size,
            ROUND(MAX(f.usdc_volume), 2) AS max_trade_size,
            MIN(f.trade_ts) AS first_trade_ts,
            MAX(f.trade_ts) AS last_trade_ts,
            CURRENT_TIMESTAMP() AS last_refreshed_at
        FROM {fact_user_activity_trades_name} f
        LEFT JOIN {dim_markets_name} d
          ON f.condition_id = d.condition_id
        GROUP BY
            f.trade_date,
            COALESCE(
                d.market_theme,
                d.market_group_title,
                d.event_title,
                f.market_label,
                f.market_question,
                f.condition_id
            ),
            COALESCE(d.market_theme_source, 'FACT_FALLBACK')
    """
    return session.sql(query)


def repair_incremental_target_schemas(
    session: Session,
    config: AnalyticsConfig,
    analytics_schema: str,
    dim_markets_name: str,
    fact_user_activity_trades_name: str,
    dim_leaderboard_users_name: str,
    fact_anomalies_name: str,
    fact_anomaly_attribution_name: str,
    fact_market_daily_name: str,
    fact_wallet_daily_name: str,
    dim_traders_name: str,
) -> None:
    repairs: list[str] = []

    if not table_has_exact_columns(
        session, fact_market_daily_name, FACT_MARKET_DAILY_COLUMNS
    ):
        fact_market_daily = build_market_daily_df(
            session.table(fact_user_activity_trades_name),
            session.table(fact_anomalies_name),
            session.table(dim_markets_name),
        )
        write_table(fact_market_daily, fact_market_daily_name)
        repairs.append(fact_market_daily_name)

    if not table_has_exact_columns(
        session, fact_wallet_daily_name, FACT_WALLET_DAILY_COLUMNS
    ):
        fact_wallet_daily = build_wallet_daily_df(
            session.table(fact_user_activity_trades_name),
            session.table(fact_anomaly_attribution_name),
        )
        write_table(fact_wallet_daily, fact_wallet_daily_name)
        repairs.append(fact_wallet_daily_name)

    if not table_has_exact_columns(session, dim_traders_name, DIM_TRADERS_COLUMNS):
        dim_traders = build_trader_profiles_df(
            session.table(fact_user_activity_trades_name),
            session.table(dim_leaderboard_users_name),
            session.table(fact_anomaly_attribution_name),
            session.table(config.dog_trader_risk_profiles_table),
        )
        write_table(dim_traders, dim_traders_name)
        repairs.append(dim_traders_name)

    if not table_has_exact_columns(
        session,
        config.tracked_market_volume_daily_table,
        TRACKED_MARKET_VOLUME_DAILY_COLUMNS,
    ):
        tracked_market_volume_daily = session.table(fact_market_daily_name).select(
            col("STAT_DATE").alias("TRADE_DATE"),
            col("MARKET_ID"),
            col("CONDITION_ID"),
            col("MARKET_QUESTION"),
            col("MARKET_LABEL"),
            col("ACTIVE"),
            col("CLOSED"),
            col("TRADE_COUNT"),
            col("UNIQUE_WALLETS").alias("UNIQUE_TRADERS"),
            col("TOTAL_USDC_VOLUME").alias("TOTAL_VOLUME_USDC"),
            col("FIRST_TRADE_TS"),
            col("LAST_TRADE_TS"),
            col("LAST_REFRESHED_AT"),
        )
        write_table(tracked_market_volume_daily, config.tracked_market_volume_daily_table)
        repairs.append(config.tracked_market_volume_daily_table)

    if not table_has_exact_columns(
        session,
        config.market_top_traders_daily_table,
        MARKET_TOP_TRADERS_DAILY_COLUMNS,
    ):
        top_traders_daily = (
            session.table(fact_user_activity_trades_name)
            .group_by(
                "TRADE_DATE",
                "MARKET_ID",
                "CONDITION_ID",
                "MARKET_QUESTION",
                "MARKET_LABEL",
                "PROXY_WALLET",
            )
            .agg(
                round_(sum_("USDC_VOLUME"), 2).alias("TOTAL_VOLUME_USDC"),
                count(lit(1)).alias("TRADE_COUNT"),
                round_(avg("USDC_VOLUME"), 2).alias("AVERAGE_TRADE_USDC"),
                round_(max_("USDC_VOLUME"), 2).alias("LARGEST_TRADE_USDC"),
            )
            .with_column("LAST_REFRESHED_AT", current_timestamp())
        )
        write_table(top_traders_daily, config.market_top_traders_daily_table)
        repairs.append(config.market_top_traders_daily_table)

    if not table_has_exact_columns(
        session,
        config.market_top_traders_all_time_table,
        MARKET_TOP_TRADERS_ALL_TIME_COLUMNS,
    ):
        top_traders_all_time = (
            session.table(fact_user_activity_trades_name)
            .group_by(
                "MARKET_ID",
                "CONDITION_ID",
                "MARKET_QUESTION",
                "MARKET_LABEL",
                "PROXY_WALLET",
            )
            .agg(
                round_(sum_("USDC_VOLUME"), 2).alias("TOTAL_VOLUME_USDC"),
                count(lit(1)).alias("TRADE_COUNT"),
                round_(avg("USDC_VOLUME"), 2).alias("AVERAGE_TRADE_USDC"),
                round_(max_("USDC_VOLUME"), 2).alias("LARGEST_TRADE_USDC"),
            )
            .with_column("LAST_REFRESHED_AT", current_timestamp())
        )
        write_table(top_traders_all_time, config.market_top_traders_all_time_table)
        repairs.append(config.market_top_traders_all_time_table)

    if not table_has_exact_columns(
        session,
        config.market_theme_daily_table,
        MARKET_THEME_DAILY_COLUMNS,
    ):
        market_theme_daily = build_market_theme_daily_df(
            session,
            fact_user_activity_trades_name,
            dim_markets_name,
        )
        write_table(market_theme_daily, config.market_theme_daily_table)
        repairs.append(config.market_theme_daily_table)

    if repairs:
        print(
            "[build] Repaired legacy schemas for incremental targets:\n"
            + "\n".join(f"[build]   {table_name}" for table_name in repairs),
            flush=True,
        )


def main() -> None:
    args = parse_args()
    snowflake_config, config = load_config(args.config_path)
    session = Session.builder.configs(snowflake_config).create()

    try:
        print(f"[build] Requested build mode: {args.mode}", flush=True)
        print_source_inventory(session, config)
        analytics_schema = schema_name_for(config.tracked_market_volume_daily_table)
        session.sql(f"CREATE SCHEMA IF NOT EXISTS {analytics_schema}").collect()
        state_table_name = ensure_build_state_table(session, analytics_schema)

        dim_markets_name = table_name(analytics_schema, "DIM_MARKETS")
        bridge_market_assets_name = table_name(analytics_schema, "BRIDGE_MARKET_ASSETS")
        fact_user_activity_trades_name = table_name(
            analytics_schema, "FACT_USER_ACTIVITY_TRADES"
        )
        fact_data_api_trades_name = table_name(analytics_schema, "FACT_DATA_API_TRADES")
        fact_clob_trades_name = table_name(analytics_schema, "FACT_CLOB_TRADES")
        fact_book_snapshots_name = table_name(
            analytics_schema, "FACT_BOOK_SNAPSHOTS"
        )
        fact_leaderboard_snapshots_name = table_name(
            analytics_schema, "FACT_LEADERBOARD_USER_SNAPSHOTS"
        )
        dim_leaderboard_users_name = table_name(
            analytics_schema, "DIM_LEADERBOARD_USERS"
        )
        fact_market_daily_name = table_name(analytics_schema, "FACT_MARKET_DAILY")
        fact_wallet_daily_name = table_name(analytics_schema, "FACT_WALLET_DAILY")
        fact_anomalies_name = table_name(analytics_schema, "FACT_ANOMALIES")
        fact_anomaly_attribution_name = table_name(
            analytics_schema, "FACT_ANOMALY_ATTRIBUTION"
        )
        dim_traders_name = table_name(analytics_schema, "DIM_TRADERS")
        market_concentration_daily_name = table_name(
            analytics_schema, "MARKET_CONCENTRATION_DAILY"
        )
        platform_daily_summary_name = table_name(
            analytics_schema, "PLATFORM_DAILY_SUMMARY"
        )
        trader_segment_snapshot_name = table_name(
            analytics_schema, "TRADER_SEGMENT_SNAPSHOT"
        )
        trader_cohort_monthly_name = table_name(
            analytics_schema, "TRADER_COHORT_MONTHLY"
        )
        market_theme_daily_name = table_name(analytics_schema, "MARKET_THEME_DAILY")
        build_mode = args.mode
        final_build_watermark = last_coyote_watermark = get_last_coyote_watermark(
            session, state_table_name
        )

        dim_markets = build_latest_markets_df(session, config).with_column(
            "LAST_REFRESHED_AT", current_timestamp()
        )
        write_table(dim_markets, dim_markets_name)

        market_assets = build_market_assets_df(session, config, analytics_schema).with_column(
            "LAST_REFRESHED_AT", current_timestamp()
        )
        write_table(market_assets, bridge_market_assets_name)

        dim_markets_ref = session.table(dim_markets_name)
        market_assets_ref = session.table(bridge_market_assets_name)

        fact_data_api_trades = build_data_api_trades_df(
            session, config, dim_markets_ref
        ).with_column("LAST_REFRESHED_AT", current_timestamp())
        write_table(fact_data_api_trades, fact_data_api_trades_name)

        fact_clob_trades = build_clob_trades_df(
            session, config, dim_markets_ref, market_assets_ref
        ).with_column("LAST_REFRESHED_AT", current_timestamp())
        write_table(fact_clob_trades, fact_clob_trades_name)

        fact_book_snapshots = build_book_snapshots_df(
            session, config, dim_markets_ref, market_assets_ref
        ).with_column("LAST_REFRESHED_AT", current_timestamp())
        write_table(fact_book_snapshots, fact_book_snapshots_name)

        leaderboard_snapshots = build_leaderboard_snapshots_df(session, config).with_column(
            "LAST_REFRESHED_AT", current_timestamp()
        )
        write_table(leaderboard_snapshots, fact_leaderboard_snapshots_name)

        leaderboard_current = build_leaderboard_current_df(
            session.table(fact_leaderboard_snapshots_name)
        ).with_column("LAST_REFRESHED_AT", current_timestamp())
        write_table(leaderboard_current, dim_leaderboard_users_name)

        fact_anomalies = build_anomalies_df(
            session, config, dim_markets_ref, market_assets_ref
        ).with_column("LAST_REFRESHED_AT", current_timestamp())
        write_table(fact_anomalies, fact_anomalies_name)

        fact_anomaly_attribution = build_anomaly_attribution_df(
            session, config, dim_markets_ref
        ).with_column("LAST_REFRESHED_AT", current_timestamp())
        write_table(fact_anomaly_attribution, fact_anomaly_attribution_name)
        if (
            build_mode == "incremental"
            and last_coyote_watermark is None
            and table_exists(session, fact_user_activity_trades_name)
        ):
            last_coyote_watermark = get_existing_fact_watermark(
                session, fact_user_activity_trades_name
            )
            if last_coyote_watermark is not None:
                print(
                    "[build] No stored incremental watermark was found. "
                    "Bootstrapping from the existing FACT_USER_ACTIVITY_TRADES table.",
                    flush=True,
                )
                update_build_state(
                    session,
                    state_table_name,
                    last_coyote_watermark,
                    "bootstrap",
                )
                final_build_watermark = last_coyote_watermark
        if build_mode == "incremental" and table_exists(
            session, fact_user_activity_trades_name
        ):
            repair_incremental_target_schemas(
                session,
                config,
                analytics_schema,
                dim_markets_name,
                fact_user_activity_trades_name,
                dim_leaderboard_users_name,
                fact_anomalies_name,
                fact_anomaly_attribution_name,
                fact_market_daily_name,
                fact_wallet_daily_name,
                dim_traders_name,
            )
        can_incremental = (
            build_mode == "incremental"
            and last_coyote_watermark is not None
            and table_exists(session, fact_user_activity_trades_name)
            and table_has_exact_columns(
                session, fact_market_daily_name, FACT_MARKET_DAILY_COLUMNS
            )
            and table_has_exact_columns(
                session, fact_wallet_daily_name, FACT_WALLET_DAILY_COLUMNS
            )
            and table_has_exact_columns(
                session, dim_traders_name, DIM_TRADERS_COLUMNS
            )
            and table_has_exact_columns(
                session,
                config.market_top_traders_daily_table,
                MARKET_TOP_TRADERS_DAILY_COLUMNS,
            )
            and table_has_exact_columns(
                session,
                config.market_top_traders_all_time_table,
                MARKET_TOP_TRADERS_ALL_TIME_COLUMNS,
            )
            and table_has_exact_columns(
                session,
                config.market_theme_daily_table,
                MARKET_THEME_DAILY_COLUMNS,
            )
        )

        if not can_incremental:
            if build_mode == "incremental":
                print(
                    "[build] Incremental mode requested but prior analytics state was not "
                    "available. Falling back to a full rebuild.",
                    flush=True,
                )
            build_mode = "full"

            fact_user_activity_trades = build_user_activity_trades_df(
                session, config, session.table(dim_markets_name)
            ).with_column("LAST_REFRESHED_AT", current_timestamp())
            write_table(fact_user_activity_trades, fact_user_activity_trades_name)

            fact_market_daily = build_market_daily_df(
                session.table(fact_user_activity_trades_name),
                session.table(fact_anomalies_name),
                dim_markets_ref,
            )
            write_table(fact_market_daily, fact_market_daily_name)

            fact_wallet_daily = build_wallet_daily_df(
                session.table(fact_user_activity_trades_name),
                session.table(fact_anomaly_attribution_name),
            )
            write_table(fact_wallet_daily, fact_wallet_daily_name)

            source_risk_profiles = session.table(config.dog_trader_risk_profiles_table)
            dim_traders = build_trader_profiles_df(
                session.table(fact_user_activity_trades_name),
                session.table(dim_leaderboard_users_name),
                session.table(fact_anomaly_attribution_name),
                source_risk_profiles,
            )
            write_table(dim_traders, dim_traders_name)

            tracked_market_volume_daily = session.table(fact_market_daily_name).select(
                col("STAT_DATE").alias("TRADE_DATE"),
                col("MARKET_ID"),
                col("CONDITION_ID"),
                col("MARKET_QUESTION"),
                col("MARKET_LABEL"),
                col("ACTIVE"),
                col("CLOSED"),
                col("TRADE_COUNT"),
                col("UNIQUE_WALLETS").alias("UNIQUE_TRADERS"),
                col("TOTAL_USDC_VOLUME").alias("TOTAL_VOLUME_USDC"),
                col("FIRST_TRADE_TS"),
                col("LAST_TRADE_TS"),
                col("LAST_REFRESHED_AT"),
            )

            top_traders_daily = (
                session.table(fact_user_activity_trades_name)
                .group_by(
                    "TRADE_DATE",
                    "MARKET_ID",
                    "CONDITION_ID",
                    "MARKET_QUESTION",
                    "MARKET_LABEL",
                    "PROXY_WALLET",
                )
                .agg(
                    round_(sum_("USDC_VOLUME"), 2).alias("TOTAL_VOLUME_USDC"),
                    count(lit(1)).alias("TRADE_COUNT"),
                    round_(avg("USDC_VOLUME"), 2).alias("AVERAGE_TRADE_USDC"),
                    round_(max_("USDC_VOLUME"), 2).alias("LARGEST_TRADE_USDC"),
                )
                .with_column("LAST_REFRESHED_AT", current_timestamp())
            )

            top_traders_all_time = (
                session.table(fact_user_activity_trades_name)
                .group_by(
                    "MARKET_ID",
                    "CONDITION_ID",
                    "MARKET_QUESTION",
                    "MARKET_LABEL",
                    "PROXY_WALLET",
                )
                .agg(
                    round_(sum_("USDC_VOLUME"), 2).alias("TOTAL_VOLUME_USDC"),
                    count(lit(1)).alias("TRADE_COUNT"),
                    round_(avg("USDC_VOLUME"), 2).alias("AVERAGE_TRADE_USDC"),
                    round_(max_("USDC_VOLUME"), 2).alias("LARGEST_TRADE_USDC"),
                )
                .with_column("LAST_REFRESHED_AT", current_timestamp())
            )
        else:
            print(
                f"[build] Running incremental rebuild from COYOTE _LOADED_AT >= "
                f"{last_coyote_watermark}",
                flush=True,
            )
            full_trade_fact_row_count = (
                fetch_table_row_count_metadata(session, fact_user_activity_trades_name)
                or 0
            )
            delta_fact_user_activity_trades = build_user_activity_trades_df(
                session,
                config,
                session.table(dim_markets_name),
                loaded_at_or_after=last_coyote_watermark,
            ).with_column("LAST_REFRESHED_AT", current_timestamp())

            delta_stats = (
                delta_fact_user_activity_trades.agg(
                    count(lit(1)).alias("ROW_COUNT"),
                    max_("SOURCE_LOADED_AT").alias("MAX_SOURCE_LOADED_AT"),
                ).collect()[0]
            )
            delta_row_count = int(delta_stats["ROW_COUNT"])
            latest_source_loaded_at = delta_stats["MAX_SOURCE_LOADED_AT"]

            if delta_row_count == 0:
                print(
                    "[build] No new COYOTE trade rows were detected. Skipping "
                    "COYOTE-derived incremental refresh steps.",
                    flush=True,
                )
                print(
                    f"[build] Delta size relative to current FACT_USER_ACTIVITY_TRADES: "
                    f"0 / {full_trade_fact_row_count:,} = "
                    f"{format_percent(0, full_trade_fact_row_count)}",
                    flush=True,
                )
                tracked_market_volume_daily = session.table(
                    config.tracked_market_volume_daily_table
                )
                top_traders_daily = session.table(config.market_top_traders_daily_table)
                top_traders_all_time = session.table(
                    config.market_top_traders_all_time_table
                )
                market_theme_daily = session.table(config.market_theme_daily_table)
                final_build_watermark = last_coyote_watermark
            else:
                print(
                    f"[build] Incremental delta contains {delta_row_count:,} trade rows.",
                    flush=True,
                )
                print(
                    f"[build] Delta size relative to current FACT_USER_ACTIVITY_TRADES: "
                    f"{delta_row_count:,} / {full_trade_fact_row_count:,} = "
                    f"{format_percent(delta_row_count, full_trade_fact_row_count)}",
                    flush=True,
                )
                merge_rows_by_keys(
                    session,
                    analytics_schema,
                    fact_user_activity_trades_name,
                    delta_fact_user_activity_trades,
                    [
                        "TRANSACTION_HASH",
                        "TRADE_TS",
                        "CONDITION_ID",
                        "OUTCOME_INDEX",
                        "SIZE",
                        "PROXY_WALLET",
                    ],
                )

                impacted_dates = delta_fact_user_activity_trades.select("TRADE_DATE").distinct()
                impacted_conditions = delta_fact_user_activity_trades.select(
                    "CONDITION_ID"
                ).distinct()
                impacted_wallets = delta_fact_user_activity_trades.select(
                    "PROXY_WALLET"
                ).distinct()
                impacted_counts = (
                    impacted_dates.agg(count(lit(1)).alias("COUNT")).collect()[0]["COUNT"],
                    impacted_conditions.agg(count(lit(1)).alias("COUNT")).collect()[0]["COUNT"],
                    impacted_wallets.agg(count(lit(1)).alias("COUNT")).collect()[0]["COUNT"],
                )
                print(
                    "[build] Incremental impact scope: "
                    f"{int(impacted_counts[0]):,} trade dates, "
                    f"{int(impacted_counts[1]):,} conditions, "
                    f"{int(impacted_counts[2]):,} wallets",
                    flush=True,
                )

                fact_user_activity_trades_ref = session.table(fact_user_activity_trades_name)
                fact_anomalies_ref = session.table(fact_anomalies_name)
                fact_anomaly_attribution_ref = session.table(
                    fact_anomaly_attribution_name
                )

                market_daily_delta = build_market_daily_df(
                    filter_df_by_keys(
                        fact_user_activity_trades_ref, impacted_dates, "TRADE_DATE"
                    ),
                    filter_df_by_keys(
                        fact_anomalies_ref, impacted_dates, "DETECTED_DATE", "TRADE_DATE"
                    ),
                    dim_markets_ref,
                )
                replace_rows_by_keys(
                    session,
                    analytics_schema,
                    fact_market_daily_name,
                    market_daily_delta,
                    ["STAT_DATE"],
                )

                wallet_daily_delta = build_wallet_daily_df(
                    filter_df_by_keys(
                        fact_user_activity_trades_ref, impacted_dates, "TRADE_DATE"
                    ),
                    filter_df_by_keys(
                        fact_anomaly_attribution_ref, impacted_dates, "TRADE_DATE"
                    ),
                )
                replace_rows_by_keys(
                    session,
                    analytics_schema,
                    fact_wallet_daily_name,
                    wallet_daily_delta,
                    ["STAT_DATE"],
                )

                source_risk_profiles = filter_df_by_keys(
                    session.table(config.dog_trader_risk_profiles_table),
                    impacted_wallets,
                    "PROXY_WALLET",
                )
                dim_traders_delta = build_trader_profiles_df(
                    filter_df_by_keys(
                        fact_user_activity_trades_ref, impacted_wallets, "PROXY_WALLET"
                    ),
                    session.table(dim_leaderboard_users_name),
                    filter_df_by_keys(
                        fact_anomaly_attribution_ref, impacted_wallets, "PROXY_WALLET"
                    ),
                    source_risk_profiles,
                )
                replace_rows_by_keys(
                    session,
                    analytics_schema,
                    dim_traders_name,
                    dim_traders_delta,
                    ["PROXY_WALLET"],
                )

                tracked_market_volume_daily = (
                    filter_df_by_keys(
                        session.table(fact_market_daily_name), impacted_dates, "STAT_DATE", "TRADE_DATE"
                    ).select(
                        col("STAT_DATE").alias("TRADE_DATE"),
                        col("MARKET_ID"),
                        col("CONDITION_ID"),
                        col("MARKET_QUESTION"),
                        col("MARKET_LABEL"),
                        col("ACTIVE"),
                        col("CLOSED"),
                        col("TRADE_COUNT"),
                        col("UNIQUE_WALLETS").alias("UNIQUE_TRADERS"),
                        col("TOTAL_USDC_VOLUME").alias("TOTAL_VOLUME_USDC"),
                        col("FIRST_TRADE_TS"),
                        col("LAST_TRADE_TS"),
                        col("LAST_REFRESHED_AT"),
                    )
                )
                replace_rows_by_keys(
                    session,
                    analytics_schema,
                    config.tracked_market_volume_daily_table,
                    tracked_market_volume_daily,
                    ["TRADE_DATE"],
                )

                top_traders_daily = (
                    filter_df_by_keys(
                        fact_user_activity_trades_ref, impacted_dates, "TRADE_DATE"
                    )
                    .group_by(
                        "TRADE_DATE",
                        "MARKET_ID",
                        "CONDITION_ID",
                        "MARKET_QUESTION",
                        "MARKET_LABEL",
                        "PROXY_WALLET",
                    )
                    .agg(
                        round_(sum_("USDC_VOLUME"), 2).alias("TOTAL_VOLUME_USDC"),
                        count(lit(1)).alias("TRADE_COUNT"),
                        round_(avg("USDC_VOLUME"), 2).alias("AVERAGE_TRADE_USDC"),
                        round_(max_("USDC_VOLUME"), 2).alias("LARGEST_TRADE_USDC"),
                    )
                    .with_column("LAST_REFRESHED_AT", current_timestamp())
                )
                replace_rows_by_keys(
                    session,
                    analytics_schema,
                    config.market_top_traders_daily_table,
                    top_traders_daily,
                    ["TRADE_DATE"],
                )

                top_traders_all_time = (
                    filter_df_by_keys(
                        fact_user_activity_trades_ref,
                        impacted_conditions,
                        "CONDITION_ID",
                    )
                    .group_by(
                        "MARKET_ID",
                        "CONDITION_ID",
                        "MARKET_QUESTION",
                        "MARKET_LABEL",
                        "PROXY_WALLET",
                    )
                    .agg(
                        round_(sum_("USDC_VOLUME"), 2).alias("TOTAL_VOLUME_USDC"),
                        count(lit(1)).alias("TRADE_COUNT"),
                        round_(avg("USDC_VOLUME"), 2).alias("AVERAGE_TRADE_USDC"),
                        round_(max_("USDC_VOLUME"), 2).alias("LARGEST_TRADE_USDC"),
                    )
                    .with_column("LAST_REFRESHED_AT", current_timestamp())
                )
                replace_rows_by_keys(
                    session,
                    analytics_schema,
                    config.market_top_traders_all_time_table,
                    top_traders_all_time,
                    ["CONDITION_ID"],
                )

                theme_fact_source = temporary_table_name(
                    analytics_schema, "FACT_THEME_SOURCE"
                )
                write_temp_table(
                    filter_df_by_keys(
                        fact_user_activity_trades_ref, impacted_dates, "TRADE_DATE"
                    ),
                    theme_fact_source,
                )
                market_theme_daily = build_market_theme_daily_df(
                    session,
                    theme_fact_source,
                    dim_markets_name,
                )
                replace_rows_by_keys(
                    session,
                    analytics_schema,
                    config.market_theme_daily_table,
                    market_theme_daily,
                    ["TRADE_DATE"],
                )

                final_build_watermark = latest_source_loaded_at

        highest_volume_markets = session.table(dim_markets_name).select(
            col("MARKET_ID"),
            col("CONDITION_ID"),
            col("MARKET_QUESTION"),
            col("MARKET_LABEL"),
            col("ACTIVE"),
            col("CLOSED"),
            col("END_DATE"),
            round_(col("LISTED_VOLUME_USDC"), 2).alias("TOTAL_VOLUME_USDC"),
            col("LAST_REFRESHED_AT"),
        )
        market_concentration_daily = build_market_concentration_daily_df(
            session,
            fact_market_daily_name,
            config.market_top_traders_daily_table,
        )
        platform_daily_summary = build_platform_daily_summary_df(
            session,
            fact_market_daily_name,
            fact_wallet_daily_name,
        )
        trader_segment_snapshot = build_trader_segment_snapshot_df(
            session, dim_traders_name
        )
        trader_cohort_monthly = build_trader_cohort_monthly_df(
            session,
            fact_wallet_daily_name,
            dim_traders_name,
        )
        if build_mode == "full":
            market_theme_daily = build_market_theme_daily_df(
                session,
                fact_user_activity_trades_name,
                dim_markets_name,
            )

        write_table(highest_volume_markets, config.highest_volume_markets_table)
        write_table(market_concentration_daily, config.market_concentration_daily_table)
        write_table(platform_daily_summary, config.platform_daily_summary_table)
        write_table(trader_segment_snapshot, config.trader_segment_snapshot_table)
        write_table(trader_cohort_monthly, config.trader_cohort_monthly_table)
        if build_mode == "full":
            write_table(market_theme_daily, config.market_theme_daily_table)

        if build_mode == "full":
            write_table(
                tracked_market_volume_daily, config.tracked_market_volume_daily_table
            )
            write_table(top_traders_daily, config.market_top_traders_daily_table)
            write_table(top_traders_all_time, config.market_top_traders_all_time_table)
            full_watermark_row = (
                session.table(fact_user_activity_trades_name)
                .agg(max_("SOURCE_LOADED_AT").alias("MAX_SOURCE_LOADED_AT"))
                .collect()[0]
            )
            final_build_watermark = full_watermark_row["MAX_SOURCE_LOADED_AT"]

        update_build_state(
            session,
            state_table_name,
            final_build_watermark,
            build_mode,
        )

        print(
            "Built analytics layer v2:\n"
            f"  - {dim_markets_name}\n"
            f"  - {fact_user_activity_trades_name}\n"
            f"  - {bridge_market_assets_name}\n"
            f"  - {fact_data_api_trades_name}\n"
            f"  - {fact_clob_trades_name}\n"
            f"  - {fact_book_snapshots_name}\n"
            f"  - {fact_leaderboard_snapshots_name}\n"
            f"  - {dim_leaderboard_users_name}\n"
            f"  - {fact_market_daily_name}\n"
            f"  - {fact_wallet_daily_name}\n"
            f"  - {fact_anomalies_name}\n"
            f"  - {fact_anomaly_attribution_name}\n"
            f"  - {dim_traders_name}\n"
            f"  - {config.tracked_market_volume_daily_table}\n"
            f"  - {config.highest_volume_markets_table}\n"
            f"  - {config.market_top_traders_daily_table}\n"
            f"  - {config.market_top_traders_all_time_table}\n"
            f"  - {config.market_concentration_daily_table}\n"
            f"  - {config.platform_daily_summary_table}\n"
            f"  - {config.trader_segment_snapshot_table}\n"
            f"  - {config.trader_cohort_monthly_table}\n"
            f"  - {config.market_theme_daily_table}"
        )
    finally:
        session.close()


if __name__ == "__main__":
    main()
