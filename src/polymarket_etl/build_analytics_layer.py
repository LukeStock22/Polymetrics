from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path
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


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build the warehouse analytics layer used by the Streamlit app."
    )
    parser.add_argument(
        "--config-path",
        default=".streamlit/secrets.toml",
        help="Path to a TOML file with [snowflake] and [app] sections.",
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


def write_table(df, table_name_str: str) -> None:
    df.write.mode("overwrite").save_as_table(table_name_str, table_type="transient")


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


def build_user_activity_trades_df(session: Session, config: AnalyticsConfig, dim_markets):
    activity = session.table(config.user_activity_table)
    trade_activity = activity.filter(upper(col("TYPE")) == lit("TRADE"))
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
        grouped_stats["MARKET_ID"],
        grouped_stats["CONDITION_ID"],
        grouped_stats["MARKET_QUESTION"],
        grouped_stats["MARKET_LABEL"],
        grouped_stats["ACTIVE"],
        grouped_stats["CLOSED"],
        grouped_stats["TRADE_COUNT"],
        grouped_stats["BUY_COUNT"],
        grouped_stats["SELL_COUNT"],
        grouped_stats["TOTAL_VOLUME"],
        grouped_stats["TOTAL_USDC_VOLUME"],
        grouped_stats["UNIQUE_WALLETS"],
        grouped_stats["AVG_TRADE_SIZE"],
        grouped_stats["MAX_TRADE_SIZE"],
        grouped_stats["VWAP"],
        grouped_stats["FIRST_TRADE_TS"],
        grouped_stats["LAST_TRADE_TS"],
        open_prices["PRICE_OPEN"],
        close_prices["PRICE_CLOSE"],
        grouped_stats["PRICE_HIGH"],
        grouped_stats["PRICE_LOW"],
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
        grouped["PROXY_WALLET"],
        grouped["TRADE_DATE"].alias("STAT_DATE"),
        grouped["TRADE_COUNT"],
        grouped["DISTINCT_MARKETS"],
        grouped["TOTAL_VOLUME"],
        grouped["TOTAL_USDC_VOLUME"],
        grouped["BUY_VOLUME"],
        grouped["SELL_VOLUME"],
        grouped["AVG_TRADE_SIZE"],
        grouped["MAX_TRADE_SIZE"],
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
        trade_rollup["PROXY_WALLET"],
        leaderboard_current["LEADERBOARD_RANK"],
        leaderboard_current["USER_NAME"],
        leaderboard_current["X_USERNAME"],
        leaderboard_current["VERIFIED_BADGE"],
        leaderboard_current["LEADERBOARD_VOLUME"],
        leaderboard_current["LEADERBOARD_PNL"],
        trade_rollup["TOTAL_TRADES"],
        trade_rollup["TOTAL_VOLUME"],
        trade_rollup["DISTINCT_MARKETS"],
        trade_rollup["AVG_TRADE_SIZE"],
        trade_rollup["MAX_SINGLE_TRADE"],
        source_risk_profiles["WIN_RATE"],
        source_risk_profiles["AVG_SECONDS_BEFORE_RESOLUTION"],
        coalesce(anomaly_by_wallet["ANOMALY_INVOLVEMENT_COUNT"], lit(0)).alias(
            "ANOMALY_INVOLVEMENT_COUNT"
        ),
        source_risk_profiles["ANOMALY_INVOLVEMENT_RATE"],
        source_risk_profiles["TOP_DETECTOR_TYPE"],
        source_risk_profiles["RISK_SCORE"],
        trade_rollup["FIRST_SEEN"],
        trade_rollup["LAST_SEEN"],
        current_timestamp().alias("UPDATED_AT"),
    )


def main() -> None:
    args = parse_args()
    snowflake_config, config = load_config(args.config_path)
    session = Session.builder.configs(snowflake_config).create()

    try:
        analytics_schema = schema_name_for(config.tracked_market_volume_daily_table)
        session.sql(f"CREATE SCHEMA IF NOT EXISTS {analytics_schema}").collect()

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

        dim_markets = build_latest_markets_df(session, config).with_column(
            "LAST_REFRESHED_AT", current_timestamp()
        )
        write_table(dim_markets, dim_markets_name)

        fact_user_activity_trades = build_user_activity_trades_df(
            session, config, session.table(dim_markets_name)
        ).with_column("LAST_REFRESHED_AT", current_timestamp())
        write_table(fact_user_activity_trades, fact_user_activity_trades_name)

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

        write_table(
            tracked_market_volume_daily, config.tracked_market_volume_daily_table
        )
        write_table(highest_volume_markets, config.highest_volume_markets_table)
        write_table(top_traders_daily, config.market_top_traders_daily_table)
        write_table(top_traders_all_time, config.market_top_traders_all_time_table)

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
            f"  - {config.market_top_traders_all_time_table}"
        )
    finally:
        session.close()


if __name__ == "__main__":
    main()
