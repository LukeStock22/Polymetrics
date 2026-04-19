from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path
import re

import altair as alt
import pandas as pd
import streamlit as st
from snowflake.snowpark import Session


IDENTIFIER_PATTERN = re.compile(r'^[A-Za-z0-9_."$]+$')
TIMEZONE_PATTERN = re.compile(r"^[A-Za-z0-9_/\-+]+$")

QUESTION_TRADE_HISTORY = "Market Trade History (Order Book)"
QUESTION_TRACKED_TODAY = "Most Popular Markets Today"
QUESTION_MARKETS_UNIQUE = "Markets With The Most Unique Traders Today"
QUESTION_MARKETS_AVG_BET = "Markets With The Largest Average Bet Today"
QUESTION_WHALE_DOMINATED = "Whale-Dominated Markets Today"
QUESTION_HIGHEST_VOLUME = "Highest Volume Markets"
QUESTION_LARGEST_BETS = "Largest Bets Placed Today"
QUESTION_TOP_TRADERS_TODAY = "Top Traders Today"
QUESTION_MOST_ACTIVE_TRADERS = "Most Active Traders Today"
QUESTION_BIGGEST_TRADERS = "Who Are The Biggest Traders Overall?"
QUESTION_TRADERS_AVG_SIZE = "Largest Traders By Average Trade Size"
QUESTION_MOST_DIVERSIFIED = "Most Diversified Traders"
QUESTION_DAILY_VOLUME = "How Much Money Was Traded Each Day?"
QUESTION_BIG_VS_SMALL = "What Separates Big Traders From Small Traders?"
QUESTION_TRADER_PROFILE = "Trader Profile Lookup"
QUESTION_TRADER_COHORTS = "Wallet Lifecycle / Trader Cohorts"
QUESTION_THEME_VOLUME = "Top Bet Themes Today"
QUESTION_THEME_UNIQUE = "Bet Themes With Most Unique Traders Today"
QUESTION_THEME_AVG_BET = "Bet Themes With Largest Average Bet Today"
QUESTION_THEME_ACTIVITY = "Most Active Bet Themes Today"

DAILY_MARKET_QUESTIONS = {
    QUESTION_TRACKED_TODAY,
    QUESTION_MARKETS_UNIQUE,
    QUESTION_MARKETS_AVG_BET,
    QUESTION_WHALE_DOMINATED,
}
QUESTIONS_WITH_DATE = DAILY_MARKET_QUESTIONS | {
    QUESTION_LARGEST_BETS,
    QUESTION_TOP_TRADERS_TODAY,
    QUESTION_MOST_ACTIVE_TRADERS,
    QUESTION_THEME_VOLUME,
    QUESTION_THEME_UNIQUE,
    QUESTION_THEME_AVG_BET,
    QUESTION_THEME_ACTIVITY,
}
QUESTIONS_WITH_MARKET_LIMIT = {
    QUESTION_TRADE_HISTORY,
    QUESTION_TRACKED_TODAY,
    QUESTION_MARKETS_UNIQUE,
    QUESTION_MARKETS_AVG_BET,
    QUESTION_WHALE_DOMINATED,
    QUESTION_HIGHEST_VOLUME,
    QUESTION_LARGEST_BETS,
    QUESTION_DAILY_VOLUME,
    QUESTION_TRADER_PROFILE,
    QUESTION_THEME_VOLUME,
    QUESTION_THEME_UNIQUE,
    QUESTION_THEME_AVG_BET,
    QUESTION_THEME_ACTIVITY,
}
QUESTIONS_WITH_TRADER_LIMIT = {
    QUESTION_TRACKED_TODAY,
    QUESTION_MARKETS_UNIQUE,
    QUESTION_MARKETS_AVG_BET,
    QUESTION_WHALE_DOMINATED,
    QUESTION_HIGHEST_VOLUME,
    QUESTION_LARGEST_BETS,
    QUESTION_DAILY_VOLUME,
    QUESTION_TOP_TRADERS_TODAY,
    QUESTION_MOST_ACTIVE_TRADERS,
    QUESTION_BIGGEST_TRADERS,
    QUESTION_TRADERS_AVG_SIZE,
    QUESTION_MOST_DIVERSIFIED,
}
QUESTIONS_WITH_HISTORY_DAYS = {
    QUESTION_TOP_TRADERS_TODAY,
    QUESTION_MOST_ACTIVE_TRADERS,
    QUESTION_BIGGEST_TRADERS,
    QUESTION_TRADERS_AVG_SIZE,
    QUESTION_MOST_DIVERSIFIED,
    QUESTION_DAILY_VOLUME,
    QUESTION_TRADER_PROFILE,
    QUESTION_TRADER_COHORTS,
}
QUESTIONS_WITH_RECENT_TRADES = {
    QUESTION_TRADE_HISTORY,
    QUESTION_TOP_TRADERS_TODAY,
    QUESTION_MOST_ACTIVE_TRADERS,
    QUESTION_BIGGEST_TRADERS,
    QUESTION_TRADERS_AVG_SIZE,
    QUESTION_MOST_DIVERSIFIED,
    QUESTION_TRADER_PROFILE,
}
QUESTIONS_WITH_MIN_TOTAL_TRADES = {
    QUESTION_TRADERS_AVG_SIZE,
    QUESTION_BIG_VS_SMALL,
}


@dataclass(frozen=True)
class AppConfig:
    user_activity_table: str
    markets_table: str
    tracked_market_volume_daily_table: str
    highest_volume_markets_table: str
    market_top_traders_daily_table: str
    market_top_traders_all_time_table: str
    dim_markets_table: str
    fact_market_daily_table: str
    fact_wallet_daily_table: str
    dim_traders_table: str
    fact_user_activity_trades_table: str
    market_concentration_daily_table: str
    platform_daily_summary_table: str
    trader_segment_snapshot_table: str
    trader_cohort_monthly_table: str
    market_theme_daily_table: str
    timezone: str
    default_market_limit: int
    default_trader_limit: int


def validate_identifier(value: str, label: str) -> str:
    if not value or not IDENTIFIER_PATTERN.fullmatch(value):
        raise ValueError(
            f"{label} must be an unquoted Snowflake identifier such as "
            "PANTHER_DB.ANALYTICS.TRACKED_MARKET_VOLUME_DAILY."
        )
    return value


def validate_timezone(value: str) -> str:
    if not value or not TIMEZONE_PATTERN.fullmatch(value):
        raise ValueError(
            "Timezone must contain only letters, digits, /, _, -, or + characters."
        )
    return value


def sql_string_literal(value: str) -> str:
    return value.replace("'", "''")


def resolved_market_predicate(alias: str = "") -> str:
    prefix = f"{alias}." if alias else ""
    return " AND ".join(
        [
            f"{prefix}market_id IS NOT NULL",
            f"{prefix}condition_id IS NOT NULL",
            f"{prefix}market_label IS NOT NULL",
            f"{prefix}market_label <> ''",
            f"{prefix}market_label <> {prefix}condition_id",
        ]
    )


def load_app_config() -> AppConfig:
    app_settings = dict(st.secrets["app"]) if "app" in st.secrets else {}
    return AppConfig(
        user_activity_table=validate_identifier(
            app_settings.get(
                "user_activity_table",
                "COYOTE_DB.PUBLIC.CURATED_POLYMARKET_USER_ACTIVITY",
            ),
            "app.user_activity_table",
        ),
        markets_table=validate_identifier(
            app_settings.get("markets_table", "PANTHER_DB.CURATED.GAMMA_MARKETS"),
            "app.markets_table",
        ),
        tracked_market_volume_daily_table=validate_identifier(
            app_settings.get(
                "tracked_market_volume_daily_table",
                "PANTHER_DB.ANALYTICS.TRACKED_MARKET_VOLUME_DAILY",
            ),
            "app.tracked_market_volume_daily_table",
        ),
        highest_volume_markets_table=validate_identifier(
            app_settings.get(
                "highest_volume_markets_table",
                "PANTHER_DB.ANALYTICS.HIGHEST_VOLUME_MARKETS",
            ),
            "app.highest_volume_markets_table",
        ),
        market_top_traders_daily_table=validate_identifier(
            app_settings.get(
                "market_top_traders_daily_table",
                "PANTHER_DB.ANALYTICS.MARKET_TOP_TRADERS_DAILY",
            ),
            "app.market_top_traders_daily_table",
        ),
        market_top_traders_all_time_table=validate_identifier(
            app_settings.get(
                "market_top_traders_all_time_table",
                "PANTHER_DB.ANALYTICS.MARKET_TOP_TRADERS_ALL_TIME",
            ),
            "app.market_top_traders_all_time_table",
        ),
        dim_markets_table=validate_identifier(
            app_settings.get(
                "dim_markets_table",
                "PANTHER_DB.ANALYTICS.DIM_MARKETS",
            ),
            "app.dim_markets_table",
        ),
        fact_market_daily_table=validate_identifier(
            app_settings.get(
                "fact_market_daily_table",
                "PANTHER_DB.ANALYTICS.FACT_MARKET_DAILY",
            ),
            "app.fact_market_daily_table",
        ),
        fact_wallet_daily_table=validate_identifier(
            app_settings.get(
                "fact_wallet_daily_table",
                "PANTHER_DB.ANALYTICS.FACT_WALLET_DAILY",
            ),
            "app.fact_wallet_daily_table",
        ),
        dim_traders_table=validate_identifier(
            app_settings.get(
                "dim_traders_table",
                "PANTHER_DB.ANALYTICS.DIM_TRADERS",
            ),
            "app.dim_traders_table",
        ),
        fact_user_activity_trades_table=validate_identifier(
            app_settings.get(
                "fact_user_activity_trades_table",
                "PANTHER_DB.ANALYTICS.FACT_USER_ACTIVITY_TRADES",
            ),
            "app.fact_user_activity_trades_table",
        ),
        market_concentration_daily_table=validate_identifier(
            app_settings.get(
                "market_concentration_daily_table",
                "PANTHER_DB.ANALYTICS.MARKET_CONCENTRATION_DAILY",
            ),
            "app.market_concentration_daily_table",
        ),
        platform_daily_summary_table=validate_identifier(
            app_settings.get(
                "platform_daily_summary_table",
                "PANTHER_DB.ANALYTICS.PLATFORM_DAILY_SUMMARY",
            ),
            "app.platform_daily_summary_table",
        ),
        trader_segment_snapshot_table=validate_identifier(
            app_settings.get(
                "trader_segment_snapshot_table",
                "PANTHER_DB.ANALYTICS.TRADER_SEGMENT_SNAPSHOT",
            ),
            "app.trader_segment_snapshot_table",
        ),
        trader_cohort_monthly_table=validate_identifier(
            app_settings.get(
                "trader_cohort_monthly_table",
                "PANTHER_DB.ANALYTICS.TRADER_COHORT_MONTHLY",
            ),
            "app.trader_cohort_monthly_table",
        ),
        market_theme_daily_table=validate_identifier(
            app_settings.get(
                "market_theme_daily_table",
                "PANTHER_DB.ANALYTICS.MARKET_THEME_DAILY",
            ),
            "app.market_theme_daily_table",
        ),
        timezone=validate_timezone(app_settings.get("timezone", "America/Chicago")),
        default_market_limit=max(5, int(app_settings.get("default_market_limit", 10))),
        default_trader_limit=max(5, int(app_settings.get("default_trader_limit", 15))),
    )


@st.cache_resource
def create_session() -> Session:
    connection_parameters = dict(st.secrets["snowflake"])
    app_config = load_app_config()
    session = Session.builder.configs(connection_parameters).create()
    session.sql(
        f"ALTER SESSION SET TIMEZONE = '{sql_string_literal(app_config.timezone)}'"
    ).collect()
    return session


def get_session() -> Session:
    session = create_session()
    try:
        session.sql("SELECT 1").collect()
        return session
    except Exception:
        try:
            session.close()
        except Exception:
            pass
        create_session.clear()
        session = create_session()
        session.sql("SELECT 1").collect()
        return session
    
@st.cache_data(ttl=300, show_spinner=False)
def fetch_trade_history(condition_id: str, trade_limit: int) -> pd.DataFrame:
    app_config = load_app_config()
    escaped_condition_id = sql_string_literal(condition_id)
    query = f"""
        SELECT
            t.trade_ts,
            COALESCE(d.user_name, t.proxy_wallet) AS trader_identity,
            t.trade_side,
            t.outcome_name,
            t.price,
            t.size,
            t.usdc_volume,
            t.transaction_hash
        FROM {app_config.fact_user_activity_trades_table} t
        LEFT JOIN {app_config.dim_traders_table} d
          ON t.proxy_wallet = d.proxy_wallet
        WHERE t.condition_id = '{escaped_condition_id}'
        ORDER BY t.trade_ts DESC
        LIMIT {trade_limit}
    """
    return run_query_to_pandas(query)@st.cache_data(ttl=300, show_spinner=False)


@st.cache_data(ttl=300, show_spinner=False)
def get_today_for_timezone() -> date:
    session = get_session()
    current_day = session.sql("SELECT CURRENT_DATE() AS TODAY").to_pandas()
    return current_day.iloc[0]["TODAY"]


def rename_columns(df: pd.DataFrame) -> pd.DataFrame:
    return df.rename(columns={column: column.lower() for column in df.columns})


def unique_columns(columns: list[str]) -> list[str]:
    return list(dict.fromkeys(columns))


def is_missing_object_error(exc: Exception) -> bool:
    message = str(exc).lower()
    return (
        "does not exist or not authorized" in message
        or "object '" in message
        or "invalid identifier" in message
    )


def run_query_to_pandas(query: str, fallback_query: str | None = None) -> pd.DataFrame:
    session = get_session()
    try:
        return rename_columns(session.sql(query).to_pandas())
    except Exception as exc:
        if fallback_query is not None and is_missing_object_error(exc):
            return rename_columns(session.sql(fallback_query).to_pandas())
        raise


def latest_market_theme_cte(markets_table: str) -> str:
    return f"""
        latest_markets AS (
            SELECT *
            FROM (
                SELECT
                    condition_id,
                    group_item_title AS market_group_title,
                    events[0]:title::STRING AS event_title,
                    events[0]:slug::STRING AS event_slug,
                    COALESCE(
                        group_item_title,
                        events[0]:title::STRING,
                        question,
                        slug,
                        market_id
                    ) AS market_theme,
                    CASE
                        WHEN group_item_title IS NOT NULL THEN 'GROUP_ITEM_TITLE'
                        WHEN events[0]:title::STRING IS NOT NULL THEN 'EVENT_TITLE'
                        ELSE 'MARKET_LABEL_FALLBACK'
                    END AS market_theme_source,
                    ROW_NUMBER() OVER (
                        PARTITION BY condition_id
                        ORDER BY updated_at DESC NULLS LAST,
                                 transformed_at DESC NULLS LAST,
                                 market_id
                    ) AS row_num
                FROM {markets_table}
                WHERE condition_id IS NOT NULL
            )
            WHERE row_num = 1
        )
    """.strip()


def to_state_key(value) -> str:
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return str(value)


def selected_row_or_default(
    df: pd.DataFrame, selection, key_column: str, state_key: str
) -> pd.Series | None:
    if df.empty:
        return None

    selected = None
    selected_rows = selection.selection.rows
    if selected_rows:
        selected = df.iloc[selected_rows[0]]
        st.session_state[state_key] = to_state_key(selected[key_column])
    elif state_key in st.session_state:
        matches = df[
            df[key_column].apply(to_state_key) == str(st.session_state[state_key])
        ]
        if not matches.empty:
            selected = matches.iloc[0]

    if selected is None:
        selected = df.iloc[0]
        st.session_state[state_key] = to_state_key(selected[key_column])

    return selected


def render_horizontal_bar_chart(
    df: pd.DataFrame,
    label_column: str,
    value_column: str,
    label_title: str,
    value_title: str,
) -> None:
    chart_df = (
        df[[label_column, value_column]]
        .copy()
        .sort_values(value_column, ascending=False)
        .reset_index(drop=True)
    )
    max_value = float(chart_df[value_column].max()) if not chart_df.empty else 0.0
    negative_padding = max(max_value * 0.05, 1.0)

    chart = (
        alt.Chart(chart_df)
        .mark_bar()
        .encode(
            x=alt.X(
                f"{value_column}:Q",
                title=value_title,
                scale=alt.Scale(domainMin=-negative_padding, nice=True, zero=False),
            ),
            y=alt.Y(
                f"{label_column}:N",
                title=label_title,
                sort=alt.SortField(field=value_column, order="descending"),
            ),
            tooltip=[
                alt.Tooltip(f"{label_column}:N", title=label_title),
                alt.Tooltip(f"{value_column}:Q", title=value_title, format=",.2f"),
            ],
        )
        .interactive()
    )
    st.altair_chart(chart, width="stretch")


def render_time_series_chart(
    df: pd.DataFrame,
    date_column: str,
    value_column: str,
    date_title: str,
    value_title: str,
) -> None:
    chart_df = df[[date_column, value_column]].copy().sort_values(date_column)
    chart = (
        alt.Chart(chart_df)
        .mark_line(point=True)
        .encode(
            x=alt.X(f"{date_column}:T", title=date_title),
            y=alt.Y(f"{value_column}:Q", title=value_title),
            tooltip=[
                alt.Tooltip(f"{date_column}:T", title=date_title),
                alt.Tooltip(f"{value_column}:Q", title=value_title, format=",.2f"),
            ],
        )
        .interactive()
    )
    st.altair_chart(chart, width="stretch")


def render_grouped_metric_chart(
    df: pd.DataFrame, category_column: str, value_columns: list[str]
) -> None:
    chart_df = df[[category_column] + value_columns].melt(
        id_vars=[category_column],
        value_vars=value_columns,
        var_name="metric",
        value_name="value",
    )
    chart = (
        alt.Chart(chart_df)
        .mark_bar()
        .encode(
            x=alt.X(f"{category_column}:N", title=None),
            y=alt.Y("value:Q", title="Average value"),
            color=alt.Color("metric:N", title="Metric"),
            xOffset="metric:N",
            tooltip=[
                alt.Tooltip(f"{category_column}:N", title="Segment"),
                alt.Tooltip("metric:N", title="Metric"),
                alt.Tooltip("value:Q", title="Value", format=",.2f"),
            ],
        )
    )
    st.altair_chart(chart, width="stretch")


def render_cohort_heatmap(df: pd.DataFrame) -> None:
    chart = (
        alt.Chart(df)
        .mark_rect()
        .encode(
            x=alt.X("months_since_first_seen:O", title="Months since first trade"),
            y=alt.Y("cohort_month:T", title="Cohort month"),
            color=alt.Color("active_traders:Q", title="Active traders"),
            tooltip=[
                alt.Tooltip("cohort_month:T", title="Cohort month"),
                alt.Tooltip("activity_month:T", title="Activity month"),
                alt.Tooltip("months_since_first_seen:Q", title="Months since first trade"),
                alt.Tooltip("active_traders:Q", title="Active traders", format=",.0f"),
                alt.Tooltip("total_volume_usdc:Q", title="Total volume (USDC)", format=",.2f"),
                alt.Tooltip("total_trades:Q", title="Total trades", format=",.0f"),
            ],
        )
    )
    st.altair_chart(chart, width="stretch")


@st.cache_data(ttl=300, show_spinner=False)
def fetch_tracked_markets_for_day(selected_date: str, market_limit: int) -> pd.DataFrame:
    app_config = load_app_config()
    query = f"""
        SELECT
            trade_date,
            market_id,
            condition_id,
            market_question,
            market_label,
            active,
            closed,
            trade_count,
            unique_traders,
            total_volume_usdc,
            first_trade_ts,
            last_trade_ts
        FROM {app_config.tracked_market_volume_daily_table}
        WHERE trade_date = '{selected_date}'
          AND {resolved_market_predicate()}
        ORDER BY total_volume_usdc DESC, trade_count DESC, market_label ASC
        LIMIT {market_limit}
    """
    return run_query_to_pandas(query)


@st.cache_data(ttl=300, show_spinner=False)
def fetch_market_daily_rankings(
    selected_date: str,
    market_limit: int,
    ranking_mode: str,
) -> pd.DataFrame:
    app_config = load_app_config()
    order_by = {
        "unique_traders": "unique_traders DESC, total_volume_usdc DESC, market_label ASC",
        "avg_trade_size": "avg_trade_size DESC, trade_count DESC, market_label ASC",
        "usdc_per_trader": "usdc_per_trader DESC, total_volume_usdc DESC, market_label ASC",
    }[ranking_mode]
    query = f"""
        SELECT
            stat_date AS trade_date,
            market_id,
            condition_id,
            market_question,
            market_label,
            active,
            closed,
            trade_count,
            unique_wallets AS unique_traders,
            total_usdc_volume AS total_volume_usdc,
            avg_trade_size,
            ROUND(total_usdc_volume / NULLIF(unique_wallets, 0), 2) AS usdc_per_trader,
            first_trade_ts,
            last_trade_ts
        FROM {app_config.fact_market_daily_table}
        WHERE stat_date = '{selected_date}'
          AND {resolved_market_predicate()}
        ORDER BY {order_by}
        LIMIT {market_limit}
    """
    return run_query_to_pandas(query)


@st.cache_data(ttl=300, show_spinner=False)
def fetch_market_concentration_for_day(
    selected_date: str,
    market_limit: int,
) -> pd.DataFrame:
    app_config = load_app_config()
    query = f"""
        SELECT
            trade_date,
            market_id,
            condition_id,
            market_question,
            market_label,
            active,
            closed,
            trade_count,
            unique_traders,
            total_volume_usdc,
            top_1_volume_usdc,
            top_5_volume_usdc,
            top_1_share_volume,
            top_5_share_volume,
            volume_per_trader_usdc,
            first_trade_ts,
            last_trade_ts
        FROM {app_config.market_concentration_daily_table}
        WHERE trade_date = '{selected_date}'
          AND {resolved_market_predicate()}
        ORDER BY top_5_share_volume DESC, total_volume_usdc DESC, market_label ASC
        LIMIT {market_limit}
    """
    fallback_query = f"""
        WITH market_base AS (
            SELECT
                stat_date AS trade_date,
                market_id,
                condition_id,
                market_question,
                market_label,
                active,
                closed,
                trade_count,
                unique_wallets AS unique_traders,
                total_usdc_volume AS total_volume_usdc,
                first_trade_ts,
                last_trade_ts
            FROM {app_config.fact_market_daily_table}
            WHERE stat_date = '{selected_date}'
              AND {resolved_market_predicate()}
        ),
        trader_ranked AS (
            SELECT
                trade_date,
                condition_id,
                proxy_wallet,
                total_volume_usdc,
                ROW_NUMBER() OVER (
                    PARTITION BY trade_date, condition_id
                    ORDER BY total_volume_usdc DESC, trade_count DESC, proxy_wallet ASC
                ) AS trader_rank
            FROM {app_config.market_top_traders_daily_table}
            WHERE trade_date = '{selected_date}'
        ),
        concentration AS (
            SELECT
                trade_date,
                condition_id,
                ROUND(SUM(CASE WHEN trader_rank = 1 THEN total_volume_usdc ELSE 0 END), 2) AS top_1_volume_usdc,
                ROUND(SUM(CASE WHEN trader_rank <= 5 THEN total_volume_usdc ELSE 0 END), 2) AS top_5_volume_usdc
            FROM trader_ranked
            GROUP BY 1, 2
        )
        SELECT
            base.trade_date,
            base.market_id,
            base.condition_id,
            base.market_question,
            base.market_label,
            base.active,
            base.closed,
            base.trade_count,
            base.unique_traders,
            base.total_volume_usdc,
            COALESCE(concentration.top_1_volume_usdc, 0) AS top_1_volume_usdc,
            COALESCE(concentration.top_5_volume_usdc, 0) AS top_5_volume_usdc,
            ROUND(
                COALESCE(concentration.top_1_volume_usdc, 0) / NULLIF(base.total_volume_usdc, 0),
                4
            ) AS top_1_share_volume,
            ROUND(
                COALESCE(concentration.top_5_volume_usdc, 0) / NULLIF(base.total_volume_usdc, 0),
                4
            ) AS top_5_share_volume,
            ROUND(base.total_volume_usdc / NULLIF(base.unique_traders, 0), 2) AS volume_per_trader_usdc,
            base.first_trade_ts,
            base.last_trade_ts
        FROM market_base AS base
        LEFT JOIN concentration
          ON base.trade_date = concentration.trade_date
         AND base.condition_id = concentration.condition_id
        ORDER BY top_5_share_volume DESC, total_volume_usdc DESC, market_label ASC
        LIMIT {market_limit}
    """
    return run_query_to_pandas(query, fallback_query)


@st.cache_data(ttl=300, show_spinner=False)
def fetch_highest_volume_markets(market_limit: int, open_only: bool) -> pd.DataFrame:
    app_config = load_app_config()
    filters = [resolved_market_predicate()]
    if open_only:
        filters.append("closed = FALSE")
    where_clause = f"WHERE {' AND '.join(filters)}"
    query = f"""
        SELECT
            market_id,
            condition_id,
            market_question,
            market_label,
            active,
            closed,
            end_date,
            total_volume_usdc
        FROM {app_config.highest_volume_markets_table}
        {where_clause}
        ORDER BY total_volume_usdc DESC, market_label ASC
        LIMIT {market_limit}
    """
    return run_query_to_pandas(query)


@st.cache_data(ttl=300, show_spinner=False)
def fetch_theme_daily_rankings(
    selected_date: str,
    theme_limit: int,
    ranking_mode: str,
) -> pd.DataFrame:
    app_config = load_app_config()
    order_by = {
        "total_volume_usdc": "total_volume_usdc DESC, unique_traders DESC, market_theme ASC",
        "unique_traders": "unique_traders DESC, total_volume_usdc DESC, market_theme ASC",
        "avg_trade_size": "avg_trade_size DESC, total_volume_usdc DESC, market_theme ASC",
        "trade_count": "trade_count DESC, total_volume_usdc DESC, market_theme ASC",
    }[ranking_mode]
    query = f"""
        SELECT
            trade_date,
            market_theme,
            market_theme_source,
            market_group_title,
            event_title,
            event_slug,
            distinct_markets,
            trade_count,
            unique_traders,
            total_volume_usdc,
            avg_trade_size,
            max_trade_size,
            first_trade_ts,
            last_trade_ts
        FROM {app_config.market_theme_daily_table}
        WHERE trade_date = '{selected_date}'
          AND market_theme_source <> 'FACT_FALLBACK'
        ORDER BY {order_by}
        LIMIT {theme_limit}
    """
    fallback_query = f"""
        WITH
        {latest_market_theme_cte(app_config.markets_table)}
        SELECT
            f.trade_date,
            COALESCE(
                m.market_theme,
                m.market_group_title,
                m.event_title,
                f.market_label,
                f.market_question,
                f.condition_id
            ) AS market_theme,
            COALESCE(m.market_theme_source, 'FACT_FALLBACK') AS market_theme_source,
            MAX(m.market_group_title) AS market_group_title,
            MAX(m.event_title) AS event_title,
            MAX(m.event_slug) AS event_slug,
            COUNT(DISTINCT f.condition_id) AS distinct_markets,
            COUNT(*) AS trade_count,
            COUNT(DISTINCT f.proxy_wallet) AS unique_traders,
            ROUND(SUM(f.usdc_volume), 2) AS total_volume_usdc,
            ROUND(AVG(f.usdc_volume), 2) AS avg_trade_size,
            ROUND(MAX(f.usdc_volume), 2) AS max_trade_size,
            MIN(f.trade_ts) AS first_trade_ts,
            MAX(f.trade_ts) AS last_trade_ts
        FROM {app_config.fact_user_activity_trades_table} f
        LEFT JOIN latest_markets m
          ON f.condition_id = m.condition_id
        WHERE f.trade_date = '{selected_date}'
          AND {resolved_market_predicate('f')}
        GROUP BY
            f.trade_date,
            COALESCE(
                m.market_theme,
                m.market_group_title,
                m.event_title,
                f.market_label,
                f.market_question,
                f.condition_id
            ),
            COALESCE(m.market_theme_source, 'FACT_FALLBACK')
        ORDER BY {order_by}
        LIMIT {theme_limit}
    """
    return run_query_to_pandas(query, fallback_query)


@st.cache_data(ttl=300, show_spinner=False)
def fetch_markets_for_theme_day(
    selected_date: str,
    market_theme: str,
    market_limit: int,
) -> pd.DataFrame:
    app_config = load_app_config()
    escaped_theme = sql_string_literal(market_theme)
    query = f"""
        SELECT
            f.stat_date AS trade_date,
            f.market_id,
            f.condition_id,
            f.market_question,
            f.market_label,
            d.market_theme,
            d.market_theme_source,
            d.market_group_title,
            d.event_title,
            f.trade_count,
            f.unique_wallets AS unique_traders,
            f.total_usdc_volume AS total_volume_usdc,
            f.avg_trade_size,
            f.first_trade_ts,
            f.last_trade_ts
        FROM {app_config.fact_market_daily_table} f
        LEFT JOIN {app_config.dim_markets_table} d
          ON f.condition_id = d.condition_id
        WHERE f.stat_date = '{selected_date}'
          AND {resolved_market_predicate('f')}
          AND COALESCE(
                d.market_theme,
                d.market_group_title,
                d.event_title,
                f.market_label,
                f.market_question,
                f.condition_id
              ) = '{escaped_theme}'
        ORDER BY total_volume_usdc DESC, trade_count DESC, market_label ASC
        LIMIT {market_limit}
    """
    fallback_query = f"""
        WITH
        {latest_market_theme_cte(app_config.markets_table)}
        SELECT
            f.stat_date AS trade_date,
            f.market_id,
            f.condition_id,
            f.market_question,
            f.market_label,
            COALESCE(
                m.market_theme,
                m.market_group_title,
                m.event_title,
                f.market_label,
                f.market_question,
                f.condition_id
            ) AS market_theme,
            COALESCE(m.market_theme_source, 'FACT_FALLBACK') AS market_theme_source,
            m.market_group_title,
            m.event_title,
            f.trade_count,
            f.unique_wallets AS unique_traders,
            f.total_usdc_volume AS total_volume_usdc,
            f.avg_trade_size,
            f.first_trade_ts,
            f.last_trade_ts
        FROM {app_config.fact_market_daily_table} f
        LEFT JOIN latest_markets m
          ON f.condition_id = m.condition_id
        WHERE f.stat_date = '{selected_date}'
          AND {resolved_market_predicate('f')}
          AND COALESCE(
                m.market_theme,
                m.market_group_title,
                m.event_title,
                f.market_label,
                f.market_question,
                f.condition_id
              ) = '{escaped_theme}'
        ORDER BY total_volume_usdc DESC, trade_count DESC, market_label ASC
        LIMIT {market_limit}
    """
    return run_query_to_pandas(query, fallback_query)


@st.cache_data(ttl=300, show_spinner=False)
def fetch_largest_bets_for_day(selected_date: str, market_limit: int) -> pd.DataFrame:
    app_config = load_app_config()
    query = f"""
        SELECT
            trade_date,
            trade_ts,
            market_id,
            condition_id,
            market_question,
            market_label,
            proxy_wallet,
            trade_side,
            outcome_name,
            price,
            size,
            usdc_volume,
            transaction_hash
        FROM {app_config.fact_user_activity_trades_table}
        WHERE trade_date = '{selected_date}'
          AND {resolved_market_predicate()}
        ORDER BY usdc_volume DESC, trade_ts DESC, market_label ASC
        LIMIT {market_limit}
    """
    return run_query_to_pandas(query)


@st.cache_data(ttl=300, show_spinner=False)
def fetch_wallet_daily_rankings(
    selected_date: str, trader_limit: int, ranking_mode: str
) -> pd.DataFrame:
    app_config = load_app_config()
    order_by = {
        "total_usdc_volume": "total_usdc_volume DESC, trade_count DESC, proxy_wallet ASC",
        "trade_count": "trade_count DESC, total_usdc_volume DESC, proxy_wallet ASC",
    }[ranking_mode]
    query = f"""
        SELECT
            proxy_wallet,
            stat_date,
            trade_count,
            distinct_markets,
            total_usdc_volume,
            buy_volume,
            sell_volume,
            avg_trade_size,
            max_trade_size
        FROM {app_config.fact_wallet_daily_table}
        WHERE stat_date = '{selected_date}'
        ORDER BY {order_by}
        LIMIT {trader_limit}
    """
    return run_query_to_pandas(query)


@st.cache_data(ttl=300, show_spinner=False)
def fetch_dim_trader_rankings(
    trader_limit: int, ranking_mode: str, min_total_trades: int
) -> pd.DataFrame:
    app_config = load_app_config()
    order_by = {
        "total_volume": "total_volume DESC, total_trades DESC, proxy_wallet ASC",
        "avg_trade_size": "avg_trade_size DESC, total_trades DESC, proxy_wallet ASC",
        "distinct_markets": "distinct_markets DESC, total_volume DESC, proxy_wallet ASC",
    }[ranking_mode]
    query = f"""
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
            first_seen,
            last_seen,
            anomaly_involvement_count
        FROM {app_config.dim_traders_table}
        WHERE total_trades >= {min_total_trades}
        ORDER BY {order_by}
        LIMIT {trader_limit}
    """
    return run_query_to_pandas(query)


@st.cache_data(ttl=300, show_spinner=False)
def fetch_top_traders_daily(
    condition_id: str, trader_limit: int, selected_date: str
) -> pd.DataFrame:
    app_config = load_app_config()
    escaped_condition_id = sql_string_literal(condition_id)
    query = f"""
        SELECT
            proxy_wallet,
            total_volume_usdc,
            trade_count,
            average_trade_usdc,
            largest_trade_usdc
        FROM {app_config.market_top_traders_daily_table}
        WHERE condition_id = '{escaped_condition_id}'
          AND trade_date = '{selected_date}'
        ORDER BY total_volume_usdc DESC, trade_count DESC, proxy_wallet ASC
        LIMIT {trader_limit}
    """
    return run_query_to_pandas(query)


@st.cache_data(ttl=300, show_spinner=False)
def fetch_top_traders_all_time(condition_id: str, trader_limit: int) -> pd.DataFrame:
    app_config = load_app_config()
    escaped_condition_id = sql_string_literal(condition_id)
    query = f"""
        SELECT
            proxy_wallet,
            total_volume_usdc,
            trade_count,
            average_trade_usdc,
            largest_trade_usdc
        FROM {app_config.market_top_traders_all_time_table}
        WHERE condition_id = '{escaped_condition_id}'
        ORDER BY total_volume_usdc DESC, trade_count DESC, proxy_wallet ASC
        LIMIT {trader_limit}
    """
    return run_query_to_pandas(query)


@st.cache_data(ttl=300, show_spinner=False)
def fetch_daily_platform_volume(day_limit: int) -> pd.DataFrame:
    app_config = load_app_config()
    query = f"""
        SELECT
            trade_date,
            total_volume_usdc,
            total_trades AS trade_count,
            markets_with_trades,
            active_traders,
            avg_volume_per_active_trader,
            avg_trades_per_active_trader
        FROM {app_config.platform_daily_summary_table}
        ORDER BY trade_date DESC
        LIMIT {day_limit}
    """
    fallback_query = f"""
        WITH wallet_day AS (
            SELECT
                stat_date,
                COUNT(*) AS active_traders,
                ROUND(AVG(total_usdc_volume), 2) AS avg_volume_per_active_trader,
                ROUND(AVG(trade_count), 2) AS avg_trades_per_active_trader
            FROM {app_config.fact_wallet_daily_table}
            GROUP BY 1
        )
        SELECT
            market_day.stat_date AS trade_date,
            ROUND(SUM(market_day.total_usdc_volume), 2) AS total_volume_usdc,
            SUM(market_day.trade_count) AS trade_count,
            COUNT_IF(market_day.trade_count > 0) AS markets_with_trades,
            wallet_day.active_traders,
            wallet_day.avg_volume_per_active_trader,
            wallet_day.avg_trades_per_active_trader
        FROM {app_config.fact_market_daily_table} AS market_day
        LEFT JOIN wallet_day
          ON market_day.stat_date = wallet_day.stat_date
        GROUP BY
            market_day.stat_date,
            wallet_day.active_traders,
            wallet_day.avg_volume_per_active_trader,
            wallet_day.avg_trades_per_active_trader
        ORDER BY trade_date DESC
        LIMIT {day_limit}
    """
    return run_query_to_pandas(query, fallback_query)


@st.cache_data(ttl=300, show_spinner=False)
def fetch_trader_profile(proxy_wallet: str) -> pd.DataFrame:
    app_config = load_app_config()
    escaped_wallet = sql_string_literal(proxy_wallet)
    query = f"""
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
            first_seen,
            last_seen,
            anomaly_involvement_count
        FROM {app_config.dim_traders_table}
        WHERE proxy_wallet = '{escaped_wallet}'
    """
    return run_query_to_pandas(query)


@st.cache_data(ttl=300, show_spinner=False)
def fetch_trader_daily_history(proxy_wallet: str, day_limit: int) -> pd.DataFrame:
    app_config = load_app_config()
    escaped_wallet = sql_string_literal(proxy_wallet)
    query = f"""
        SELECT
            stat_date,
            trade_count,
            distinct_markets,
            total_usdc_volume,
            avg_trade_size
        FROM {app_config.fact_wallet_daily_table}
        WHERE proxy_wallet = '{escaped_wallet}'
        ORDER BY stat_date DESC
        LIMIT {day_limit}
    """
    return run_query_to_pandas(query)


@st.cache_data(ttl=300, show_spinner=False)
def fetch_trader_market_footprint(proxy_wallet: str, market_limit: int) -> pd.DataFrame:
    app_config = load_app_config()
    escaped_wallet = sql_string_literal(proxy_wallet)
    query = f"""
        SELECT
            market_label,
            condition_id,
            ROUND(SUM(usdc_volume), 2) AS total_volume_usdc,
            COUNT(*) AS trade_count,
            ROUND(AVG(usdc_volume), 2) AS average_trade_usdc,
            MAX(trade_ts) AS last_trade_ts
        FROM {app_config.fact_user_activity_trades_table}
        WHERE proxy_wallet = '{escaped_wallet}'
          AND {resolved_market_predicate()}
        GROUP BY market_label, condition_id
        ORDER BY total_volume_usdc DESC, trade_count DESC, market_label ASC
        LIMIT {market_limit}
    """
    return run_query_to_pandas(query)


@st.cache_data(ttl=300, show_spinner=False)
def fetch_trader_recent_trades(proxy_wallet: str, trade_limit: int) -> pd.DataFrame:
    app_config = load_app_config()
    escaped_wallet = sql_string_literal(proxy_wallet)
    query = f"""
        SELECT
            trade_ts,
            market_label,
            trade_side,
            outcome_name,
            usdc_volume,
            price,
            size
        FROM {app_config.fact_user_activity_trades_table}
        WHERE proxy_wallet = '{escaped_wallet}'
          AND {resolved_market_predicate()}
        ORDER BY trade_ts DESC
        LIMIT {trade_limit}
    """
    return run_query_to_pandas(query)


@st.cache_data(ttl=300, show_spinner=False)
def fetch_big_vs_small_trader_segments(min_total_trades: int) -> pd.DataFrame:
    app_config = load_app_config()
    query = f"""
        SELECT
            CASE
                WHEN volume_decile = 1 THEN 'Top decile by total volume'
                ELSE 'Remaining traders'
            END AS segment,
            COUNT(*) AS trader_count,
            ROUND(AVG(total_volume), 2) AS avg_total_volume,
            ROUND(AVG(total_trades), 2) AS avg_total_trades,
            ROUND(AVG(avg_trade_size), 2) AS avg_trade_size,
            ROUND(AVG(distinct_markets), 2) AS avg_distinct_markets,
            ROUND(AVG(anomaly_involvement_count), 2) AS avg_anomaly_involvement
        FROM {app_config.trader_segment_snapshot_table}
        WHERE total_trades >= {min_total_trades}
        GROUP BY 1
        ORDER BY CASE WHEN segment = 'Top decile by total volume' THEN 0 ELSE 1 END
    """
    fallback_query = f"""
        WITH ranked_traders AS (
            SELECT
                *,
                NTILE(10) OVER (ORDER BY total_volume DESC NULLS LAST) AS volume_decile
            FROM {app_config.dim_traders_table}
            WHERE total_trades >= {min_total_trades}
        )
        SELECT
            CASE
                WHEN volume_decile = 1 THEN 'Top decile by total volume'
                ELSE 'Remaining traders'
            END AS segment,
            COUNT(*) AS trader_count,
            ROUND(AVG(total_volume), 2) AS avg_total_volume,
            ROUND(AVG(total_trades), 2) AS avg_total_trades,
            ROUND(AVG(avg_trade_size), 2) AS avg_trade_size,
            ROUND(AVG(distinct_markets), 2) AS avg_distinct_markets,
            ROUND(AVG(anomaly_involvement_count), 2) AS avg_anomaly_involvement
        FROM ranked_traders
        GROUP BY 1
        ORDER BY CASE WHEN segment = 'Top decile by total volume' THEN 0 ELSE 1 END
    """
    return run_query_to_pandas(query, fallback_query)


@st.cache_data(ttl=300, show_spinner=False)
def fetch_trader_cohort_monthly(month_limit: int) -> pd.DataFrame:
    app_config = load_app_config()
    query = f"""
        SELECT
            cohort_month,
            activity_month,
            months_since_first_seen,
            active_traders,
            total_volume_usdc,
            total_trades,
            avg_volume_per_active_trader,
            avg_trades_per_active_trader,
            avg_active_days
        FROM {app_config.trader_cohort_monthly_table}
        WHERE months_since_first_seen BETWEEN 0 AND {month_limit}
        ORDER BY cohort_month DESC, months_since_first_seen ASC
    """
    fallback_query = f"""
        WITH wallet_monthly AS (
            SELECT
                wallet_daily.proxy_wallet,
                DATE_TRUNC('month', CAST(traders.first_seen AS DATE)) AS cohort_month,
                DATE_TRUNC('month', wallet_daily.stat_date) AS activity_month,
                DATEDIFF(
                    'month',
                    DATE_TRUNC('month', CAST(traders.first_seen AS DATE)),
                    DATE_TRUNC('month', wallet_daily.stat_date)
                ) AS months_since_first_seen,
                SUM(wallet_daily.total_usdc_volume) AS total_volume_usdc,
                SUM(wallet_daily.trade_count) AS total_trades,
                COUNT(DISTINCT wallet_daily.stat_date) AS active_days
            FROM {app_config.fact_wallet_daily_table} AS wallet_daily
            INNER JOIN {app_config.dim_traders_table} AS traders
              ON wallet_daily.proxy_wallet = traders.proxy_wallet
            WHERE DATEDIFF(
                    'month',
                    DATE_TRUNC('month', CAST(traders.first_seen AS DATE)),
                    DATE_TRUNC('month', wallet_daily.stat_date)
                ) BETWEEN 0 AND {month_limit}
            GROUP BY 1, 2, 3, 4
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
            ROUND(AVG(active_days), 2) AS avg_active_days
        FROM wallet_monthly
        GROUP BY 1, 2, 3
        ORDER BY cohort_month DESC, months_since_first_seen ASC
    """
    return run_query_to_pandas(query, fallback_query)


def render_market_trader_drilldown(
    selected_market: pd.Series,
    top_traders_df: pd.DataFrame,
    time_scope: str,
    selected_value_label: str,
    selected_value_text: str,
) -> None:
    st.subheader("Top traders in the selected market")
    st.write(f"Selected market: **{selected_market['market_label']}**")
    st.caption(f"Top traders are ranked by total traded volume {time_scope}.")

    metric_col_1, metric_col_2 = st.columns(2)
    metric_col_1.metric(selected_value_label, selected_value_text)
    metric_col_2.metric("Top traders shown", f"{len(top_traders_df):,}")

    if top_traders_df.empty:
        st.info("No trader rows were found for the selected market in the analytics layer.")
        return

    st.dataframe(
        top_traders_df,
        hide_index=True,
        width="stretch",
        column_config={
            "proxy_wallet": st.column_config.TextColumn("Proxy wallet"),
            "total_volume_usdc": st.column_config.NumberColumn(
                "Total volume (USDC)", format="$%.2f"
            ),
            "trade_count": st.column_config.NumberColumn("Trades", format="%d"),
            "average_trade_usdc": st.column_config.NumberColumn(
                "Average trade (USDC)", format="$%.2f"
            ),
            "largest_trade_usdc": st.column_config.NumberColumn(
                "Largest trade (USDC)", format="$%.2f"
            ),
        },
    )
    render_horizontal_bar_chart(
        top_traders_df,
        label_column="proxy_wallet",
        value_column="total_volume_usdc",
        label_title="Proxy wallet",
        value_title="Total volume (USDC)",
    )


def render_trader_profile_section(
    proxy_wallet: str,
    market_limit: int,
    history_days: int,
    recent_trade_limit: int,
) -> None:
    profile_df = fetch_trader_profile(proxy_wallet)
    if profile_df.empty:
        st.info("No trader profile was found for the selected wallet.")
        return

    profile = profile_df.iloc[0]
    display_name = profile["proxy_wallet"]
    if pd.notna(profile.get("user_name")) and str(profile["user_name"]).strip():
        display_name = f"{profile['user_name']} ({profile['proxy_wallet']})"

    st.subheader("Trader profile")
    st.write(f"Selected trader: **{display_name}**")

    metric_col_1, metric_col_2, metric_col_3, metric_col_4 = st.columns(4)
    metric_col_1.metric("Total volume", f"${float(profile['total_volume']):,.2f}")
    metric_col_2.metric("Total trades", f"{int(profile['total_trades']):,}")
    metric_col_3.metric("Distinct markets", f"{int(profile['distinct_markets']):,}")
    metric_col_4.metric("Average trade size", f"${float(profile['avg_trade_size']):,.2f}")

    history_df = fetch_trader_daily_history(proxy_wallet, history_days)
    footprint_df = fetch_trader_market_footprint(proxy_wallet, market_limit)
    recent_trades_df = fetch_trader_recent_trades(proxy_wallet, recent_trade_limit)

    history_tab, footprint_tab, recent_tab = st.tabs(
        ["Daily Activity", "Market Footprint", "Recent Trades"]
    )

    with history_tab:
        if history_df.empty:
            st.info("No daily wallet history is available for this trader.")
        else:
            st.dataframe(
                history_df.sort_values("stat_date", ascending=False),
                hide_index=True,
                width="stretch",
                column_config={
                    "stat_date": st.column_config.DateColumn("Date"),
                    "trade_count": st.column_config.NumberColumn("Trades", format="%d"),
                    "distinct_markets": st.column_config.NumberColumn(
                        "Markets", format="%d"
                    ),
                    "total_usdc_volume": st.column_config.NumberColumn(
                        "Volume (USDC)", format="$%.2f"
                    ),
                    "avg_trade_size": st.column_config.NumberColumn(
                        "Average trade (USDC)", format="$%.2f"
                    ),
                },
            )
            render_time_series_chart(
                history_df,
                date_column="stat_date",
                value_column="total_usdc_volume",
                date_title="Date",
                value_title="Daily volume (USDC)",
            )

    with footprint_tab:
        if footprint_df.empty:
            st.info("No market footprint rows were found for this trader.")
        else:
            st.dataframe(
                footprint_df,
                hide_index=True,
                width="stretch",
                column_config={
                    "market_label": st.column_config.TextColumn("Market"),
                    "total_volume_usdc": st.column_config.NumberColumn(
                        "Total volume (USDC)", format="$%.2f"
                    ),
                    "trade_count": st.column_config.NumberColumn("Trades", format="%d"),
                    "average_trade_usdc": st.column_config.NumberColumn(
                        "Average trade (USDC)", format="$%.2f"
                    ),
                    "last_trade_ts": st.column_config.DatetimeColumn("Last trade"),
                },
            )
            render_horizontal_bar_chart(
                footprint_df,
                label_column="market_label",
                value_column="total_volume_usdc",
                label_title="Market",
                value_title="Total volume (USDC)",
            )

    with recent_tab:
        if recent_trades_df.empty:
            st.info("No recent trades were found for this trader.")
        else:
            st.dataframe(
                recent_trades_df,
                hide_index=True,
                width="stretch",
                column_config={
                    "trade_ts": st.column_config.DatetimeColumn("Trade time"),
                    "market_label": st.column_config.TextColumn("Market"),
                    "trade_side": st.column_config.TextColumn("Side"),
                    "outcome_name": st.column_config.TextColumn("Outcome"),
                    "usdc_volume": st.column_config.NumberColumn(
                        "Trade size (USDC)", format="$%.2f"
                    ),
                    "price": st.column_config.NumberColumn("Price", format="%.4f"),
                    "size": st.column_config.NumberColumn("Contracts", format="%.6f"),
                },
            )


def render_daily_market_question(
    question_name: str, selected_date_sql: str, market_limit: int, trader_limit: int
) -> None:
    if question_name == QUESTION_TRACKED_TODAY:
        markets_df = fetch_tracked_markets_for_day(selected_date_sql, market_limit)
        st.caption(
            "Warehouse-driven daily tracked-volume rankings from "
            "`ANALYTICS.TRACKED_MARKET_VOLUME_DAILY`."
        )
        st.subheader(f"Top tracked markets on {selected_date_sql}")
        st.caption(
            "This ranking comes from the materialized analytics table rather than a "
            "live join inside the app."
        )
        metric_label = "Tracked volume (USDC)"
        chart_value_column = "total_volume_usdc"
        metric_format = "$%.2f"
        selection_key = "daily_market_selection::tracked"
    elif question_name == QUESTION_WHALE_DOMINATED:
        markets_df = fetch_market_concentration_for_day(selected_date_sql, market_limit)
        st.caption(
            "Warehouse-driven concentration analytics from "
            "`ANALYTICS.MARKET_CONCENTRATION_DAILY`."
        )
        st.subheader(f"Whale-dominated markets on {selected_date_sql}")
        st.caption(
            "This view ranks markets by the share of daily volume captured by the top 5 traders."
        )
        metric_label = "Top 5 share of volume"
        chart_value_column = "top_5_share_volume"
        metric_format = "%.4f"
        selection_key = "daily_market_selection::concentration"
    else:
        ranking_mode = {
            QUESTION_MARKETS_UNIQUE: "unique_traders",
            QUESTION_MARKETS_AVG_BET: "avg_trade_size",
        }[question_name]
        markets_df = fetch_market_daily_rankings(
            selected_date_sql, market_limit, ranking_mode
        )
        titles = {
            QUESTION_MARKETS_UNIQUE: (
                f"Markets with the most unique traders on {selected_date_sql}",
                "Unique traders",
                "This highlights breadth of participation rather than raw dollars.",
            ),
            QUESTION_MARKETS_AVG_BET: (
                f"Markets with the largest average bet on {selected_date_sql}",
                "Average trade size (USDC)",
                "This contrasts whale-heavy markets against smaller-bet markets.",
            ),
        }
        header, metric_label, caption = titles[question_name]
        st.caption(
            "Warehouse-driven daily market analytics from `ANALYTICS.FACT_MARKET_DAILY`."
        )
        st.subheader(header)
        st.caption(caption)
        chart_value_column = ranking_mode
        metric_format = "%d" if ranking_mode == "unique_traders" else "$%.2f"
        selection_key = f"daily_market_selection::{ranking_mode}"

    if markets_df.empty:
        st.warning("No market rows were found for the selected day.")
        return

    metric_col_1, metric_col_2, metric_col_3 = st.columns(3)
    metric_col_1.metric(
        "Visible volume", f"${float(markets_df['total_volume_usdc'].sum()):,.2f}"
    )
    metric_col_2.metric("Visible trades", f"{int(markets_df['trade_count'].sum()):,}")
    metric_col_3.metric(
        "Visible markets", f"{markets_df['condition_id'].nunique():,}"
    )

    columns = ["market_label"]
    if chart_value_column != "total_volume_usdc":
        columns.append(chart_value_column)
    if question_name == QUESTION_WHALE_DOMINATED:
        columns.extend(["top_1_share_volume", "top_1_volume_usdc", "top_5_volume_usdc"])
    columns.extend(
        [
            "total_volume_usdc",
            "trade_count",
            "unique_traders",
            "first_trade_ts",
            "last_trade_ts",
        ]
    )
    table = st.dataframe(
        markets_df[unique_columns(columns)],
        hide_index=True,
        width="stretch",
        on_select="rerun",
        selection_mode="single-row",
        column_config={
            "market_label": st.column_config.TextColumn("Market"),
            chart_value_column: st.column_config.NumberColumn(
                metric_label,
                format=metric_format,
            ),
            "top_1_share_volume": st.column_config.NumberColumn(
                "Top 1 share", format="%.4f"
            ),
            "top_1_volume_usdc": st.column_config.NumberColumn(
                "Top 1 volume (USDC)", format="$%.2f"
            ),
            "top_5_volume_usdc": st.column_config.NumberColumn(
                "Top 5 volume (USDC)", format="$%.2f"
            ),
            "total_volume_usdc": st.column_config.NumberColumn(
                "Total volume (USDC)", format="$%.2f"
            ),
            "trade_count": st.column_config.NumberColumn("Trades", format="%d"),
            "unique_traders": st.column_config.NumberColumn(
                "Unique traders", format="%d"
            ),
            "first_trade_ts": st.column_config.DatetimeColumn("First trade"),
            "last_trade_ts": st.column_config.DatetimeColumn("Last trade"),
        },
        key=selection_key,
    )
    selected_market = selected_row_or_default(
        markets_df, table, "condition_id", f"{selection_key}::selected"
    )
    if selected_market is None:
        return

    render_horizontal_bar_chart(
        markets_df,
        label_column="market_label",
        value_column=chart_value_column,
        label_title="Market",
        value_title=metric_label,
    )

    top_traders_df = fetch_top_traders_daily(
        selected_market["condition_id"], trader_limit, selected_date_sql
    )
    render_market_trader_drilldown(
        selected_market=selected_market,
        top_traders_df=top_traders_df,
        time_scope=f"on {selected_date_sql}",
        selected_value_label=metric_label,
        selected_value_text=(
            f"{float(selected_market[chart_value_column]):.4f}"
            if question_name == QUESTION_WHALE_DOMINATED
            else
            f"{int(selected_market[chart_value_column]):,}"
            if chart_value_column == "unique_traders"
            else f"${float(selected_market[chart_value_column]):,.2f}"
        ),
    )


def render_highest_volume_question(market_limit: int, trader_limit: int, open_only: bool) -> None:
    markets_df = fetch_highest_volume_markets(market_limit, open_only)
    st.caption(
        "Warehouse-driven listed-volume rankings from "
        "`ANALYTICS.HIGHEST_VOLUME_MARKETS`."
    )
    st.subheader("Highest volume markets")
    st.caption(
        "This ranking comes from the materialized analytics table built from the curated markets dataset."
    )

    if markets_df.empty:
        st.warning("No listed-volume market rows were found for the selected controls.")
        return

    metric_col_1, metric_col_2, metric_col_3 = st.columns(3)
    metric_col_1.metric(
        "Visible listed volume", f"${float(markets_df['total_volume_usdc'].sum()):,.2f}"
    )
    metric_col_2.metric("Visible markets", f"{len(markets_df):,}")
    metric_col_3.metric(
        "Visible open markets", f"{int((~markets_df['closed'].fillna(False)).sum()):,}"
    )

    table = st.dataframe(
        markets_df[
            ["market_label", "total_volume_usdc", "active", "closed", "end_date"]
        ],
        hide_index=True,
        width="stretch",
        on_select="rerun",
        selection_mode="single-row",
        column_config={
            "market_label": st.column_config.TextColumn("Market"),
            "total_volume_usdc": st.column_config.NumberColumn(
                "Listed volume (USDC)", format="$%.2f"
            ),
            "active": st.column_config.CheckboxColumn("Active"),
            "closed": st.column_config.CheckboxColumn("Closed"),
            "end_date": st.column_config.DatetimeColumn("End date"),
        },
        key="highest_volume_markets_table",
    )
    selected_market = selected_row_or_default(
        markets_df,
        table,
        "condition_id",
        "highest_volume_markets_table::selected",
    )
    if selected_market is None:
        return

    render_horizontal_bar_chart(
        markets_df,
        label_column="market_label",
        value_column="total_volume_usdc",
        label_title="Market",
        value_title="Listed volume (USDC)",
    )

    top_traders_df = fetch_top_traders_all_time(
        selected_market["condition_id"], trader_limit
    )
    render_market_trader_drilldown(
        selected_market=selected_market,
        top_traders_df=top_traders_df,
        time_scope="across all tracked history",
        selected_value_label="Selected market listed volume",
        selected_value_text=f"${float(selected_market['total_volume_usdc']):,.2f}",
    )


def render_largest_bets_question(
    selected_date_sql: str, market_limit: int, trader_limit: int
) -> None:
    markets_df = fetch_largest_bets_for_day(selected_date_sql, market_limit)
    st.caption(
        "Largest individual trades for the selected day from "
        "`ANALYTICS.FACT_USER_ACTIVITY_TRADES`, with a daily market trader drill-down."
    )
    st.subheader(f"Largest bets on {selected_date_sql}")
    st.caption(
        "Each row is an individual trade ranked by its USDC size, sourced from the warehouse trade fact."
    )

    if markets_df.empty:
        st.warning("No trade rows were found for the selected day.")
        return

    metric_col_1, metric_col_2, metric_col_3 = st.columns(3)
    metric_col_1.metric(
        "Visible bet volume", f"${float(markets_df['usdc_volume'].sum()):,.2f}"
    )
    metric_col_2.metric("Visible bets", f"{len(markets_df):,}")
    metric_col_3.metric("Visible markets", f"{markets_df['condition_id'].nunique():,}")

    table = st.dataframe(
        markets_df[
            [
                "market_label",
                "usdc_volume",
                "proxy_wallet",
                "trade_side",
                "outcome_name",
                "price",
                "size",
                "trade_ts",
            ]
        ],
        hide_index=True,
        width="stretch",
        on_select="rerun",
        selection_mode="single-row",
        column_config={
            "market_label": st.column_config.TextColumn("Market"),
            "usdc_volume": st.column_config.NumberColumn(
                "Bet size (USDC)", format="$%.2f"
            ),
            "proxy_wallet": st.column_config.TextColumn("Proxy wallet"),
            "trade_side": st.column_config.TextColumn("Side"),
            "outcome_name": st.column_config.TextColumn("Outcome"),
            "price": st.column_config.NumberColumn("Price", format="%.4f"),
            "size": st.column_config.NumberColumn("Contracts", format="%.6f"),
            "trade_ts": st.column_config.DatetimeColumn("Trade time"),
        },
        key="largest_bets_table",
    )
    selected_market = selected_row_or_default(
        markets_df, table, "transaction_hash", "largest_bets_table::selected"
    )
    if selected_market is None:
        return

    render_horizontal_bar_chart(
        markets_df,
        label_column="market_label",
        value_column="usdc_volume",
        label_title="Market",
        value_title="Bet size (USDC)",
    )

    top_traders_df = fetch_top_traders_daily(
        selected_market["condition_id"], trader_limit, selected_date_sql
    )
    render_market_trader_drilldown(
        selected_market=selected_market,
        top_traders_df=top_traders_df,
        time_scope=f"on {selected_date_sql}",
        selected_value_label="Selected bet size",
        selected_value_text=f"${float(selected_market['usdc_volume']):,.2f}",
    )


def render_wallet_daily_question(
    question_name: str,
    selected_date_sql: str,
    trader_limit: int,
    market_limit: int,
    history_days: int,
) -> None:
    ranking_mode = {
        QUESTION_TOP_TRADERS_TODAY: "total_usdc_volume",
        QUESTION_MOST_ACTIVE_TRADERS: "trade_count",
    }[question_name]
    traders_df = fetch_wallet_daily_rankings(selected_date_sql, trader_limit, ranking_mode)

    if question_name == QUESTION_TOP_TRADERS_TODAY:
        st.caption(
            "Daily trader rankings from `ANALYTICS.FACT_WALLET_DAILY`, grouped by wallet."
        )
        st.subheader(f"Top traders on {selected_date_sql}")
        chart_title = "Total volume (USDC)"
        question_caption = (
            "This answers who moved the most dollars, not who placed the most trades."
        )
    else:
        st.caption(
            "Daily activity rankings from `ANALYTICS.FACT_WALLET_DAILY`, grouped by wallet."
        )
        st.subheader(f"Most active traders on {selected_date_sql}")
        chart_title = "Trades"
        question_caption = (
            "This separates frequency from volume, which is useful when comparing trader styles."
        )
    st.caption(question_caption)

    if traders_df.empty:
        st.warning("No daily trader rows were found for the selected day.")
        return

    metric_col_1, metric_col_2, metric_col_3 = st.columns(3)
    metric_col_1.metric(
        "Visible trader volume", f"${float(traders_df['total_usdc_volume'].sum()):,.2f}"
    )
    metric_col_2.metric("Visible traders", f"{len(traders_df):,}")
    metric_col_3.metric(
        "Visible trades", f"{int(traders_df['trade_count'].sum()):,}"
    )

    columns = ["proxy_wallet"]
    if ranking_mode != "total_usdc_volume":
        columns.append(ranking_mode)
    columns.extend(
        [
            "total_usdc_volume",
            "trade_count",
            "distinct_markets",
            "avg_trade_size",
            "max_trade_size",
        ]
    )
    table = st.dataframe(
        traders_df[unique_columns(columns)],
        hide_index=True,
        width="stretch",
        on_select="rerun",
        selection_mode="single-row",
        column_config={
            "proxy_wallet": st.column_config.TextColumn("Proxy wallet"),
            "total_usdc_volume": st.column_config.NumberColumn(
                "Total volume (USDC)", format="$%.2f"
            ),
            "trade_count": st.column_config.NumberColumn("Trades", format="%d"),
            "distinct_markets": st.column_config.NumberColumn(
                "Markets", format="%d"
            ),
            "avg_trade_size": st.column_config.NumberColumn(
                "Average trade (USDC)", format="$%.2f"
            ),
            "max_trade_size": st.column_config.NumberColumn(
                "Largest trade (USDC)", format="$%.2f"
            ),
            ranking_mode: st.column_config.NumberColumn(
                chart_title, format="$%.2f" if ranking_mode == "total_usdc_volume" else "%d"
            ),
        },
        key=f"wallet_daily::{ranking_mode}",
    )
    selected_trader = selected_row_or_default(
        traders_df,
        table,
        "proxy_wallet",
        f"wallet_daily::{ranking_mode}::selected",
    )
    if selected_trader is None:
        return

    render_horizontal_bar_chart(
        traders_df,
        label_column="proxy_wallet",
        value_column=ranking_mode,
        label_title="Proxy wallet",
        value_title=chart_title,
    )
    render_trader_profile_section(
        selected_trader["proxy_wallet"],
        market_limit=market_limit,
        history_days=history_days,
        recent_trade_limit=20,
    )


def render_all_time_trader_question(
    question_name: str,
    trader_limit: int,
    market_limit: int,
    history_days: int,
    min_total_trades: int,
) -> None:
    ranking_mode = {
        QUESTION_BIGGEST_TRADERS: "total_volume",
        QUESTION_TRADERS_AVG_SIZE: "avg_trade_size",
        QUESTION_MOST_DIVERSIFIED: "distinct_markets",
    }[question_name]
    traders_df = fetch_dim_trader_rankings(
        trader_limit=trader_limit,
        ranking_mode=ranking_mode,
        min_total_trades=min_total_trades if ranking_mode == "avg_trade_size" else 1,
    )

    headers = {
        QUESTION_BIGGEST_TRADERS: (
            "Who are the biggest traders overall?",
            "Total volume (USDC)",
            "This is the cleanest all-time wallet ranking built from the trader dimension.",
        ),
        QUESTION_TRADERS_AVG_SIZE: (
            "Largest traders by average trade size",
            "Average trade size (USDC)",
            "This is useful for discussing trading style rather than just raw scale.",
        ),
        QUESTION_MOST_DIVERSIFIED: (
            "Most diversified traders",
            "Distinct markets",
            "This supports the proposal question about how differently users trade across markets.",
        ),
    }
    title, chart_title, caption = headers[question_name]

    st.caption("All-time trader analytics from `ANALYTICS.DIM_TRADERS`.")
    st.subheader(title)
    st.caption(caption)

    if traders_df.empty:
        st.warning("No trader rows were found for the selected controls.")
        return

    metric_col_1, metric_col_2, metric_col_3 = st.columns(3)
    metric_col_1.metric("Visible traders", f"{len(traders_df):,}")
    metric_col_2.metric(
        "Visible total volume", f"${float(traders_df['total_volume'].sum()):,.2f}"
    )
    metric_col_3.metric(
        "Visible total trades", f"{int(traders_df['total_trades'].sum()):,}"
    )

    columns = ["proxy_wallet", "user_name"]
    if ranking_mode != "total_volume":
        columns.append(ranking_mode)
    columns.extend(
        [
            "total_volume",
            "total_trades",
            "distinct_markets",
            "avg_trade_size",
            "max_single_trade",
        ]
    )
    table = st.dataframe(
        traders_df[unique_columns(columns)],
        hide_index=True,
        width="stretch",
        on_select="rerun",
        selection_mode="single-row",
        column_config={
            "proxy_wallet": st.column_config.TextColumn("Proxy wallet"),
            "user_name": st.column_config.TextColumn("User name"),
            "total_volume": st.column_config.NumberColumn(
                "Total volume (USDC)", format="$%.2f"
            ),
            "total_trades": st.column_config.NumberColumn("Trades", format="%d"),
            "distinct_markets": st.column_config.NumberColumn(
                "Markets", format="%d"
            ),
            "avg_trade_size": st.column_config.NumberColumn(
                "Average trade (USDC)", format="$%.2f"
            ),
            "max_single_trade": st.column_config.NumberColumn(
                "Largest trade (USDC)", format="$%.2f"
            ),
            ranking_mode: st.column_config.NumberColumn(
                chart_title,
                format="$%.2f"
                if ranking_mode in {"total_volume", "avg_trade_size"}
                else "%d",
            ),
        },
        key=f"dim_traders::{ranking_mode}",
    )
    selected_trader = selected_row_or_default(
        traders_df,
        table,
        "proxy_wallet",
        f"dim_traders::{ranking_mode}::selected",
    )
    if selected_trader is None:
        return

    render_horizontal_bar_chart(
        traders_df,
        label_column="proxy_wallet",
        value_column=ranking_mode,
        label_title="Proxy wallet",
        value_title=chart_title,
    )
    render_trader_profile_section(
        selected_trader["proxy_wallet"],
        market_limit=market_limit,
        history_days=history_days,
        recent_trade_limit=20,
    )


def render_daily_volume_question(
    day_limit: int, market_limit: int, trader_limit: int
) -> None:
    days_df = fetch_daily_platform_volume(day_limit)
    st.caption(
        "Precomputed daily platform summary from `ANALYTICS.PLATFORM_DAILY_SUMMARY`."
    )
    st.subheader("How much money was traded each day?")
    st.caption(
        "This is a strong big-data demonstration because a warehouse-scale market fact is compressed into an app-facing daily summary mart."
    )

    if days_df.empty:
        st.warning("No daily platform volume rows were found.")
        return

    metric_col_1, metric_col_2, metric_col_3 = st.columns(3)
    metric_col_1.metric(
        "Visible traded volume", f"${float(days_df['total_volume_usdc'].sum()):,.2f}"
    )
    metric_col_2.metric(
        "Peak day volume", f"${float(days_df['total_volume_usdc'].max()):,.2f}"
    )
    metric_col_3.metric("Visible days", f"{len(days_df):,}")

    days_display_df = days_df.sort_values("trade_date", ascending=False)
    table = st.dataframe(
        days_display_df,
        hide_index=True,
        width="stretch",
        on_select="rerun",
        selection_mode="single-row",
        column_config={
            "trade_date": st.column_config.DateColumn("Date"),
            "total_volume_usdc": st.column_config.NumberColumn(
                "Volume (USDC)", format="$%.2f"
            ),
            "trade_count": st.column_config.NumberColumn("Trades", format="%d"),
            "markets_with_trades": st.column_config.NumberColumn("Markets", format="%d"),
            "active_traders": st.column_config.NumberColumn(
                "Active traders", format="%d"
            ),
            "avg_volume_per_active_trader": st.column_config.NumberColumn(
                "Avg volume per trader", format="$%.2f"
            ),
            "avg_trades_per_active_trader": st.column_config.NumberColumn(
                "Avg trades per trader", format="%.2f"
            ),
        },
        key="daily_platform_volume_table",
    )
    selected_day = selected_row_or_default(
        days_display_df, table, "trade_date", "daily_platform_volume_table::selected"
    )
    if selected_day is None:
        return

    render_time_series_chart(
        days_df,
        date_column="trade_date",
        value_column="total_volume_usdc",
        date_title="Date",
        value_title="Total volume (USDC)",
    )

    selected_date_sql = pd.Timestamp(selected_day["trade_date"]).date().isoformat()
    st.divider()
    st.subheader(f"Top markets on {selected_date_sql}")
    selected_day_markets = fetch_tracked_markets_for_day(selected_date_sql, market_limit)
    if selected_day_markets.empty:
        st.info("No market rows were found for the selected day.")
        return

    table = st.dataframe(
        selected_day_markets[
            [
                "market_label",
                "total_volume_usdc",
                "trade_count",
                "unique_traders",
                "first_trade_ts",
                "last_trade_ts",
            ]
        ],
        hide_index=True,
        width="stretch",
        on_select="rerun",
        selection_mode="single-row",
        column_config={
            "market_label": st.column_config.TextColumn("Market"),
            "total_volume_usdc": st.column_config.NumberColumn(
                "Volume (USDC)", format="$%.2f"
            ),
            "trade_count": st.column_config.NumberColumn("Trades", format="%d"),
            "unique_traders": st.column_config.NumberColumn(
                "Unique traders", format="%d"
            ),
            "first_trade_ts": st.column_config.DatetimeColumn("First trade"),
            "last_trade_ts": st.column_config.DatetimeColumn("Last trade"),
        },
        key="daily_platform_markets_table",
    )
    selected_market = selected_row_or_default(
        selected_day_markets,
        table,
        "condition_id",
        "daily_platform_markets_table::selected",
    )
    if selected_market is None:
        return

    top_traders_df = fetch_top_traders_daily(
        selected_market["condition_id"], trader_limit, selected_date_sql
    )
    render_market_trader_drilldown(
        selected_market=selected_market,
        top_traders_df=top_traders_df,
        time_scope=f"on {selected_date_sql}",
        selected_value_label="Selected market volume",
        selected_value_text=f"${float(selected_market['total_volume_usdc']):,.2f}",
    )


def render_theme_daily_question(
    question_name: str,
    selected_date_sql: str,
    theme_limit: int,
) -> None:
    ranking_mode = {
        QUESTION_THEME_VOLUME: "total_volume_usdc",
        QUESTION_THEME_UNIQUE: "unique_traders",
        QUESTION_THEME_AVG_BET: "avg_trade_size",
        QUESTION_THEME_ACTIVITY: "trade_count",
    }[question_name]
    themes_df = fetch_theme_daily_rankings(selected_date_sql, theme_limit, ranking_mode)

    st.caption(
        "Market-theme analytics built from PANTHER market metadata and COYOTE trade activity."
    )
    header, metric_label, metric_format, caption = {
        QUESTION_THEME_VOLUME: (
            f"Top bet themes on {selected_date_sql}",
            "Total volume (USDC)",
            "$%.2f",
            "Themes are normalized from PANTHER `group_item_title` first, then event title as a fallback, and ranked by traded USDC volume.",
        ),
        QUESTION_THEME_UNIQUE: (
            f"Bet themes with the most unique traders on {selected_date_sql}",
            "Unique traders",
            "%d",
            "This highlights which kinds of bets attracted the broadest participation rather than just the largest dollar flow.",
        ),
        QUESTION_THEME_AVG_BET: (
            f"Bet themes with the largest average bet on {selected_date_sql}",
            "Average trade size (USDC)",
            "$%.2f",
            "This contrasts themes dominated by larger average positions against themes with smaller trade sizing.",
        ),
        QUESTION_THEME_ACTIVITY: (
            f"Most active bet themes on {selected_date_sql}",
            "Trades",
            "%d",
            "This emphasizes sheer trading frequency, which can differ meaningfully from both total volume and trader breadth.",
        ),
    }[question_name]
    st.subheader(header)
    st.caption(caption)

    if themes_df.empty:
        st.warning("No market-theme rows were found for the selected day.")
        return

    metric_col_1, metric_col_2, metric_col_3 = st.columns(3)
    metric_col_1.metric(
        "Visible theme volume", f"${float(themes_df['total_volume_usdc'].sum()):,.2f}"
    )
    metric_col_2.metric("Visible themes", f"{len(themes_df):,}")
    metric_col_3.metric(
        "Visible distinct markets", f"{int(themes_df['distinct_markets'].sum()):,}"
    )

    table = st.dataframe(
        themes_df[
            unique_columns(
                [
                    "market_theme",
                    ranking_mode,
                    "total_volume_usdc",
                    "unique_traders",
                    "distinct_markets",
                    "trade_count",
                    "avg_trade_size",
                    "market_theme_source",
                    "market_group_title",
                    "event_title",
                ]
            )
        ],
        hide_index=True,
        width="stretch",
        on_select="rerun",
        selection_mode="single-row",
        column_config={
            "market_theme": st.column_config.TextColumn("Bet theme"),
            ranking_mode: st.column_config.NumberColumn(metric_label, format=metric_format),
            "total_volume_usdc": st.column_config.NumberColumn(
                "Total volume (USDC)", format="$%.2f"
            ),
            "unique_traders": st.column_config.NumberColumn(
                "Unique traders", format="%d"
            ),
            "distinct_markets": st.column_config.NumberColumn(
                "Markets", format="%d"
            ),
            "trade_count": st.column_config.NumberColumn("Trades", format="%d"),
            "avg_trade_size": st.column_config.NumberColumn(
                "Average trade (USDC)", format="$%.2f"
            ),
            "market_theme_source": st.column_config.TextColumn("Theme source"),
            "market_group_title": st.column_config.TextColumn("Group title"),
            "event_title": st.column_config.TextColumn("Event title"),
        },
        key=f"theme_daily::{ranking_mode}",
    )
    selected_theme = selected_row_or_default(
        themes_df,
        table,
        "market_theme",
        f"theme_daily::{ranking_mode}::selected",
    )
    if selected_theme is None:
        return

    render_horizontal_bar_chart(
        themes_df,
        label_column="market_theme",
        value_column=ranking_mode,
        label_title="Bet theme",
        value_title=metric_label,
    )

    theme_markets_df = fetch_markets_for_theme_day(
        selected_date_sql,
        str(selected_theme["market_theme"]),
        theme_limit,
    )
    st.divider()
    st.subheader(f"Top markets inside theme: {selected_theme['market_theme']}")
    st.caption(
        f"Theme source: {selected_theme.get('market_theme_source', 'unknown')}."
    )
    if theme_markets_df.empty:
        st.info("No market rows were found for the selected theme.")
        return

    st.dataframe(
        theme_markets_df[
            [
                "market_label",
                "total_volume_usdc",
                "unique_traders",
                "trade_count",
                "avg_trade_size",
                "event_title",
                "market_group_title",
            ]
        ],
        hide_index=True,
        width="stretch",
        column_config={
            "market_label": st.column_config.TextColumn("Market"),
            "total_volume_usdc": st.column_config.NumberColumn(
                "Total volume (USDC)", format="$%.2f"
            ),
            "unique_traders": st.column_config.NumberColumn(
                "Unique traders", format="%d"
            ),
            "trade_count": st.column_config.NumberColumn("Trades", format="%d"),
            "avg_trade_size": st.column_config.NumberColumn(
                "Average trade (USDC)", format="$%.2f"
            ),
            "event_title": st.column_config.TextColumn("Event title"),
            "market_group_title": st.column_config.TextColumn("Group title"),
        },
    )


def render_big_vs_small_question(min_total_trades: int) -> None:
    comparison_df = fetch_big_vs_small_trader_segments(min_total_trades)
    st.caption(
        "Segment comparison from `ANALYTICS.TRADER_SEGMENT_SNAPSHOT`."
    )
    st.subheader("What separates big traders from small traders?")
    st.caption(
        "This compares the top decile of traders by lifetime total volume against the remaining traders."
    )

    if comparison_df.empty or len(comparison_df) < 2:
        st.warning("There was not enough trader data to build the segment comparison.")
        return

    st.dataframe(
        comparison_df,
        hide_index=True,
        width="stretch",
        column_config={
            "segment": st.column_config.TextColumn("Segment"),
            "trader_count": st.column_config.NumberColumn("Traders", format="%d"),
            "avg_total_volume": st.column_config.NumberColumn(
                "Avg total volume", format="$%.2f"
            ),
            "avg_total_trades": st.column_config.NumberColumn(
                "Avg total trades", format="%.2f"
            ),
            "avg_trade_size": st.column_config.NumberColumn(
                "Avg trade size", format="$%.2f"
            ),
            "avg_distinct_markets": st.column_config.NumberColumn(
                "Avg distinct markets", format="%.2f"
            ),
            "avg_anomaly_involvement": st.column_config.NumberColumn(
                "Avg anomaly involvement", format="%.2f"
            ),
        },
    )
    render_grouped_metric_chart(
        comparison_df,
        category_column="segment",
        value_columns=[
            "avg_total_trades",
            "avg_trade_size",
            "avg_distinct_markets",
            "avg_anomaly_involvement",
        ],
    )


def render_trader_cohort_question(history_days: int) -> None:
    month_limit = max(6, history_days // 30)
    cohort_df = fetch_trader_cohort_monthly(month_limit)
    st.caption(
        "Cohort analytics from `ANALYTICS.TRADER_COHORT_MONTHLY`."
    )
    st.subheader("Wallet lifecycle / trader cohorts")
    st.caption(
        "This compresses large-scale wallet activity into cohort-month behavior, which is useful for presenting retention and lifecycle patterns."
    )

    if cohort_df.empty:
        st.warning("No cohort rows were found in the analytics layer.")
        return

    metric_col_1, metric_col_2, metric_col_3 = st.columns(3)
    metric_col_1.metric("Visible cohort rows", f"{len(cohort_df):,}")
    metric_col_2.metric(
        "Distinct cohorts", f"{cohort_df['cohort_month'].nunique():,}"
    )
    metric_col_3.metric(
        "Max months tracked", f"{int(cohort_df['months_since_first_seen'].max()):,}"
    )

    st.dataframe(
        cohort_df.sort_values(["cohort_month", "months_since_first_seen"], ascending=[False, True]),
        hide_index=True,
        width="stretch",
        column_config={
            "cohort_month": st.column_config.DatetimeColumn("Cohort month"),
            "activity_month": st.column_config.DatetimeColumn("Activity month"),
            "months_since_first_seen": st.column_config.NumberColumn(
                "Months since first trade", format="%d"
            ),
            "active_traders": st.column_config.NumberColumn(
                "Active traders", format="%d"
            ),
            "total_volume_usdc": st.column_config.NumberColumn(
                "Total volume (USDC)", format="$%.2f"
            ),
            "total_trades": st.column_config.NumberColumn(
                "Total trades", format="%d"
            ),
            "avg_volume_per_active_trader": st.column_config.NumberColumn(
                "Avg volume per active trader", format="$%.2f"
            ),
            "avg_trades_per_active_trader": st.column_config.NumberColumn(
                "Avg trades per active trader", format="%.2f"
            ),
            "avg_active_days": st.column_config.NumberColumn(
                "Avg active days", format="%.2f"
            ),
        },
    )
    render_cohort_heatmap(cohort_df)


def render_trader_profile_lookup(
    market_limit: int,
    history_days: int,
    recent_trade_limit: int,
) -> None:
    st.caption(
        "Trader-level lookup backed by `ANALYTICS.DIM_TRADERS`, `ANALYTICS.FACT_WALLET_DAILY`, and `ANALYTICS.FACT_USER_ACTIVITY_TRADES`."
    )
    st.subheader("Trader profile lookup")
    st.caption(
        "This directly supports the proposal goal of explaining how a specific user trades on Polymarket."
    )

    candidate_df = fetch_dim_trader_rankings(100, "total_volume", 1)
    candidate_options = candidate_df["proxy_wallet"].tolist()
    selected_wallet = st.text_input("Wallet address")
    fallback_wallet = st.selectbox(
        "Or select a top trader",
        options=candidate_options,
        index=0 if candidate_options else None,
    )
    trader_wallet = selected_wallet.strip() or fallback_wallet

    if not trader_wallet:
        st.info("Enter a wallet address or select a trader to continue.")
        return

    render_trader_profile_section(
        trader_wallet,
        market_limit=market_limit,
        history_days=history_days,
        recent_trade_limit=recent_trade_limit,
    )

def render_trade_history_question(market_limit: int, trade_limit: int) -> None:
    st.subheader(QUESTION_TRADE_HISTORY)

    markets_df = fetch_highest_volume_markets(market_limit, open_only=False)

    if markets_df.empty:
        st.warning("No markets found to display.")
        return

    table = st.dataframe(
        markets_df[["market_label", "total_volume_usdc", "active", "closed"]],
        hide_index=True,
        width="stretch",
        on_select="rerun",
        selection_mode="single-row",
        column_config={
            "market_label": st.column_config.TextColumn("Market"),
            "total_volume_usdc": st.column_config.NumberColumn("Listed volume (USDC)", format="$%.2f"),
        },
        key="trade_history_market_selection",
    )

    selected_market = selected_row_or_default(
        markets_df, table, "condition_id", "trade_history_market_selection::selected"
    )

    if selected_market is None:
        return

    st.divider()
    st.subheader(f"Recent Trades: {selected_market['market_label']}")

    trades_df = fetch_trade_history(selected_market["condition_id"], trade_limit)

    if trades_df.empty:
        st.info("No trades found for this market in the activity table.")
        return

    
    def color_side(val):
        if pd.isna(val):
            return ''
        color = 'rgba(39, 201, 115, 0.2)' if str(val).upper() == 'BUY' else 'rgba(255, 104, 113, 0.2)'
        return f'background-color: {color}'

    styled_df = trades_df.style.map(color_side, subset=['trade_side'])

    st.dataframe(
        styled_df,
        hide_index=True,
        width="stretch",
        column_config={
            "trade_ts": st.column_config.DatetimeColumn("Time"),
            "trader_identity": st.column_config.TextColumn("Trader"),
            "trade_side": st.column_config.TextColumn("Side"),
            "outcome_name": st.column_config.TextColumn("Outcome"),
            "price": st.column_config.NumberColumn("Price", format="%.4f"),
            "size": st.column_config.NumberColumn("Shares", format="%.2f"),
            "usdc_volume": st.column_config.NumberColumn("Total Value", format="$%.2f"),
            "transaction_hash": st.column_config.TextColumn("Tx Hash"),
        }
    )

def render_data_analysis_page() -> None:
    st.title("Polymetrics")

    try:
        app_config = load_app_config()
        today = get_today_for_timezone()
    except Exception as exc:  # pragma: no cover
        st.error(
            "The app could not initialize the Snowflake session. "
            "Check `.streamlit/secrets.toml`, your Snowflake access, and whether "
            "the analytics build has been run."
        )
        st.exception(exc)
        return

    with st.sidebar:
        st.header("Controls")
        question_name = st.selectbox(
            "Question to answer",
            options=[
                QUESTION_TRACKED_TODAY,
                QUESTION_TRADE_HISTORY,
                QUESTION_MARKETS_UNIQUE,
                QUESTION_MARKETS_AVG_BET,
                QUESTION_WHALE_DOMINATED,
                QUESTION_HIGHEST_VOLUME,
                QUESTION_LARGEST_BETS,
                QUESTION_TOP_TRADERS_TODAY,
                QUESTION_MOST_ACTIVE_TRADERS,
                QUESTION_BIGGEST_TRADERS,
                QUESTION_TRADERS_AVG_SIZE,
                QUESTION_MOST_DIVERSIFIED,
                QUESTION_THEME_VOLUME,
                QUESTION_THEME_UNIQUE,
                QUESTION_THEME_AVG_BET,
                QUESTION_THEME_ACTIVITY,
                QUESTION_DAILY_VOLUME,
                QUESTION_BIG_VS_SMALL,
                QUESTION_TRADER_PROFILE,
                QUESTION_TRADER_COHORTS,
            ],
        )

        selected_date = today
        if question_name in QUESTIONS_WITH_DATE:
            selected_date = st.date_input(
                "Analysis day",
                value=today,
                help=(
                    "Defaults to the current day in the configured Snowflake session "
                    f"timezone: {app_config.timezone}."
                ),
            )

        market_limit = min(app_config.default_market_limit, 25)
        trader_limit = min(app_config.default_trader_limit, 50)
        history_days = 60
        recent_trade_limit = 20
        min_total_trades = 10

        if question_name in QUESTIONS_WITH_MARKET_LIMIT:
            market_limit = st.slider(
                "Rows to show",
                min_value=5,
                max_value=25,
                value=market_limit,
                step=1,
            )
        if question_name in QUESTIONS_WITH_TRADER_LIMIT:
            trader_limit = st.slider(
                "Top traders to show",
                min_value=5,
                max_value=50,
                value=trader_limit,
                step=1,
            )
        if question_name in QUESTIONS_WITH_HISTORY_DAYS:
            history_days = st.slider(
                "History window (days)",
                min_value=14,
                max_value=180,
                value=history_days,
                step=1,
            )
        if question_name in QUESTIONS_WITH_RECENT_TRADES:
            recent_trade_limit = st.slider(
                "Recent trades to show",
                min_value=10,
                max_value=50,
                value=recent_trade_limit,
                step=5,
            )
        if question_name in QUESTIONS_WITH_MIN_TOTAL_TRADES:
            min_total_trades = st.slider(
                "Minimum lifetime trades",
                min_value=1,
                max_value=100,
                value=min_total_trades,
                step=1,
            )
        open_only = False
        if question_name == QUESTION_HIGHEST_VOLUME:
            open_only = st.checkbox("Open markets only", value=True)

        if st.button("Refresh data"):
            st.cache_data.clear()
            st.rerun()

    selected_date_sql = selected_date.isoformat()

    if question_name in DAILY_MARKET_QUESTIONS:
        render_daily_market_question(
            question_name, selected_date_sql, market_limit, trader_limit
        )
    elif question_name == QUESTION_HIGHEST_VOLUME:
        render_highest_volume_question(market_limit, trader_limit, open_only)
    elif question_name == QUESTION_LARGEST_BETS:
        render_largest_bets_question(selected_date_sql, market_limit, trader_limit)
    elif question_name in {QUESTION_TOP_TRADERS_TODAY, QUESTION_MOST_ACTIVE_TRADERS}:
        render_wallet_daily_question(
            question_name,
            selected_date_sql,
            trader_limit,
            market_limit,
            history_days,
        )
    elif question_name in {
        QUESTION_BIGGEST_TRADERS,
        QUESTION_TRADERS_AVG_SIZE,
        QUESTION_MOST_DIVERSIFIED,
    }:
        render_all_time_trader_question(
            question_name,
            trader_limit,
            market_limit,
            history_days,
            min_total_trades,
        )
    elif question_name == QUESTION_DAILY_VOLUME:
        render_daily_volume_question(history_days, market_limit, trader_limit)
    elif question_name in {
        QUESTION_THEME_VOLUME,
        QUESTION_THEME_UNIQUE,
        QUESTION_THEME_AVG_BET,
        QUESTION_THEME_ACTIVITY,
    }:
        render_theme_daily_question(question_name, selected_date_sql, market_limit)
    elif question_name == QUESTION_BIG_VS_SMALL:
        render_big_vs_small_question(min_total_trades)
    elif question_name == QUESTION_TRADER_PROFILE:
        render_trader_profile_lookup(market_limit, history_days, recent_trade_limit)
    elif question_name == QUESTION_TRADER_COHORTS:
        render_trader_cohort_question(history_days)
    elif question_name == QUESTION_TRADE_HISTORY:
        render_trade_history_question(market_limit, recent_trade_limit)


def main() -> None:
    st.set_page_config(page_title="Polymetrics", layout="wide")
    navigation = st.navigation(
        [
            st.Page(render_data_analysis_page, title="Data Analysis", default=True),
            st.Page(
                Path(__file__).parent / "pages" / "2_Data_Availability.py",
                title="Data Availability",
            ),
            st.Page(
                Path(__file__).parent / "pages" / "3_Anomaly_Detection.py",
                title="Anomaly Detection",
            ),
        ]
    )
    navigation.run()


if __name__ == "__main__":
    main()
