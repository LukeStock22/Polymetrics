from __future__ import annotations

from dataclasses import dataclass
from datetime import date
import re

import pandas as pd
import streamlit as st
from snowflake.snowpark import Session


IDENTIFIER_PATTERN = re.compile(r'^[A-Za-z0-9_."$]+$')
TIMEZONE_PATTERN = re.compile(r"^[A-Za-z0-9_/\-+]+$")

QUESTION_TRACKED_TODAY = "Most Popular Markets Today"
QUESTION_HIGHEST_VOLUME = "Highest Volume Markets"


@dataclass(frozen=True)
class AppConfig:
    user_activity_table: str
    markets_table: str
    timezone: str
    default_market_limit: int
    default_trader_limit: int


def validate_identifier(value: str, label: str) -> str:
    if not value or not IDENTIFIER_PATTERN.fullmatch(value):
        raise ValueError(
            f"{label} must be an unquoted Snowflake identifier such as "
            "CURATED_POLYMARKET_USER_ACTIVITY or DB.SCHEMA.TABLE."
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
        timezone=validate_timezone(app_settings.get("timezone", "America/Chicago")),
        default_market_limit=max(5, int(app_settings.get("default_market_limit", 10))),
        default_trader_limit=max(5, int(app_settings.get("default_trader_limit", 15))),
    )


@st.cache_resource
def get_session() -> Session:
    connection_parameters = dict(st.secrets["snowflake"])
    app_config = load_app_config()
    session = Session.builder.configs(connection_parameters).create()
    session.sql(
        f"ALTER SESSION SET TIMEZONE = '{sql_string_literal(app_config.timezone)}'"
    ).collect()
    return session


@st.cache_data(ttl=300, show_spinner=False)
def get_today_for_timezone() -> date:
    session = get_session()
    current_day = session.sql("SELECT CURRENT_DATE() AS TODAY").to_pandas()
    return current_day.iloc[0]["TODAY"]


@st.cache_data(ttl=300, show_spinner=False)
def fetch_tracked_markets_for_day(selected_date: str, market_limit: int) -> pd.DataFrame:
    app_config = load_app_config()
    session = get_session()
    query = f"""
        WITH markets_dedup AS (
            SELECT
                market_id,
                condition_id,
                question,
                slug,
                active,
                closed,
                updated_at,
                transformed_at
            FROM {app_config.markets_table}
            WHERE condition_id IS NOT NULL
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY condition_id
                ORDER BY updated_at DESC NULLS LAST, transformed_at DESC NULLS LAST, market_id
            ) = 1
        ),
        trades_for_day AS (
            SELECT
                TO_TIMESTAMP_NTZ(timestamp) AS trade_ts,
                proxyWallet AS proxy_wallet,
                conditionId AS condition_id,
                TRY_TO_DOUBLE(usdcSize) AS usdc_size
            FROM {app_config.user_activity_table}
            WHERE type = 'TRADE'
              AND conditionId IS NOT NULL
              AND TO_DATE(TO_TIMESTAMP_NTZ(timestamp)) = '{selected_date}'
        )
        SELECT
            markets_dedup.market_id,
            markets_dedup.condition_id,
            COALESCE(markets_dedup.question, markets_dedup.slug, markets_dedup.market_id) AS question,
            COALESCE(markets_dedup.slug, markets_dedup.question, markets_dedup.market_id) AS market_label,
            COALESCE(markets_dedup.active, FALSE) AS active,
            COALESCE(markets_dedup.closed, FALSE) AS closed,
            COUNT(*) AS trade_count,
            COUNT(DISTINCT trades_for_day.proxy_wallet) AS unique_traders,
            ROUND(SUM(COALESCE(trades_for_day.usdc_size, 0)), 2) AS total_volume_usdc,
            MIN(trades_for_day.trade_ts) AS first_trade_ts,
            MAX(trades_for_day.trade_ts) AS last_trade_ts
        FROM trades_for_day
        JOIN markets_dedup
          ON trades_for_day.condition_id = markets_dedup.condition_id
        GROUP BY 1, 2, 3, 4, 5, 6
        ORDER BY total_volume_usdc DESC, trade_count DESC, market_label ASC
        LIMIT {market_limit}
    """
    data = session.sql(query).to_pandas()
    if data.empty:
        return data
    return data.rename(
        columns={
            "MARKET_ID": "market_id",
            "CONDITION_ID": "condition_id",
            "QUESTION": "question",
            "MARKET_LABEL": "market_label",
            "ACTIVE": "active",
            "CLOSED": "closed",
            "TRADE_COUNT": "trade_count",
            "UNIQUE_TRADERS": "unique_traders",
            "TOTAL_VOLUME_USDC": "total_volume_usdc",
            "FIRST_TRADE_TS": "first_trade_ts",
            "LAST_TRADE_TS": "last_trade_ts",
        }
    )


@st.cache_data(ttl=300, show_spinner=False)
def fetch_highest_volume_markets(market_limit: int, active_only: bool) -> pd.DataFrame:
    app_config = load_app_config()
    session = get_session()
    active_filter = "AND COALESCE(active, FALSE) = TRUE" if active_only else ""
    query = f"""
        WITH markets_dedup AS (
            SELECT
                market_id,
                condition_id,
                question,
                slug,
                active,
                closed,
                end_date,
                updated_at,
                transformed_at,
                COALESCE(volume_num, volume, volume_clob, 0) AS total_volume_usdc
            FROM {app_config.markets_table}
            WHERE condition_id IS NOT NULL
              {active_filter}
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY condition_id
                ORDER BY updated_at DESC NULLS LAST, transformed_at DESC NULLS LAST, market_id
            ) = 1
        )
        SELECT
            market_id,
            condition_id,
            COALESCE(question, slug, market_id) AS question,
            COALESCE(slug, question, market_id) AS market_label,
            COALESCE(active, FALSE) AS active,
            COALESCE(closed, FALSE) AS closed,
            end_date,
            ROUND(COALESCE(total_volume_usdc, 0), 2) AS total_volume_usdc
        FROM markets_dedup
        ORDER BY total_volume_usdc DESC, market_label ASC
        LIMIT {market_limit}
    """
    data = session.sql(query).to_pandas()
    if data.empty:
        return data
    return data.rename(
        columns={
            "MARKET_ID": "market_id",
            "CONDITION_ID": "condition_id",
            "QUESTION": "question",
            "MARKET_LABEL": "market_label",
            "ACTIVE": "active",
            "CLOSED": "closed",
            "END_DATE": "end_date",
            "TOTAL_VOLUME_USDC": "total_volume_usdc",
        }
    )


@st.cache_data(ttl=300, show_spinner=False)
def fetch_top_traders(
    condition_id: str, trader_limit: int, selected_date: str | None
) -> pd.DataFrame:
    app_config = load_app_config()
    session = get_session()
    escaped_condition_id = sql_string_literal(condition_id)
    date_filter = ""
    if selected_date is not None:
        date_filter = (
            f"AND TO_DATE(TO_TIMESTAMP_NTZ(timestamp)) = '{selected_date}'"
        )

    query = f"""
        WITH market_trades AS (
            SELECT
                proxyWallet AS proxy_wallet,
                TRY_TO_DOUBLE(usdcSize) AS usdc_size
            FROM {app_config.user_activity_table}
            WHERE type = 'TRADE'
              AND conditionId = '{escaped_condition_id}'
              {date_filter}
        )
        SELECT
            proxy_wallet,
            ROUND(SUM(COALESCE(usdc_size, 0)), 2) AS total_volume_usdc,
            COUNT(*) AS trade_count,
            ROUND(AVG(COALESCE(usdc_size, 0)), 2) AS average_trade_usdc,
            ROUND(MAX(COALESCE(usdc_size, 0)), 2) AS largest_trade_usdc
        FROM market_trades
        GROUP BY 1
        ORDER BY total_volume_usdc DESC, trade_count DESC, proxy_wallet ASC
        LIMIT {trader_limit}
    """
    data = session.sql(query).to_pandas()
    if data.empty:
        return data
    return data.rename(
        columns={
            "PROXY_WALLET": "proxy_wallet",
            "TOTAL_VOLUME_USDC": "total_volume_usdc",
            "TRADE_COUNT": "trade_count",
            "AVERAGE_TRADE_USDC": "average_trade_usdc",
            "LARGEST_TRADE_USDC": "largest_trade_usdc",
        }
    )


def render_summary(question_name: str, markets_df: pd.DataFrame) -> None:
    metric_col_1, metric_col_2, metric_col_3 = st.columns(3)
    total_volume = float(markets_df["total_volume_usdc"].sum())

    if question_name == QUESTION_TRACKED_TODAY:
        metric_col_1.metric("Visible tracked trade volume", f"${total_volume:,.2f}")
        metric_col_2.metric("Visible trades", f"{int(markets_df['trade_count'].sum()):,}")
        metric_col_3.metric(
            "Visible unique traders", f"{int(markets_df['unique_traders'].sum()):,}"
        )
        return

    active_count = int(markets_df["active"].fillna(False).sum())
    metric_col_1.metric("Visible listed volume", f"${total_volume:,.2f}")
    metric_col_2.metric("Visible markets", f"{len(markets_df):,}")
    metric_col_3.metric("Visible active markets", f"{active_count:,}")


def render_market_table(question_name: str, markets_df: pd.DataFrame):
    if question_name == QUESTION_TRACKED_TODAY:
        return st.dataframe(
            markets_df[
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
            use_container_width=True,
            on_select="rerun",
            selection_mode="single-row",
            column_config={
                "market_label": st.column_config.TextColumn("Market"),
                "total_volume_usdc": st.column_config.NumberColumn(
                    "Tracked volume (USDC)", format="$%.2f"
                ),
                "trade_count": st.column_config.NumberColumn("Trades", format="%d"),
                "unique_traders": st.column_config.NumberColumn(
                    "Unique traders", format="%d"
                ),
                "first_trade_ts": st.column_config.DatetimeColumn("First trade"),
                "last_trade_ts": st.column_config.DatetimeColumn("Last trade"),
            },
            key="tracked_markets_table",
        )

    return st.dataframe(
        markets_df[
            [
                "market_label",
                "total_volume_usdc",
                "active",
                "closed",
                "end_date",
            ]
        ],
        hide_index=True,
        use_container_width=True,
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


def get_selected_market(
    question_name: str, markets_df: pd.DataFrame, market_selection
) -> pd.Series:
    selection_state_key = f"selected_condition_id::{question_name}"
    selected_market = None
    selected_rows = market_selection.selection.rows

    if selected_rows:
        selected_market = markets_df.iloc[selected_rows[0]]
        st.session_state[selection_state_key] = selected_market["condition_id"]
    elif selection_state_key in st.session_state:
        matches = markets_df[
            markets_df["condition_id"] == st.session_state[selection_state_key]
        ]
        if not matches.empty:
            selected_market = matches.iloc[0]

    if selected_market is None:
        selected_market = markets_df.iloc[0]
        st.session_state[selection_state_key] = selected_market["condition_id"]

    return selected_market


def main() -> None:
    st.set_page_config(page_title="Polymetrics", layout="wide")
    st.title("Polymetrics")

    try:
        app_config = load_app_config()
        today = get_today_for_timezone()
    except Exception as exc:  # pragma: no cover - UI error path
        st.error(
            "The app could not initialize the Snowflake session. "
            "Check `.streamlit/secrets.toml` and your Snowflake access."
        )
        st.exception(exc)
        return

    with st.sidebar:
        st.header("Controls")
        question_name = st.selectbox(
            "Question to answer",
            options=[QUESTION_TRACKED_TODAY, QUESTION_HIGHEST_VOLUME],
        )
        market_limit = st.slider(
            "Markets to show",
            min_value=5,
            max_value=25,
            value=min(app_config.default_market_limit, 25),
            step=1,
        )
        trader_limit = st.slider(
            "Top traders to show",
            min_value=5,
            max_value=50,
            value=min(app_config.default_trader_limit, 50),
            step=1,
        )

        selected_date = today
        active_only = False
        if question_name == QUESTION_TRACKED_TODAY:
            selected_date = st.date_input(
                "Trading day",
                value=today,
                help=(
                    "Defaults to the current day in the configured Snowflake session "
                    f"timezone: {app_config.timezone}."
                ),
            )
        else:
            active_only = st.checkbox("Active markets only", value=True)

        if st.button("Refresh data"):
            st.cache_data.clear()
            st.rerun()

    if question_name == QUESTION_TRACKED_TODAY:
        st.caption(
            "Tracked-wallet trade volume by day from the user-activity table, with a "
            "top-trader drill-down."
        )
        selected_date_sql = selected_date.isoformat()
        markets_df = fetch_tracked_markets_for_day(selected_date_sql, market_limit)
    else:
        st.caption(
            "Highest-volume markets from the curated markets table, ranked by listed "
            "market volume."
        )
        selected_date_sql = None
        markets_df = fetch_highest_volume_markets(market_limit, active_only)

    if markets_df.empty:
        st.warning(
            "No markets were found for the selected controls. Check the table names, "
            "your Snowflake permissions, or the selected filters."
        )
        return

    render_summary(question_name, markets_df)

    if question_name == QUESTION_TRACKED_TODAY:
        st.subheader(f"Top tracked markets on {selected_date_sql}")
        st.caption(
            "These rankings come from the ingested user-activity table rather than "
            "platform-wide Polymarket volume."
        )
    else:
        st.subheader("Highest volume markets")
        st.caption(
            "These rankings come from `GAMMA_MARKETS.volume_num` and can be filtered "
            "to active markets only."
        )

    market_selection = render_market_table(question_name, markets_df)
    selected_market = get_selected_market(question_name, markets_df, market_selection)

    chart_df = markets_df.set_index("market_label")[["total_volume_usdc"]]
    st.bar_chart(chart_df, horizontal=True)

    if question_name == QUESTION_TRACKED_TODAY:
        trader_time_scope = f"on {selected_date_sql}"
    else:
        trader_time_scope = "across tracked history"

    st.subheader("Top traders in the selected market")
    st.write(f"Selected market: **{selected_market['market_label']}**")
    st.caption(f"Top traders are ranked by total traded volume {trader_time_scope}.")

    top_traders_df = fetch_top_traders(
        selected_market["condition_id"], trader_limit, selected_date_sql
    )

    trader_metric_col_1, trader_metric_col_2 = st.columns(2)
    market_volume_label = (
        "Selected market tracked volume"
        if question_name == QUESTION_TRACKED_TODAY
        else "Selected market listed volume"
    )
    trader_metric_col_1.metric(
        market_volume_label, f"${float(selected_market['total_volume_usdc']):,.2f}"
    )
    trader_metric_col_2.metric(
        "Top traders shown", f"{min(len(top_traders_df), trader_limit):,}"
    )

    if top_traders_df.empty:
        st.info("No trader rows were found for the selected market in the tracked data.")
        return

    st.dataframe(
        top_traders_df,
        hide_index=True,
        use_container_width=True,
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

    trader_chart_df = top_traders_df.set_index("proxy_wallet")[["total_volume_usdc"]]
    st.bar_chart(trader_chart_df, horizontal=True)


if __name__ == "__main__":
    main()
