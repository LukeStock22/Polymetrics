from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path
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
    tracked_market_volume_daily_table: str
    highest_volume_markets_table: str
    market_top_traders_daily_table: str
    market_top_traders_all_time_table: str
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
def get_today_for_timezone() -> date:
    session = get_session()
    current_day = session.sql("SELECT CURRENT_DATE() AS TODAY").to_pandas()
    return current_day.iloc[0]["TODAY"]


def rename_columns(df: pd.DataFrame) -> pd.DataFrame:
    return df.rename(columns={column: column.lower() for column in df.columns})


@st.cache_data(ttl=300, show_spinner=False)
def fetch_tracked_markets_for_day(selected_date: str, market_limit: int) -> pd.DataFrame:
    app_config = load_app_config()
    session = get_session()
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
        ORDER BY total_volume_usdc DESC, trade_count DESC, market_label ASC
        LIMIT {market_limit}
    """
    return rename_columns(session.sql(query).to_pandas())


@st.cache_data(ttl=300, show_spinner=False)
def fetch_highest_volume_markets(market_limit: int, active_only: bool) -> pd.DataFrame:
    app_config = load_app_config()
    session = get_session()
    active_filter = "WHERE closed = FALSE" if active_only else ""
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
        {active_filter}
        ORDER BY total_volume_usdc DESC, market_label ASC
        LIMIT {market_limit}
    """
    return rename_columns(session.sql(query).to_pandas())


@st.cache_data(ttl=300, show_spinner=False)
def fetch_top_traders_daily(
    condition_id: str, trader_limit: int, selected_date: str
) -> pd.DataFrame:
    app_config = load_app_config()
    session = get_session()
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
    return rename_columns(session.sql(query).to_pandas())


@st.cache_data(ttl=300, show_spinner=False)
def fetch_top_traders_all_time(condition_id: str, trader_limit: int) -> pd.DataFrame:
    app_config = load_app_config()
    session = get_session()
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
    return rename_columns(session.sql(query).to_pandas())


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

    open_count = int((~markets_df["closed"].fillna(False)).sum())
    metric_col_1.metric("Visible listed volume", f"${total_volume:,.2f}")
    metric_col_2.metric("Visible markets", f"{len(markets_df):,}")
    metric_col_3.metric("Visible open markets", f"{open_count:,}")


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
            width="stretch",
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


def render_data_analysis_page() -> None:
    st.title("Polymetrics")

    try:
        app_config = load_app_config()
        today = get_today_for_timezone()
    except Exception as exc:  # pragma: no cover - UI error path
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
            options=[QUESTION_TRACKED_TODAY, QUESTION_HIGHEST_VOLUME],
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
        if question_name != QUESTION_TRACKED_TODAY:
            active_only = st.checkbox("Open markets only", value=True)

        if st.button("Refresh data"):
            st.cache_data.clear()
            st.rerun()

    if question_name == QUESTION_TRACKED_TODAY:
        st.caption(
            "Warehouse-driven daily tracked-volume rankings from "
            "`ANALYTICS.TRACKED_MARKET_VOLUME_DAILY`."
        )
        selected_date_sql = selected_date.isoformat()
        markets_df = fetch_tracked_markets_for_day(selected_date_sql, market_limit)
    else:
        st.caption(
            "Warehouse-driven listed-volume rankings from "
            "`ANALYTICS.HIGHEST_VOLUME_MARKETS`."
        )
        selected_date_sql = None
        markets_df = fetch_highest_volume_markets(market_limit, active_only)

    if markets_df.empty:
        st.warning(
            "No analytics rows were found for the selected controls. Run the analytics "
            "build script and confirm the analytics tables contain data."
        )
        return

    render_summary(question_name, markets_df)

    if question_name == QUESTION_TRACKED_TODAY:
        st.subheader(f"Top tracked markets on {selected_date_sql}")
        st.caption(
            "This ranking comes from the materialized analytics table rather than a "
            "live join inside the app."
        )
    else:
        st.subheader("Highest volume markets")
        st.caption(
            "This ranking comes from the materialized analytics table built from the "
            "curated markets dataset."
        )

    market_selection = render_market_table(question_name, markets_df)
    selected_market = get_selected_market(question_name, markets_df, market_selection)

    chart_df = markets_df.set_index("market_label")[["total_volume_usdc"]]
    st.bar_chart(chart_df, horizontal=True)

    if question_name == QUESTION_TRACKED_TODAY:
        trader_time_scope = f"on {selected_date_sql}"
        top_traders_df = fetch_top_traders_daily(
            selected_market["condition_id"], trader_limit, selected_date_sql
        )
    else:
        trader_time_scope = "across all tracked history"
        top_traders_df = fetch_top_traders_all_time(
            selected_market["condition_id"], trader_limit
        )

    st.subheader("Top traders in the selected market")
    st.write(f"Selected market: **{selected_market['market_label']}**")
    st.caption(f"Top traders are ranked by total traded volume {trader_time_scope}.")

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

    trader_chart_df = top_traders_df.set_index("proxy_wallet")[["total_volume_usdc"]]
    st.bar_chart(trader_chart_df, horizontal=True)


def main() -> None:
    st.set_page_config(page_title="Polymetrics", layout="wide")
    navigation = st.navigation(
        [
            st.Page(render_data_analysis_page, title="Data Analysis", default=True),
            st.Page(
                Path(__file__).parent / "pages" / "2_Data_Availability.py",
                title="Data Availability",
            ),
        ]
    )
    navigation.run()


if __name__ == "__main__":
    main()
