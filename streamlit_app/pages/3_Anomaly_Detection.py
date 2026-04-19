"""Anomaly Detection dashboard.

Reads from DOG_DB.DOG_SCHEMA.DETECTED_ANOMALIES (populated live by the
Kafka-consumer-with-detectors pipeline on linuxlab). Shows detector
breakdown, hottest markets, time series, and a per-market drill-down
with the triggering trades and detector-specific details.

Table names come from st.secrets["app"]:
  - dog_detected_anomalies_table
  - dog_clob_trades_stream_table
  - markets_table  (for titles)
"""

from __future__ import annotations

from pathlib import Path
import sys

import altair as alt
import pandas as pd
import streamlit as st

APP_DIR = Path(__file__).resolve().parents[1]
if str(APP_DIR) not in sys.path:
    sys.path.insert(0, str(APP_DIR))

from app import run_query_to_pandas, sql_string_literal, validate_identifier


# ---------------- config ----------------

def _get_table(key: str, default: str) -> str:
    app_settings = dict(st.secrets["app"]) if "app" in st.secrets else {}
    return validate_identifier(app_settings.get(key, default), f"app.{key}")


def _tables() -> dict[str, str]:
    return {
        "anomalies": _get_table(
            "dog_detected_anomalies_table",
            "DOG_DB.DOG_SCHEMA.DETECTED_ANOMALIES",
        ),
        "trades": _get_table(
            "dog_clob_trades_stream_table",
            "DOG_DB.DOG_SCHEMA.CLOB_TRADES_STREAM",
        ),
        "markets": _get_table(
            "markets_table",
            "PANTHER_DB.CURATED.GAMMA_MARKETS",
        ),
    }


# ---------------- queries ----------------

@st.cache_data(ttl=60, show_spinner=False)
def fetch_summary() -> pd.DataFrame:
    t = _tables()
    return run_query_to_pandas(f"""
        SELECT
            detector_type,
            COUNT(*)                              AS hits,
            ROUND(AVG(severity_score), 3)         AS avg_severity,
            ROUND(MAX(severity_score), 3)         AS max_severity,
            COUNT(DISTINCT market_condition_id)   AS markets_affected,
            MIN(detected_at)                      AS first_seen,
            MAX(detected_at)                      AS last_seen
        FROM {t['anomalies']}
        GROUP BY detector_type
        ORDER BY hits DESC
    """)


@st.cache_data(ttl=60, show_spinner=False)
def fetch_hourly_timeline(hours: int = 48) -> pd.DataFrame:
    t = _tables()
    return run_query_to_pandas(f"""
        SELECT
            DATE_TRUNC('HOUR', detected_at) AS hour,
            detector_type,
            COUNT(*)                        AS hits
        FROM {t['anomalies']}
        WHERE detected_at >= DATEADD('HOUR', -{int(hours)}, CURRENT_TIMESTAMP())
        GROUP BY 1, 2
        ORDER BY 1
    """)


@st.cache_data(ttl=60, show_spinner=False)
def fetch_hot_markets(limit: int) -> pd.DataFrame:
    t = _tables()
    return run_query_to_pandas(
        f"""
        WITH hits AS (
            SELECT
                market_condition_id,
                COUNT(*)                            AS anomaly_count,
                LISTAGG(DISTINCT detector_type, ', ') WITHIN GROUP (ORDER BY detector_type)
                                                    AS detectors_fired,
                ROUND(AVG(severity_score), 3)       AS avg_severity,
                ROUND(MAX(severity_score), 3)       AS peak_severity,
                MAX(detected_at)                    AS last_anomaly_at
            FROM {t['anomalies']}
            GROUP BY market_condition_id
        )
        SELECT
            h.market_condition_id,
            m.QUESTION       AS market_title,
            h.anomaly_count,
            h.detectors_fired,
            h.avg_severity,
            h.peak_severity,
            h.last_anomaly_at
        FROM hits h
        LEFT JOIN {t['markets']} m
          ON m.CONDITION_ID = h.market_condition_id
        ORDER BY h.anomaly_count DESC
        LIMIT {int(limit)}
        """,
        fallback_query=f"""
        SELECT
            market_condition_id,
            NULL                                   AS market_title,
            COUNT(*)                               AS anomaly_count,
            LISTAGG(DISTINCT detector_type, ', ')
              WITHIN GROUP (ORDER BY detector_type) AS detectors_fired,
            ROUND(AVG(severity_score), 3)          AS avg_severity,
            ROUND(MAX(severity_score), 3)          AS peak_severity,
            MAX(detected_at)                       AS last_anomaly_at
        FROM {t['anomalies']}
        GROUP BY market_condition_id
        ORDER BY anomaly_count DESC
        LIMIT {int(limit)}
        """,
    )


@st.cache_data(ttl=60, show_spinner=False)
def fetch_market_anomalies(condition_id: str, limit: int = 200) -> pd.DataFrame:
    t = _tables()
    escaped = sql_string_literal(condition_id)
    return run_query_to_pandas(f"""
        SELECT
            detected_at,
            detector_type,
            ROUND(severity_score, 3) AS severity,
            asset_id,
            details:z_score::FLOAT        AS z_score,
            details:impact_ratio::FLOAT   AS impact_ratio,
            details:prev_price::FLOAT     AS prev_price,
            details:new_price::FLOAT      AS new_price,
            details:trade_size::FLOAT     AS whale_size,
            details:percentile_rank::FLOAT AS whale_percentile,
            details:short_window_volume::FLOAT AS window_volume,
            details:minutes_until_resolution::FLOAT AS min_to_resolution,
            details AS full_details
        FROM {t['anomalies']}
        WHERE market_condition_id = '{escaped}'
        ORDER BY detected_at DESC
        LIMIT {int(limit)}
    """)


@st.cache_data(ttl=60, show_spinner=False)
def fetch_recent_trades_for_market(condition_id: str, limit: int = 200) -> pd.DataFrame:
    t = _tables()
    escaped = sql_string_literal(condition_id)
    return run_query_to_pandas(f"""
        SELECT
            ws_timestamp,
            side,
            price,
            size,
            asset_id
        FROM {t['trades']}
        WHERE market_condition_id = '{escaped}'
        ORDER BY ws_timestamp DESC
        LIMIT {int(limit)}
    """)


@st.cache_data(ttl=60, show_spinner=False)
def fetch_recent_anomalies(limit: int) -> pd.DataFrame:
    t = _tables()
    return run_query_to_pandas(
        f"""
        SELECT
            a.detected_at,
            a.detector_type,
            ROUND(a.severity_score, 3) AS severity,
            a.market_condition_id,
            m.QUESTION                 AS market_title,
            a.asset_id
        FROM {t['anomalies']} a
        LEFT JOIN {t['markets']} m
          ON m.CONDITION_ID = a.market_condition_id
        ORDER BY a.detected_at DESC
        LIMIT {int(limit)}
        """,
        fallback_query=f"""
        SELECT
            detected_at,
            detector_type,
            ROUND(severity_score, 3) AS severity,
            market_condition_id,
            NULL                     AS market_title,
            asset_id
        FROM {t['anomalies']}
        ORDER BY detected_at DESC
        LIMIT {int(limit)}
        """,
    )


# ---------------- UI ----------------

st.set_page_config(page_title="Anomaly Detection", layout="wide")
st.title("Anomaly Detection")
st.caption(
    "Live output from the streaming fraud-detection layer "
    "(Polymarket WS → Kafka → detectors → Snowflake). Four detectors run "
    "on every trade: volume_spike, whale, price_impact, timing_burst. "
    "Cached 60s."
)

# ---- top-level summary ----

try:
    summary = fetch_summary()
except Exception as exc:
    st.error(f"Could not reach DETECTED_ANOMALIES: {exc}")
    st.stop()

if summary.empty:
    st.warning(
        "No anomalies found yet. The streaming pipeline writes to "
        f"{_tables()['anomalies']} — confirm it is running on linuxlab."
    )
    st.stop()

total_hits = int(summary["hits"].sum())
markets_affected = int(summary["markets_affected"].max())

col1, col2, col3, col4 = st.columns(4)
col1.metric("Total anomalies", f"{total_hits:,}")
col2.metric("Detectors firing", len(summary))
col3.metric("Markets affected", f"{markets_affected:,}")
col4.metric(
    "Overall avg severity",
    f"{(summary['avg_severity'] * summary['hits']).sum() / total_hits:.3f}",
)

st.subheader("Breakdown by detector")
st.dataframe(
    summary,
    hide_index=True,
    use_container_width=True,
    column_config={
        "detector_type": "Detector",
        "hits": st.column_config.NumberColumn("Hits", format="%d"),
        "avg_severity": st.column_config.NumberColumn("Avg severity", format="%.3f"),
        "max_severity": st.column_config.NumberColumn("Max severity", format="%.3f"),
        "markets_affected": st.column_config.NumberColumn("Markets", format="%d"),
        "first_seen": st.column_config.DatetimeColumn("First seen"),
        "last_seen": st.column_config.DatetimeColumn("Last seen"),
    },
)

# ---- timeline ----

st.subheader("Anomalies over time")
timeline_hours = st.slider("Lookback (hours)", 6, 168, 48, step=6)
timeline = fetch_hourly_timeline(timeline_hours)
if timeline.empty:
    st.info("No anomalies in that window.")
else:
    chart = (
        alt.Chart(timeline)
        .mark_bar()
        .encode(
            x=alt.X("hour:T", title="Hour"),
            y=alt.Y("hits:Q", title="Anomalies", stack="zero"),
            color=alt.Color("detector_type:N", title="Detector"),
            tooltip=["hour:T", "detector_type:N", "hits:Q"],
        )
        .properties(height=280)
    )
    st.altair_chart(chart, use_container_width=True)

# ---- hottest markets ----

st.subheader("Hottest markets")
hot_limit = st.slider("How many markets", 5, 50, 15)
hot = fetch_hot_markets(hot_limit)
if hot.empty:
    st.info("No markets with anomalies yet.")
else:
    st.dataframe(
        hot,
        hide_index=True,
        use_container_width=True,
        column_config={
            "market_condition_id": "Market condition id",
            "market_title": "Market",
            "anomaly_count": st.column_config.NumberColumn("Anomalies", format="%d"),
            "detectors_fired": "Detectors",
            "avg_severity": st.column_config.NumberColumn("Avg severity", format="%.3f"),
            "peak_severity": st.column_config.NumberColumn("Peak severity", format="%.3f"),
            "last_anomaly_at": st.column_config.DatetimeColumn("Last anomaly"),
        },
    )

    # ---- per-market drill-down ----

    st.subheader("Market drill-down")

    options = hot["market_condition_id"].tolist()
    labels = {
        row["market_condition_id"]:
            f"{row['market_condition_id'][:14]}… — "
            f"{row['market_title'] or '(no title)'}"
            f" ({int(row['anomaly_count'])} anomalies)"
        for _, row in hot.iterrows()
    }
    selected = st.selectbox(
        "Pick one of the hottest markets",
        options=options,
        format_func=lambda cid: labels.get(cid, cid),
    )

    if selected:
        left, right = st.columns([3, 2])

        with left:
            st.markdown("**Anomalies on this market**")
            anomalies_df = fetch_market_anomalies(selected)
            st.dataframe(
                anomalies_df.drop(columns=["full_details"]),
                hide_index=True,
                use_container_width=True,
            )
            with st.expander("Raw detector details (JSON)"):
                st.dataframe(
                    anomalies_df[["detected_at", "detector_type", "full_details"]],
                    hide_index=True,
                    use_container_width=True,
                )

        with right:
            st.markdown("**Recent trades on this market**")
            trades_df = fetch_recent_trades_for_market(selected)
            st.dataframe(
                trades_df,
                hide_index=True,
                use_container_width=True,
                column_config={
                    "ws_timestamp": st.column_config.DatetimeColumn("Timestamp"),
                    "side": "Side",
                    "price": st.column_config.NumberColumn("Price", format="%.4f"),
                    "size": st.column_config.NumberColumn("Size", format="%.2f"),
                },
            )

# ---- latest anomaly firehose ----

st.subheader("Latest anomalies (all markets)")
recent_limit = st.slider("How many rows", 20, 500, 100, step=20, key="recent_limit")
recent = fetch_recent_anomalies(recent_limit)
st.dataframe(
    recent,
    hide_index=True,
    use_container_width=True,
    column_config={
        "detected_at": st.column_config.DatetimeColumn("Detected at"),
        "detector_type": "Detector",
        "severity": st.column_config.NumberColumn("Severity", format="%.3f"),
        "market_condition_id": "Market condition id",
        "market_title": "Market",
        "asset_id": "Asset id",
    },
)
