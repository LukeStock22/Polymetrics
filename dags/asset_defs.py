from __future__ import annotations

try:
    from airflow.sdk import Asset as AirflowAsset
except ImportError:  # Airflow 2.x compatibility
    from airflow.datasets import Dataset as AirflowAsset


CURATED_GAMMA_MARKETS_ASSET = AirflowAsset(
    "snowflake://PANTHER_DB/CURATED/GAMMA_MARKETS"
)
CURATED_USER_ACTIVITY_ASSET = AirflowAsset(
    "snowflake://COYOTE_DB/PUBLIC/CURATED_POLYMARKET_USER_ACTIVITY"
)

