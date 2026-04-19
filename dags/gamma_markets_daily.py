from __future__ import annotations

import os
from pathlib import Path
import sys
from typing import Dict, List

import pendulum
from airflow.decorators import dag, task
from airflow.exceptions import AirflowException

DAGS_DIR = Path(__file__).resolve().parent
if str(DAGS_DIR) not in sys.path:
    sys.path.append(str(DAGS_DIR))

from asset_defs import CURATED_GAMMA_MARKETS_ASSET
from polymarket_etl.gamma_fetch import GammaSource, GAMMA_MARKETS_BASE, write_market_pages
from polymarket_etl.gamma_ingest import build_manifest_rows_sql, put_files_to_stage
from polymarket_etl.snowflake_sql import (
    bootstrap_sql,
    copy_market_pages_into_stage_sql,
    merge_curated_markets_sql,
    merge_raw_market_pages_sql,
    truncate_stage_table_sql,
)


SNOWFLAKE_CONN_ID = os.environ.get("POLYMARKET_SNOWFLAKE_CONN_ID", "Snowflake")
SNOWFLAKE_DATABASE = os.environ.get("POLYMARKET_SNOWFLAKE_DATABASE", "PANTHER_DB")
MARKETS_DATA_DIR = Path(
    os.environ.get(
        "POLYMARKET_MARKETS_DIR",
        str(Path(__file__).resolve().parents[1] / "data" / "raw" / "gamma" / "markets"),
    )
).resolve()
FETCH_LIMIT = int(os.environ.get("POLYMARKET_MARKETS_LIMIT", "200"))
FETCH_SLEEP = float(os.environ.get("POLYMARKET_MARKETS_SLEEP", "0.15"))


def get_snowflake_hook():
    from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

    return SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)


@dag(
    dag_id="gamma_markets_daily",
    schedule="0 * * * *",
    # schedule="33 * * * *",
    start_date=pendulum.datetime(2026, 4, 7, tz="America/Chicago"),
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "polymetrics"},
    tags=["polymarket", "gamma", "snowflake", "daily"],
)
def gamma_markets_daily():
    @task()
    def compute_window() -> Dict[str, str]:
        from airflow.exceptions import AirflowSkipException
        from airflow.sdk import get_current_context

        ctx = get_current_context()
        tz = pendulum.timezone("America/Chicago")
        interval_start_raw = ctx.get("data_interval_start")
        interval_end_raw = ctx.get("data_interval_end")
        now = pendulum.now(tz)

        # Scheduled runs have a data interval; manual triggers do not.
        if interval_start_raw is not None and interval_end_raw is not None:
            interval_start = pendulum.instance(interval_start_raw).in_timezone(tz)
            interval_end = pendulum.instance(interval_end_raw).in_timezone(tz)
            end = interval_end if interval_end >= now else now
        else:
            interval_start = None
            end = now

        hook = get_snowflake_hook()
        watermark = hook.get_first(
            f"SELECT MAX(updated_at) FROM {SNOWFLAKE_DATABASE}.CURATED.GAMMA_MARKETS"
        )[0]
        if watermark:
            if watermark.tzinfo is None:
                start = pendulum.instance(watermark, tz="UTC").in_timezone(tz)
            else:
                start = pendulum.instance(watermark).in_timezone(tz)
        else:
            if interval_start is not None:
                start = interval_start
            else:
                start = end.subtract(hours=1)

        if start >= end:
            raise AirflowSkipException("Watermark is already current — nothing to update.")
        return {
            "start": start.to_iso8601_string(),
            "end": end.to_iso8601_string(),
            "date": start.to_date_string(),
        }

    @task()
    def ensure_snowflake_objects() -> None:
        hook = get_snowflake_hook()
        for statement in bootstrap_sql(SNOWFLAKE_DATABASE).split(";"):
            sql = statement.strip()
            if sql:
                hook.run(sql)

    @task()
    def fetch_market_pages(window: Dict[str, str]) -> Dict[str, object]:
        run_dir = MARKETS_DATA_DIR / window["date"]
        start = pendulum.parse(window["start"])
        end = pendulum.parse(window["end"])
        sources = [
            GammaSource(
                page_name="active_closedfalse",
                base_url=GAMMA_MARKETS_BASE,
                params={"active": "true", "closed": "false"},
            ),
            GammaSource(
                page_name="closed_true",
                base_url=GAMMA_MARKETS_BASE,
                params={
                    "closed": "true",
                    "end_date_min": start.in_timezone("UTC").to_iso8601_string(),
                    "end_date_max": end.in_timezone("UTC").to_iso8601_string(),
                },
            ),
        ]
        files, total_rows = write_market_pages(
            run_dir=run_dir,
            start=start,
            end=end,
            limit=FETCH_LIMIT,
            sleep_seconds=FETCH_SLEEP,
            sources=sources,
        )
        return {
            "run_dir": str(run_dir),
            "files": [path.name for path in files],
            "total_rows": total_rows,
        }

    @task()
    def upload_market_pages(info: Dict[str, object]) -> None:
        files = info["files"]
        if not files:
            raise AirflowException("No market updates found for the daily window.")
        run_dir = Path(str(info["run_dir"]))
        hook = get_snowflake_hook()
        put_files_to_stage(hook, SNOWFLAKE_DATABASE, [run_dir / name for name in files])

    @task()
    def load_raw_market_pages(info: Dict[str, object]) -> None:
        files = info["files"]
        if not files:
            raise AirflowException("No files available for the raw load.")
        run_dir = Path(str(info["run_dir"]))
        hook = get_snowflake_hook()
        manifest_sql = build_manifest_rows_sql(run_dir, list(files))
        statements = [
            truncate_stage_table_sql(SNOWFLAKE_DATABASE),
            copy_market_pages_into_stage_sql(SNOWFLAKE_DATABASE, manifest_sql),
            merge_raw_market_pages_sql(SNOWFLAKE_DATABASE),
        ]
        for sql in statements:
            hook.run(sql)

    @task(outlets=[CURATED_GAMMA_MARKETS_ASSET])
    def upsert_curated_markets() -> None:
        hook = get_snowflake_hook()
        hook.run(merge_curated_markets_sql(SNOWFLAKE_DATABASE))

    @task()
    def check_stage_rows() -> None:
        hook = get_snowflake_hook()
        count = hook.get_first(
            f"SELECT COUNT(*) FROM {SNOWFLAKE_DATABASE}.RAW.GAMMA_MARKET_PAGES_STAGE"
        )[0]
        if count <= 0:
            raise AirflowException("Stage table has zero rows after load.")

    @task()
    def check_raw_dupes() -> None:
        hook = get_snowflake_hook()
        dupes = hook.get_first(
            f"""
            SELECT COUNT(*)
            FROM (
              SELECT source_file_name, file_content_key, COUNT(*) AS c
              FROM {SNOWFLAKE_DATABASE}.RAW.GAMMA_MARKET_PAGES
              GROUP BY 1, 2
              HAVING COUNT(*) > 1
            )
            """
        )[0]
        if dupes > 0:
            raise AirflowException("Duplicate rows detected in RAW.GAMMA_MARKET_PAGES.")

    @task()
    def check_curated_dupes() -> None:
        hook = get_snowflake_hook()
        dupes = hook.get_first(
            f"""
            SELECT COUNT(*)
            FROM (
              SELECT market_id, COUNT(*) AS c
              FROM {SNOWFLAKE_DATABASE}.CURATED.GAMMA_MARKETS
              GROUP BY 1
              HAVING COUNT(*) > 1
            )
            """
        )[0]
        if dupes > 0:
            raise AirflowException("Duplicate market_id values detected in CURATED.GAMMA_MARKETS.")

    @task()
    def check_freshness(window: Dict[str, str]) -> None:
        hook = get_snowflake_hook()
        max_updated = hook.get_first(
            f"SELECT MAX(updated_at) FROM {SNOWFLAKE_DATABASE}.CURATED.GAMMA_MARKETS"
        )[0]
        if not max_updated:
            raise AirflowException("No updated_at values found in curated markets.")
        window_start = pendulum.parse(window["start"])
        if max_updated.tzinfo is None:
            max_updated_dt = pendulum.instance(max_updated, tz="UTC")
        else:
            max_updated_dt = pendulum.instance(max_updated).in_timezone("UTC")
        window_start_dt = window_start.in_timezone("UTC")
        if max_updated_dt < window_start_dt:
            raise AirflowException("Curated markets are not fresh for the daily window.")

    window = compute_window()
    ensure = ensure_snowflake_objects()
    fetch = fetch_market_pages(window)
    upload = upload_market_pages(fetch)
    load = load_raw_market_pages(fetch)
    upsert = upsert_curated_markets()

    stage_check = check_stage_rows()
    raw_dupes = check_raw_dupes()
    curated_dupes = check_curated_dupes()
    freshness = check_freshness(window)

    ensure >> fetch >> upload >> load >> upsert
    upsert >> stage_check >> raw_dupes >> curated_dupes >> freshness


gamma_markets_daily_dag = gamma_markets_daily()
