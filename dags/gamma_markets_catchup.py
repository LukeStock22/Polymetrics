from __future__ import annotations

import os
from pathlib import Path
from typing import Dict, List

import pendulum
from airflow.decorators import dag, task
from airflow.exceptions import AirflowException

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
    dag_id="gamma_markets_catchup",
    schedule=None,
    start_date=pendulum.datetime(2026, 4, 7, tz="America/Chicago"),
    catchup=False,
    max_active_runs=1,
    max_active_tasks=1,
    default_args={"owner": "polymetrics"},
    tags=["polymarket", "gamma", "snowflake", "catchup"],
)
def gamma_markets_catchup():
    @task()
    def ensure_snowflake_objects() -> None:
        hook = get_snowflake_hook()
        for statement in bootstrap_sql(SNOWFLAKE_DATABASE).split(";"):
            sql = statement.strip()
            if sql:
                hook.run(sql)

    @task()
    def compute_windows() -> List[Dict[str, str]]:
        from airflow.exceptions import AirflowSkipException

        hook = get_snowflake_hook()
        watermark = hook.get_first(
            f"SELECT MAX(updated_at) FROM {SNOWFLAKE_DATABASE}.CURATED.GAMMA_MARKETS"
        )[0]
        tz = pendulum.timezone("America/Chicago")
        end = pendulum.now(tz).start_of("day")
        if watermark:
            start = pendulum.instance(watermark, tz=tz)
        else:
            start = pendulum.datetime(2000, 1, 1, tz="UTC").in_timezone(tz)
        if start >= end:
            raise AirflowSkipException("Watermark is already current — nothing to catch up.")
        label_end = end.subtract(days=1)
        window_label = f"{start.to_date_string()}_to_{label_end.to_date_string()}"
        return [
            {
                "start": start.to_iso8601_string(),
                "end": end.to_iso8601_string(),
                "date": window_label,
            }
        ]

    @task()
    def process_window(window: Dict[str, str]) -> Dict[str, object]:
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
        if not files:
            return {"run_dir": str(run_dir), "files": [], "total_rows": total_rows}

        hook = get_snowflake_hook()
        put_files_to_stage(hook, SNOWFLAKE_DATABASE, files)
        manifest_sql = build_manifest_rows_sql(run_dir, [path.name for path in files])
        statements = [
            truncate_stage_table_sql(SNOWFLAKE_DATABASE),
            copy_market_pages_into_stage_sql(SNOWFLAKE_DATABASE, manifest_sql),
            merge_raw_market_pages_sql(SNOWFLAKE_DATABASE),
            merge_curated_markets_sql(SNOWFLAKE_DATABASE),
        ]
        for sql in statements:
            hook.run(sql)
        return {"run_dir": str(run_dir), "files": [p.name for p in files], "total_rows": total_rows}

    @task()
    def final_checks(windows: List[Dict[str, str]]) -> None:
        if not windows:
            return
        hook = get_snowflake_hook()
        raw_dupes = hook.get_first(
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
        if raw_dupes > 0:
            raise AirflowException("Duplicate rows detected in RAW.GAMMA_MARKET_PAGES.")
        curated_dupes = hook.get_first(
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
        if curated_dupes > 0:
            raise AirflowException("Duplicate market_id values detected in CURATED.GAMMA_MARKETS.")
        max_updated = hook.get_first(
            f"SELECT MAX(updated_at) FROM {SNOWFLAKE_DATABASE}.CURATED.GAMMA_MARKETS"
        )[0]
        tz = pendulum.timezone("America/Chicago")
        expected = pendulum.now(tz).start_of("day").subtract(days=1)
        if max_updated:
            if max_updated.tzinfo is None:
                max_updated_dt = pendulum.instance(max_updated, tz="UTC")
            else:
                max_updated_dt = pendulum.instance(max_updated).in_timezone("UTC")
            expected_dt = expected.in_timezone("UTC")
        else:
            max_updated_dt = None
            expected_dt = expected.in_timezone("UTC")
        if not max_updated_dt or max_updated_dt < expected_dt:
            raise AirflowException("Curated markets are not fresh through yesterday midnight.")

    ensure = ensure_snowflake_objects()
    windows = compute_windows()
    processed = process_window.expand(window=windows)
    checks = final_checks(windows)

    ensure >> windows >> processed >> checks


gamma_markets_catchup_dag = gamma_markets_catchup()
