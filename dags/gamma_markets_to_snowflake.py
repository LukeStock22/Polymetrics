from __future__ import annotations

import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Iterable, List, Optional

from airflow.decorators import dag, task
from airflow.exceptions import AirflowException
from airflow.operators.python import get_current_context

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = PROJECT_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from polymarket_etl.market_files import discover_market_page_files, summarize_market_page_files
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
        str(PROJECT_ROOT / "data" / "raw" / "gamma" / "markets"),
    )
).resolve()


def get_snowflake_hook():
    from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

    return SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)


def sql_string_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def sql_nullable_string_literal(value: Optional[str]) -> str:
    if value is None:
        return "NULL"
    return sql_string_literal(value)


def sql_bool_literal(value):
    if value is None:
        return "NULL"
    return "TRUE" if value else "FALSE"


def build_manifest_rows_sql(file_names: Optional[List[str]] = None) -> str:
    files = discover_market_page_files(MARKETS_DATA_DIR)
    if not files:
        raise AirflowException(f"No market files found in {MARKETS_DATA_DIR}")

    allowed = set(file_names or [])
    if allowed:
        files = [item for item in files if item.file_name in allowed]
    if not files:
        raise AirflowException("No matching files found for the raw load manifest")

    rows = []
    for item in files:
        rows.append(
            "("
            f"{sql_string_literal(item.file_name)}, "
            f"{sql_string_literal(item.page_name)}, "
            f"{item.page_limit}, "
            f"{item.page_offset}, "
            f"{sql_bool_literal(item.is_active)}, "
            f"{sql_bool_literal(item.is_closed)}, "
            f"{item.row_count if item.row_count is not None else 'NULL'}, "
            f"{sql_nullable_string_literal(item.md5_hex)}"
            ")"
        )
    return (
        "SELECT * FROM VALUES\n"
        + ",\n".join(rows)
        + "\nAS manifest(source_file_name, page_name, page_limit, page_offset, "
        "source_is_active, source_is_closed, expected_row_count, expected_md5_hex)"
    )


def put_files_to_stage(files: Iterable[Path]) -> None:
    hook = get_snowflake_hook()
    stage_name = f'@"{SNOWFLAKE_DATABASE}"."RAW"."GAMMA_MARKETS_STAGE"'

    for path in files:
        put_sql = (
            f"PUT {sql_string_literal('file://' + str(path))} "
            f"{stage_name} "
            "AUTO_COMPRESS=FALSE "
            "OVERWRITE=TRUE "
            "PARALLEL=8"
        )
        hook.run(put_sql)


@dag(
    dag_id="gamma_markets_to_snowflake",
    schedule=None,
    start_date=datetime(2026, 3, 1),
    catchup=False,
    default_args={"owner": "polymetrics"},
    tags=["polymarket", "gamma", "snowflake"],
)
def gamma_markets_to_snowflake():
    @task()
    def inspect_local_market_files() -> dict:
        files = discover_market_page_files(
            MARKETS_DATA_DIR, include_content_metrics=True
        )
        summary = summarize_market_page_files(files)
        if not files:
            raise AirflowException(f"No files found in {MARKETS_DATA_DIR}")
        return summary

    @task()
    def ensure_snowflake_objects() -> None:
        hook = get_snowflake_hook()
        for statement in bootstrap_sql(SNOWFLAKE_DATABASE).split(";"):
            sql = statement.strip()
            if sql:
                hook.run(sql)

    @task()
    def upload_market_pages() -> List[str]:
        files = discover_market_page_files(MARKETS_DATA_DIR)
        context = get_current_context()
        dag_run = context.get("dag_run")
        skip_upload = bool(dag_run and dag_run.conf and dag_run.conf.get("skip_upload"))
        if not skip_upload:
            put_files_to_stage([item.absolute_path for item in files])
        return [item.file_name for item in files]

    @task()
    def load_raw_market_pages(file_names: List[str]) -> None:
        if not file_names:
            raise AirflowException("No files were uploaded to Snowflake stage")

        hook = get_snowflake_hook()
        manifest_sql = build_manifest_rows_sql(file_names)
        statements = [
            truncate_stage_table_sql(SNOWFLAKE_DATABASE),
            copy_market_pages_into_stage_sql(SNOWFLAKE_DATABASE, manifest_sql),
            merge_raw_market_pages_sql(SNOWFLAKE_DATABASE),
        ]
        for sql in statements:
            hook.run(sql)

    @task()
    def upsert_curated_markets() -> None:
        hook = get_snowflake_hook()
        hook.run(merge_curated_markets_sql(SNOWFLAKE_DATABASE))

    local_summary = inspect_local_market_files()
    snowflake_objects = ensure_snowflake_objects()
    staged_files = upload_market_pages()
    raw_loaded = load_raw_market_pages(staged_files)
    local_summary >> snowflake_objects >> staged_files >> raw_loaded >> upsert_curated_markets()

gamma_markets_to_snowflake_dag = gamma_markets_to_snowflake()
