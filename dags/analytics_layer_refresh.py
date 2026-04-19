from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path
from typing import Dict, List

import pendulum
from airflow.decorators import dag, task
from airflow.exceptions import AirflowException

DAGS_DIR = Path(__file__).resolve().parent
if str(DAGS_DIR) not in sys.path:
    sys.path.append(str(DAGS_DIR))

from asset_defs import CURATED_GAMMA_MARKETS_ASSET, CURATED_USER_ACTIVITY_ASSET


PROJECT_ROOT = Path(__file__).resolve().parents[1]
BUILD_SCRIPT = PROJECT_ROOT / "src" / "polymarket_etl" / "build_analytics_layer.py"
DEFAULT_CONFIG_PATH = PROJECT_ROOT / ".streamlit" / "secrets.toml"

ANALYTICS_CONFIG_PATH = Path(
    os.environ.get("POLYMARKET_ANALYTICS_CONFIG_PATH", str(DEFAULT_CONFIG_PATH))
).resolve()
ANALYTICS_BUILD_MODE = os.environ.get("POLYMARKET_ANALYTICS_BUILD_MODE", "incremental")
ANALYTICS_PYTHON_BIN = os.environ.get("POLYMARKET_ANALYTICS_PYTHON", sys.executable)
SNOWFLAKE_CONN_ID = os.environ.get("POLYMARKET_SNOWFLAKE_CONN_ID", "Snowflake")
ANALYTICS_DATABASE = os.environ.get("POLYMARKET_ANALYTICS_DATABASE", "PANTHER_DB")
ANALYTICS_SCHEMA = os.environ.get("POLYMARKET_ANALYTICS_SCHEMA", "ANALYTICS")


def get_snowflake_hook():
    from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

    return SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)


@dag(
    dag_id="analytics_layer_refresh",
    schedule=(CURATED_GAMMA_MARKETS_ASSET | CURATED_USER_ACTIVITY_ASSET),
    start_date=pendulum.datetime(2026, 4, 18, tz="America/Chicago"),
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "polymetrics"},
    tags=["polymarket", "analytics", "snowflake", "snowpark", "asset-aware"],
)
def analytics_layer_refresh():
    @task()
    def validate_runtime() -> Dict[str, str]:
        if ANALYTICS_BUILD_MODE not in {"full", "incremental"}:
            raise AirflowException(
                "POLYMARKET_ANALYTICS_BUILD_MODE must be either 'full' or 'incremental'."
            )
        if not BUILD_SCRIPT.exists():
            raise AirflowException(f"Build script not found: {BUILD_SCRIPT}")
        if not ANALYTICS_CONFIG_PATH.exists():
            raise AirflowException(
                "Analytics config file was not found. Set "
                "POLYMARKET_ANALYTICS_CONFIG_PATH for the Airflow runtime."
            )
        return {
            "build_script": str(BUILD_SCRIPT),
            "config_path": str(ANALYTICS_CONFIG_PATH),
            "project_root": str(PROJECT_ROOT),
            "python_bin": ANALYTICS_PYTHON_BIN,
            "build_mode": ANALYTICS_BUILD_MODE,
        }

    @task()
    def run_build(runtime: Dict[str, str]) -> Dict[str, str]:
        cmd = [
            runtime["python_bin"],
            runtime["build_script"],
            "--config-path",
            runtime["config_path"],
            "--mode",
            runtime["build_mode"],
        ]
        result = subprocess.run(
            cmd,
            cwd=runtime["project_root"],
            capture_output=True,
            text=True,
            env={**os.environ, "PYTHONUNBUFFERED": "1"},
            check=False,
        )
        if result.stdout:
            print(result.stdout)
        if result.stderr:
            print(result.stderr)
        if result.returncode != 0:
            raise AirflowException(
                "Analytics build failed with exit code "
                f"{result.returncode}. Inspect the task log for build output."
            )
        return {
            "build_mode": runtime["build_mode"],
            "config_path": runtime["config_path"],
        }

    @task()
    def validate_refresh(build_result: Dict[str, str]) -> None:
        hook = get_snowflake_hook()
        required_tables: List[str] = [
            "DIM_MARKETS",
            "FACT_USER_ACTIVITY_TRADES",
            "FACT_MARKET_DAILY",
            "FACT_WALLET_DAILY",
            "TRACKED_MARKET_VOLUME_DAILY",
            "MARKET_TOP_TRADERS_DAILY",
            "MARKET_THEME_DAILY",
        ]
        table_names_sql = ", ".join(f"'{name}'" for name in required_tables)
        existing_rows = hook.get_records(
            f"""
            SELECT table_name
            FROM {ANALYTICS_DATABASE}.information_schema.tables
            WHERE table_schema = '{ANALYTICS_SCHEMA}'
              AND table_name IN ({table_names_sql})
            """
        )
        existing_tables = {row[0] for row in existing_rows}
        missing_tables = [name for name in required_tables if name not in existing_tables]
        if missing_tables:
            raise AirflowException(
                "Analytics refresh completed but required tables are missing: "
                + ", ".join(missing_tables)
            )

        build_state = hook.get_first(
            f"""
            SELECT last_build_mode, updated_at, last_successful_coyote_loaded_at
            FROM {ANALYTICS_DATABASE}.{ANALYTICS_SCHEMA}.ANALYTICS_BUILD_STATE
            WHERE pipeline_name = 'analytics_layer_v2'
            """
        )
        if not build_state:
            raise AirflowException(
                "Analytics build state row was not found after the refresh."
            )
        print(
            "[analytics-dag] Build state verified: "
            f"mode={build_state[0]}, updated_at={build_state[1]}, "
            f"coyote_watermark={build_state[2]}, requested_mode={build_result['build_mode']}"
        )

    runtime = validate_runtime()
    build = run_build(runtime)
    validate_refresh(build)


analytics_layer_refresh_dag = analytics_layer_refresh()
