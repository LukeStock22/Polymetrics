"""DAG: anomaly_model_training

Nightly training of per-market Isolation Forest models for anomaly
detection.  Uses the past 7 days of trade data from Snowflake.
Trained models are serialized and stored to a Snowflake internal stage.

Runs daily at 07:00 UTC (after order_flow_daily_agg completes).
"""

from __future__ import annotations

import io
import json
import os
import pickle
import sys
import tempfile
from datetime import datetime
from pathlib import Path

from airflow.decorators import dag, task

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = PROJECT_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

SNOWFLAKE_CONN_ID = os.environ.get("POLYMARKET_SNOWFLAKE_CONN_ID", "Snowflake")
SNOWFLAKE_DATABASE = os.environ.get("POLYMARKET_SNOWFLAKE_DATABASE", "DOG_DB")
MODEL_LOOKBACK_DAYS = int(os.environ.get("MODEL_LOOKBACK_DAYS", "7"))
MIN_TRADES_FOR_MODEL = int(os.environ.get("MIN_TRADES_FOR_MODEL", "100"))


def get_snowflake_hook():
    from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
    return SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)


@dag(
    dag_id="anomaly_model_training",
    schedule="0 7 * * *",
    start_date=datetime(2026, 4, 1),
    catchup=False,
    default_args={"owner": "polymetrics"},
    tags=["polymarket", "anomaly", "ml"],
)
def anomaly_model_training():

    @task()
    def ensure_model_stage() -> None:
        """Create internal stage for model storage if not exists."""
        hook = get_snowflake_hook()
        hook.run(f"""
            CREATE STAGE IF NOT EXISTS {SNOWFLAKE_DATABASE}.DOG_SCHEMA.ANOMALY_MODELS_STAGE
            COMMENT = 'Serialized anomaly detection models'
        """)

    @task()
    def identify_trainable_markets() -> list[str]:
        """Find markets with enough recent trade data to train a model."""
        hook = get_snowflake_hook()
        result = hook.get_records(f"""
            SELECT condition_id, COUNT(*) AS trade_count
            FROM   {SNOWFLAKE_DATABASE}.DOG_SCHEMA.DATA_API_TRADES
            WHERE  trade_timestamp >= DATEADD('day', -{MODEL_LOOKBACK_DAYS}, CURRENT_TIMESTAMP())
            GROUP BY condition_id
            HAVING trade_count >= {MIN_TRADES_FOR_MODEL}
            ORDER BY trade_count DESC
            LIMIT 200
        """)
        markets = [row[0] for row in result]
        return markets

    @task()
    def train_models(markets: list[str]) -> dict:
        """Train an Isolation Forest for each market and upload to stage."""
        import numpy as np
        from sklearn.ensemble import IsolationForest

        hook = get_snowflake_hook()
        trained = 0
        failed = 0

        for condition_id in markets:
            try:
                rows = hook.get_records(f"""
                    SELECT size, price, outcome_index,
                           EXTRACT(HOUR FROM trade_timestamp) AS trade_hour,
                           EXTRACT(DOW FROM trade_timestamp) AS trade_dow
                    FROM   {SNOWFLAKE_DATABASE}.DOG_SCHEMA.DATA_API_TRADES
                    WHERE  condition_id = %s
                      AND  trade_timestamp >= DATEADD('day', -{MODEL_LOOKBACK_DAYS},
                                                      CURRENT_TIMESTAMP())
                    ORDER BY trade_timestamp
                """, parameters=[condition_id])

                if len(rows) < MIN_TRADES_FOR_MODEL:
                    continue

                X = np.array(rows, dtype=float)

                model = IsolationForest(
                    n_estimators=100,
                    contamination=0.01,
                    random_state=42,
                    n_jobs=-1,
                )
                model.fit(X)

                # Serialize and upload
                model_bytes = pickle.dumps({
                    "model": model,
                    "condition_id": condition_id,
                    "n_samples": len(rows),
                    "features": ["size", "price", "outcome_index", "trade_hour", "trade_dow"],
                    "trained_at": datetime.utcnow().isoformat(),
                })

                with tempfile.NamedTemporaryFile(suffix=".pkl", delete=False) as f:
                    f.write(model_bytes)
                    tmp_path = f.name

                safe_id = condition_id[:32].replace("/", "_")
                hook.run(
                    f"PUT 'file://{tmp_path}' "
                    f"@{SNOWFLAKE_DATABASE}.DOG_SCHEMA.ANOMALY_MODELS_STAGE/{safe_id}.pkl "
                    f"AUTO_COMPRESS=TRUE OVERWRITE=TRUE"
                )
                Path(tmp_path).unlink(missing_ok=True)
                trained += 1

            except Exception as exc:
                failed += 1
                import logging
                logging.getLogger(__name__).warning(
                    "Failed to train model for %s: %s", condition_id[:16], exc
                )

        return {"trained": trained, "failed": failed, "total_markets": len(markets)}

    stage = ensure_model_stage()
    markets = identify_trainable_markets()
    result = train_models(markets)
    stage >> markets >> result


anomaly_model_training_dag = anomaly_model_training()
