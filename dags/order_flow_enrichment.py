"""DAG: order_flow_enrichment

Polls the Polymarket Data API for recent trades (with wallet addresses)
and loads them into RAW.DATA_API_TRADES.  Then enriches
RAW.CLOB_TRADES_STREAM rows that are missing a wallet_address by
joining on (conditionId, timestamp, size, price).

Intended schedule: every 15-30 minutes.
"""

from __future__ import annotations

import json
import os
import sys
import urllib.request
from datetime import datetime, timedelta
from pathlib import Path

from airflow.decorators import dag, task

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = PROJECT_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

SNOWFLAKE_CONN_ID = os.environ.get("POLYMARKET_SNOWFLAKE_CONN_ID", "Snowflake")
SNOWFLAKE_DATABASE = os.environ.get("POLYMARKET_SNOWFLAKE_DATABASE", "DOG_DB")
DATA_API_BASE = os.environ.get("POLYMARKET_DATA_API_BASE", "https://data-api.polymarket.com")
REST_POLL_LIMIT = int(os.environ.get("REST_POLL_LIMIT", "10000"))


def get_snowflake_hook():
    from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
    return SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)


@dag(
    is_paused_upon_creation=True,
    dag_id="order_flow_enrichment",
    schedule=timedelta(minutes=30),
    start_date=datetime(2026, 4, 1),
    catchup=False,
    default_args={"owner": "polymetrics"},
    tags=["polymarket", "order_flow", "enrichment"],
)
def order_flow_enrichment():

    @task()
    def fetch_recent_trades() -> dict:
        """Pull recent trades from the Data API.  Returns summary stats."""
        url = f"{DATA_API_BASE}/trades?limit={REST_POLL_LIMIT}&takerOnly=false"
        req = urllib.request.Request(url, headers={"User-Agent": "PolyMetrics/1.0"})
        with urllib.request.urlopen(req, timeout=60) as resp:
            trades = json.loads(resp.read())

        if not trades:
            return {"fetched": 0}

        # Build INSERT values
        hook = get_snowflake_hook()
        batch_size = 500
        inserted = 0

        for i in range(0, len(trades), batch_size):
            batch = trades[i : i + batch_size]
            values = []
            for t in batch:
                values.append((
                    t.get("proxyWallet", ""),
                    t.get("conditionId", ""),
                    t.get("asset", ""),
                    t.get("side", ""),
                    t.get("outcome", ""),
                    t.get("outcomeIndex"),
                    t.get("price"),
                    t.get("size"),
                    t.get("title", ""),
                    t.get("slug", ""),
                    t.get("eventSlug", ""),
                    t.get("transactionHash", ""),
                    t.get("timestamp"),
                    json.dumps(t),
                ))

            sql = f"""
                INSERT INTO {SNOWFLAKE_DATABASE}.DOG_SCHEMA.DATA_API_TRADES
                    (proxy_wallet, condition_id, asset, side, outcome,
                     outcome_index, price, size, title, slug, event_slug,
                     transaction_hash, trade_timestamp, raw_payload)
                SELECT column1, column2, column3, column4, column5,
                       column6::NUMBER(2,0),
                       column7::NUMBER(18,6),
                       column8::NUMBER(38,12),
                       column9, column10, column11, column12,
                       TO_TIMESTAMP(column13::NUMBER(18,0)),
                       PARSE_JSON(column14)
                FROM VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                WHERE NOT EXISTS (
                    SELECT 1 FROM {SNOWFLAKE_DATABASE}.DOG_SCHEMA.DATA_API_TRADES t
                    WHERE t.transaction_hash = column12
                      AND t.proxy_wallet = column1
                      AND t.condition_id = column2
                )
            """
            hook.run(sql, parameters=values)
            inserted += len(batch)

        return {"fetched": len(trades), "inserted_approx": inserted}

    @task()
    def enrich_stream_trades() -> dict:
        """Join stream trades with API trades to fill in wallet_address."""
        hook = get_snowflake_hook()

        sql = f"""
            UPDATE {SNOWFLAKE_DATABASE}.DOG_SCHEMA.CLOB_TRADES_STREAM s
            SET    s.wallet_address = d.proxy_wallet,
                   s.enriched_at   = CURRENT_TIMESTAMP()
            FROM   {SNOWFLAKE_DATABASE}.DOG_SCHEMA.DATA_API_TRADES d
            WHERE  s.wallet_address IS NULL
              AND  s.market_condition_id = d.condition_id
              AND  ABS(DATEDIFF('second', s.ws_timestamp,
                       d.trade_timestamp)) <= 5
              AND  ABS(s.size - d.size) < 0.001
              AND  ABS(s.price - d.price) < 0.0001
        """
        result = hook.run(sql)
        return {"enrichment": "complete"}

    fetch = fetch_recent_trades()
    enrich = enrich_stream_trades()
    fetch >> enrich


order_flow_enrichment_dag = order_flow_enrichment()
