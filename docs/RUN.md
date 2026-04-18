# PolyMetrics Run Guide

This is the minimal, repeatable sequence to run the project after opening the repo.

## 1) Start the Airflow environment

```bash
qlogin-airflow25
cd /home/compute/l.d.stockbridge/Polymetrics
source airflow_home.env.example
```

## 2) Initialize or migrate the Airflow metadata DB

```bash
airflow db migrate
```

If Airflow prompts:
```
Please confirm database initialize (or wait 4 seconds to skip it). Are you sure? [y/N]
```
Type `y` and press Enter.

Important:

- `airflow_home.env.example` points Airflow at `/tmp/polymetrics_airflow.db`.
- That SQLite file is node-local, so if you reopen the project on a different Linuxlab node, you are effectively on a fresh Airflow metadata DB until you recreate it on that node.
- A fresh metadata DB does not have your `Snowflake` connection, serialized DAGs, users, or prior run state.

## 2.1) Ensure the Snowflake connection exists

If this is a fresh Airflow metadata DB, you must create the `Snowflake` connection before any DAG run:

```bash
airflow connections add Snowflake \
  --conn-type snowflake \
  --conn-login PANTHER \
  --conn-schema PUBLIC \
  --conn-password '' \
  --conn-extra '{"account":"UNB02139","warehouse":"PANTHER_WH","database":"PANTHER_DB","role":"TRAINING_ROLE","private_key_file":"/home/compute/l.d.stockbridge/.snowflake_keys/rsa_key_airflow.p8"}'
```

Verify it exists:

```bash
airflow connections get Snowflake
```

## 3) Reserialize DAGs

```bash
airflow dags reserialize
```

Do this after reconnecting to a new session and after changing DAG or ETL code.

If you see `sqlite3.OperationalError: no such table: dag_bundle`, that means the Airflow DB is not initialized for this environment. Fix it by rerunning:

```bash
airflow db migrate
airflow dags reserialize
```

If `process_window` fails with `HTTP Error 403: Forbidden`, the Gamma API is blocking the request. The fetcher now sends a User-Agent and retries with backoff, so rerun after updating `src/polymarket_etl/gamma_fetch.py`.

If a `process_window` task appears stuck for a long time, the fetcher now uses a network timeout and logs per-page progress so you can see forward motion in the task log.

If a later task still seems to be running old SQL after you changed the repo code, restart Airflow and rerun `airflow dags reserialize`. For example, the current source uses `MD5_HEX(...)` for the payload hash, so a task log showing `TO_HEX(MD5(...))` means the runtime is still using stale serialized DAG code.

## 4) Confirm DAGs load

```bash
airflow dags list
```

You should see:

- `gamma_markets_to_snowflake`
- `gamma_markets_daily`
- `gamma_markets_catchup`
- `analytics_layer_refresh`

## 5) Trigger a DAG manually (optional)

```bash
airflow dags trigger gamma_markets_catchup
```

To trigger the analytics refresh manually:

```bash
airflow dags trigger analytics_layer_refresh
```

## 5.1) Run the analytics build once in the same Airflow shell

If you want to test the analytics build directly before relying on the DAG, run the same Snowpark build script that `analytics_layer_refresh` calls:

```bash
python src/polymarket_etl/build_analytics_layer.py \
  --config-path "$POLYMARKET_ANALYTICS_CONFIG_PATH" \
  --mode "$POLYMARKET_ANALYTICS_BUILD_MODE"
```

This should be run only after:

- `source airflow_home.env.example`
- the `Snowflake` Airflow connection exists
- `.streamlit/secrets.toml` exists at `POLYMARKET_ANALYTICS_CONFIG_PATH` and is valid in the Linux Airflow environment

To force a one-time full rebuild instead of the default incremental mode:

```bash
python src/polymarket_etl/build_analytics_layer.py \
  --config-path "$POLYMARKET_ANALYTICS_CONFIG_PATH" \
  --mode full
```

This CLI path is useful for validating that:

- the Airflow shell can import the project code
- Snowpark can authenticate to Snowflake from the Linux environment
- the analytics layer can be rebuilt before connecting upstream DAG completion to the analytics DAG

If this direct CLI run works, the Airflow DAG is using the same build script and the same environment variables, so the next step is orchestration rather than ETL debugging.

Then check run state:

```bash
airflow dags list-runs gamma_markets_catchup
```

To see task-level progress for a specific run:

```bash
RUN_ID="<paste_run_id_here>"
airflow tasks states-for-dag-run gamma_markets_catchup "$RUN_ID"
```

The catch-up DAG runs a single reconciliation window from the current watermark to yesterday midnight. Closed markets are filtered by `end_date_min`/`end_date_max`, so it avoids scanning the full closed history every run.
The daily DAG uses the Airflow data interval but will also respect the latest `updated_at` watermark (it extends the window up to “now” and starts from the most recent curated update).
The analytics DAG runs the Snowpark build script in `incremental` mode by default and verifies that the core analytics tables plus `ANALYTICS_BUILD_STATE` are present afterward.
Concretely:

- `start` = `MAX(updated_at)` from `CURATED.GAMMA_MARKETS`
- `end` = max(`data_interval_end`, `now`) at runtime
- analytics refresh time = `25` minutes past each hour by default

## 6) Start the Airflow services (for scheduled runs)

The scheduler must be running for daily schedules to execute. Use standalone (recommended):

```bash
nohup airflow standalone > /tmp/polymetrics_standalone.log 2>&1 &
```

Check health:

```bash
curl -s http://127.0.0.1:8080/api/v2/monitor/health
```

## Notes

- Always run Airflow in the same `qlogin-airflow25` session you used to initialize the DB.
- If the session ends, scheduled jobs will stop until Airflow is restarted.
- On a new node, treat the startup as a reconnect: run `airflow db migrate`, recreate or verify `Snowflake`, then run `airflow dags reserialize` before triggering the catch-up DAG.
 - If a daily run fails before `upsert_curated_markets`, the watermark does not advance. If it fails after `upsert_curated_markets` (for example, in a DQ check), curated has already been updated so the watermark has advanced even though the DAG is marked failed.
