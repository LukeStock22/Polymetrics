# Standalone Airflow Home Setup

This guide explains how to make the `Polymetrics` repo its own Airflow home instead of reusing `airflow25`.

## Resulting layout

After setup, this repo acts as `AIRFLOW_HOME` and contains:

- [dags](/home/compute/l.d.stockbridge/Polymetrics/dags)
- [plugins](/home/compute/l.d.stockbridge/Polymetrics/plugins)
- [logs](/home/compute/l.d.stockbridge/Polymetrics/logs)
- `airflow.db`
- `airflow.cfg`

The pipeline code itself remains in:

- [dags/gamma_markets_to_snowflake.py](/home/compute/l.d.stockbridge/Polymetrics/dags/gamma_markets_to_snowflake.py)
- [src/polymarket_etl](/home/compute/l.d.stockbridge/Polymetrics/src/polymarket_etl)

## What to copy from `airflow25`

You do not need to copy DAG files from `airflow25`.

You may want to copy:

- Airflow connection settings from the Airflow UI
- any useful runtime habits from [../airflow25/airflow.cfg](/home/compute/l.d.stockbridge/airflow25/airflow.cfg)

You should not copy:

- `airflow25/airflow.db`
- old `logs/`
- old DAG files unrelated to Polymarket

## Step 1: Use a shell where Airflow already works

The commands below must be run in a shell that already has:

- `airflow` on `PATH`
- the Snowflake provider installed

If this command fails, you are not in the right runtime yet:

```bash
airflow version
```

## Step 2: Point Airflow at this repo

From the repo root:

```bash
source airflow_home.env.example
```

If you prefer not to `source` the file directly, copy the exports into your shell manually.

## Step 3: Initialize the Airflow metadata database

```bash
airflow db migrate
```

This should create:

- `airflow.db`
- `airflow.cfg`

inside the active SQLite path if they do not already exist.

Current recommended metadata DB:

- `/tmp/polymetrics_airflow.db`

Why `/tmp` instead of the repo directory:

- SQLite is fragile for concurrent Airflow access
- Linuxlab sessions may land on different nodes
- a shared-home SQLite file can produce `sqlite3.OperationalError: disk I/O error`

For this standalone setup, keep all Airflow processes on the same node and use the `/tmp` SQLite file from `airflow_home.env.example`.

## Step 4: Create or recreate the `Snowflake` connection

In the Airflow UI or CLI, create a connection with:

- connection ID: `Snowflake`
- connection type: `Snowflake`
- login: your Snowflake username
- account: `UNB02139`
- warehouse: `PANTHER_WH`
- database: `PANTHER_DB`
- role: `TRAINING_ROLE`

Recommended for the first successful run:

- use password auth only
- clear `private_key_file`
- leave `private_key_content` empty

This avoids the earlier key-file error seen in the `airflow25` logs.

## Step 5: Create an Airflow user

If you need a local UI user:

```bash
airflow users create \
  --username admin \
  --firstname Airflow \
  --lastname Admin \
  --role Admin \
  --email admin@example.com \
  --password 'change-me-now'
```

## Step 6: Confirm the DAG loads

```bash
airflow dags list | rg gamma_markets_to_snowflake
airflow dags list-import-errors
```

If the DAG does not appear, fix import errors before doing anything with Snowflake.

## Step 7: Start Airflow

Important:

- start all Airflow processes from the same compute session / same node
- do not run scheduler on one node and CLI on another while using SQLite

In one terminal:

```bash
source airflow_home.env.example
airflow scheduler
```

In another terminal:

```bash
source airflow_home.env.example
airflow api-server --port 8080
```

Then open the UI and confirm the DAG is visible.

## Step 8: Trigger the pipeline

```bash
source airflow_home.env.example
airflow dags trigger gamma_markets_to_snowflake
```

## Step 9: Debug in order

Watch these tasks in order:

1. `inspect_local_market_files`
2. `ensure_snowflake_objects`
3. `upload_market_pages`
4. `load_raw_market_pages`
5. `upsert_curated_markets`

Typical meaning of failures:

- `ensure_snowflake_objects`
  - Snowflake connection invalid or insufficient Snowflake privileges
- `upload_market_pages`
  - `PUT` issue, local file visibility issue, or Snowflake auth issue
- `load_raw_market_pages`
  - stage/copy mismatch or SQL issue
- `upsert_curated_markets`
  - transform SQL or data-shape issue

## Notes for a junior engineer

The difference between the two approaches is simple:

- reusing `airflow25`
  - one shared Airflow home for multiple projects
- standalone `Polymetrics`
  - this repo contains both the project code and the Airflow runtime state

The standalone layout is easier to reason about because everything related to this pipeline lives in one place.
