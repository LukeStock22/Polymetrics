# Polymarket Gamma to Snowflake

This repo now has a first-pass ingestion path for `data/raw/gamma/markets/*.json` into Snowflake.

## What gets created

- `PANTHER_DB.RAW.GAMMA_MARKET_PAGES_STAGE`
  - scratch table used during each Airflow run
- `PANTHER_DB.RAW.GAMMA_MARKET_PAGES`
  - one row per uploaded JSON page file version
  - stores the full page payload as a `VARIANT`
- `PANTHER_DB.CURATED.GAMMA_MARKETS`
  - latest known record for each `market_id`
  - keeps important scalar fields typed and retains the full `market_payload`
- `PANTHER_DB.RAW.GAMMA_MARKETS_STAGE`
  - internal Snowflake stage used by `PUT`

## Repo files

- [dags/gamma_markets_to_snowflake.py](/home/compute/l.d.stockbridge/polymarket/dags/gamma_markets_to_snowflake.py)
- [src/polymarket_etl/market_files.py](/home/compute/l.d.stockbridge/polymarket/src/polymarket_etl/market_files.py)
- [src/polymarket_etl/snowflake_sql.py](/home/compute/l.d.stockbridge/polymarket/src/polymarket_etl/snowflake_sql.py)
- [sql/snowflake/00_bootstrap_polymarket.sql](/home/compute/l.d.stockbridge/polymarket/sql/snowflake/00_bootstrap_polymarket.sql)

## Airflow config

Set these environment variables in the Airflow runtime that will load the DAG:

```bash
export POLYMARKET_SNOWFLAKE_CONN_ID=Snowflake
export POLYMARKET_SNOWFLAKE_DATABASE=PANTHER_DB
export POLYMARKET_MARKETS_DIR=/home/compute/l.d.stockbridge/polymarket/data/raw/gamma/markets
export PYTHONPATH=/home/compute/l.d.stockbridge/polymarket/src:$PYTHONPATH
```

If you want this repo to act as its own `AIRFLOW_HOME`, see [standalone_airflow_home.md](/home/compute/l.d.stockbridge/polymarket/docs/standalone_airflow_home.md).

The DAG assumes the Airflow connection already exists and has permission to:

- create databases, schemas, stages, file formats, and tables
- `PUT` local files into the internal stage
- `COPY INTO` and `MERGE` into the target tables

## Reuse `airflow25`

The sibling [airflow25](/home/compute/l.d.stockbridge/airflow25) folder is already laid out as an `AIRFLOW_HOME` and is the environment to mimic.

Its relevant settings are:

- `dags_folder = /home/compute/l.d.stockbridge/airflow25/dags`
- `plugins_folder = /home/compute/l.d.stockbridge/airflow25/plugins`
- `executor = LocalExecutor`
- `sql_alchemy_conn = sqlite:////home/compute/l.d.stockbridge/airflow25/airflow.db`
- active Snowflake connection ID used by the existing DAGs: `Snowflake`

To run your Polymarket DAGs against that Airflow home, use environment overrides rather than editing `airflow25/airflow.cfg`:

```bash
export AIRFLOW_HOME=/home/compute/l.d.stockbridge/airflow25
export AIRFLOW__CORE__DAGS_FOLDER=/home/compute/l.d.stockbridge/polymarket/dags
export AIRFLOW__CORE__PLUGINS_FOLDER=/home/compute/l.d.stockbridge/airflow25/plugins
export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=sqlite:////home/compute/l.d.stockbridge/airflow25/airflow.db
export POLYMARKET_SNOWFLAKE_CONN_ID=Snowflake
export POLYMARKET_SNOWFLAKE_DATABASE=PANTHER_DB
export POLYMARKET_MARKETS_DIR=/home/compute/l.d.stockbridge/polymarket/data/raw/gamma/markets
export PYTHONPATH=/home/compute/l.d.stockbridge/polymarket/src
```

If you prefer to keep both the old `urbanrides` DAG and the new Polymarket DAG visible at once, set:

```bash
export AIRFLOW__CORE__DAGS_FOLDER=/home/compute/l.d.stockbridge/airflow25/dags
```

and then place a copy or symlink of [gamma_markets_to_snowflake.py](/home/compute/l.d.stockbridge/polymarket/dags/gamma_markets_to_snowflake.py) into [../airflow25/dags](/home/compute/l.d.stockbridge/airflow25/dags). I did not do that copy automatically because `airflow25` is outside this repo workspace.

## Snowflake connection note

The `airflow25` logs show a real Airflow connection named `Snowflake` is present and has been used successfully for query tasks. They also show some runs failed with:

- `The private_key_file path points to an empty or invalid file.`

That means the Airflow connection exists, but if your current auth method is key-pair auth, the file path configured in the Airflow connection extras still needs to point to a valid key on the machine running Airflow.

## First run

1. Run [sql/snowflake/00_bootstrap_polymarket.sql](/home/compute/l.d.stockbridge/polymarket/sql/snowflake/00_bootstrap_polymarket.sql) manually if you want to validate object creation before Airflow.
2. Copy or mount [dags/gamma_markets_to_snowflake.py](/home/compute/l.d.stockbridge/polymarket/dags/gamma_markets_to_snowflake.py) into your Airflow `dags/` folder.
3. Make sure the repo `src/` directory is on Airflow `PYTHONPATH`.
4. Trigger `gamma_markets_to_snowflake`.

## Load behavior

- The DAG scans local JSON files and uploads every file to the Snowflake stage with `OVERWRITE=TRUE`.
- Raw deduping is handled by `MERGE` on `(source_file_name, file_content_key)`.
- The curated `GAMMA_MARKETS` table keeps only the newest version per `market_id`, based on `updatedAt` from the payload and then `raw_loaded_at`.

## Known constraints

- I could not run the DAG locally because this shell does not currently have `airflow` or the Snowflake provider installed.
- The current curated table focuses on the market-level record and leaves nested arrays and objects in `VARIANT` columns.
- If your Airflow workers do not have direct filesystem access to this repo path, the upload task will need to be moved to external stage storage such as S3.
