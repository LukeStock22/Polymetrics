# Plugins Folder

This directory exists so the `Polymetrics` repo can serve as its own `AIRFLOW_HOME`.

For the current pipeline, the main reusable code lives in:

- [src/polymarket_etl](/home/compute/l.d.stockbridge/Polymetrics/src/polymarket_etl)

The DAG [gamma_markets_to_snowflake.py](/home/compute/l.d.stockbridge/Polymetrics/dags/gamma_markets_to_snowflake.py) adds that `src/` directory to `sys.path` at runtime, so no custom Airflow plugin code is required yet.

If you later add custom operators, hooks, or sensors, place them here.
