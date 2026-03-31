# Include Folder

Use this folder for SQL files, templates, seed files, or other static assets that Airflow tasks may reference at runtime.

The current Polymarket pipeline stores its Snowflake bootstrap SQL under:

- [sql/snowflake/00_bootstrap_polymarket.sql](/home/compute/l.d.stockbridge/polymarket/sql/snowflake/00_bootstrap_polymarket.sql)

You can keep using `sql/` as-is, or move reusable task assets into `include/` later if you want a more standard Airflow-home layout.

