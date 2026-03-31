# PolyMetrics Overview

## Project Purpose

My understanding is that PolyMetrics is meant to become a Polymarket analytics platform rather than just a one-off ingestion script.

The proposal in [PROPOSAL.md](/home/compute/l.d.stockbridge/Polymetrics/PROPOSAL.md) points to three layers of value:

1. Collect large-scale Polymarket market and user activity data from official APIs, including both historical and live feeds.
2. Store that data in a structured warehouse so it can support repeatable analysis.
3. Build analytics and, eventually, a dashboard that helps answer questions about trader profitability, trading behavior, market activity, and user-level strategy.

The current repository looks like the first concrete slice of that larger goal. Right now it is primarily an ingestion and warehouse-bootstrap project:

- Airflow orchestrates the load.
- Snowflake is the target warehouse.
- The current DAG focuses on Gamma market-page JSON ingestion.
- Supporting scripts suggest the repo is preparing to broaden coverage across market pages, market detail/tag data, and other raw Polymarket sources.

## Current Project Shape

The structure mostly makes sense for the stage the project is in. It separates orchestration, reusable code, SQL, raw data collection, and docs in a way that should scale reasonably well.

### What each top-level area appears to do

- [dags](/home/compute/l.d.stockbridge/Polymetrics/dags)
  - Airflow DAG definitions.
  - The main workflow is [gamma_markets_to_snowflake.py](/home/compute/l.d.stockbridge/Polymetrics/dags/gamma_markets_to_snowflake.py), which inspects local files, ensures Snowflake objects exist, uploads files to a Snowflake stage, loads raw rows, and upserts curated market records.

- [src/polymarket_etl](/home/compute/l.d.stockbridge/Polymetrics/src/polymarket_etl)
  - Reusable Python logic for the pipeline.
  - This is the right place for parsing, file discovery, SQL builders, API clients, and future transformation helpers.
  - Keeping this logic out of the DAG file is a good choice.

- [sql/snowflake](/home/compute/l.d.stockbridge/Polymetrics/sql/snowflake)
  - Snowflake setup SQL.
  - This is a clean home for bootstrap and warehouse-side definitions.

- [scripts](/home/compute/l.d.stockbridge/Polymetrics/scripts)
  - Data acquisition helpers.
  - The naming suggests a staged extraction workflow:
    - fetch market pages
    - build a market-id list
    - fetch market details and tags

- [data](/home/compute/l.d.stockbridge/Polymetrics/data)
  - Raw local data landing area.
  - The `raw/clob`, `raw/data_api`, and `raw/gamma` split is sensible if you expect multiple Polymarket source systems with different shapes and latency patterns.

- [docs](/home/compute/l.d.stockbridge/Polymetrics/docs)
  - Operational documentation for Airflow and Snowflake setup.
  - This is useful because the repo currently doubles as both application code and its own pipeline runtime.

- [plugins](/home/compute/l.d.stockbridge/Polymetrics/plugins)
  - Placeholder for custom Airflow plugins.
  - Empty-but-present is fine if the repo is intended to act as `AIRFLOW_HOME`.

- [include](/home/compute/l.d.stockbridge/Polymetrics/include)
  - Placeholder for static runtime assets Airflow tasks may reference later.

## Does the Organization Make Sense?

Yes, overall it does.

For an early-stage data platform repo, this is a reasonable structure:

- extraction helpers in `scripts/`
- orchestration in `dags/`
- reusable Python in `src/`
- warehouse SQL in `sql/`
- docs in `docs/`
- raw landed files in `data/`

That separation is healthy and should make the project easier to extend.

## What Feels Strong

- The repo already reflects a pipeline mindset instead of keeping everything in one notebook or shell script.
- `src/polymarket_etl` is a good foundation for growth.
- The source-specific raw data layout under `data/raw/` is a sensible choice.
- The Airflow/Snowflake setup is concrete enough to support repeatable ingestion.

## What Feels Confusing or Fragile

There are a few structural and naming issues that will become more painful as the project grows:

- The repository directory is named `Polymetrics`, but many docs and config values refer to `polymarket`. That mismatch will confuse both humans and Airflow configuration.
- The repo currently mixes source-controlled project code with Airflow runtime state such as `airflow.cfg`, `airflow.db`, and `logs/`. That is acceptable for local experimentation, but it blurs the line between code and runtime artifacts.
- The current docs emphasize the Gamma market-page ingestion path, while the proposal describes a much broader analytics product including user-level trading analysis and live data. The repo structure can support that vision, but the documentation should make clear that ingestion is the current phase, not the entire project.

## Recommended Direction

If the team keeps building on this repo, I would think about the structure in three layers:

1. `scripts/` or a future `extractors/` area for raw API collection.
2. `dags/` plus `src/polymarket_etl/` for orchestration and pipeline logic.
3. `sql/`, dashboard code, and analysis modules for downstream analytics products.

If the dashboard becomes a real application, it will probably deserve its own top-level directory later, such as `app/` or `dashboard/`, rather than being mixed into the ingestion code.

## Summary

My current understanding is:

- PolyMetrics is intended to be a Polymarket data and analytics platform.
- The proposal’s end goal is trader- and market-level analytics, ideally combining historical and live data.
- The current repository is in the warehouse-ingestion phase, centered on loading Polymarket Gamma market data into Snowflake through Airflow.
- The folder structure is broadly good for that phase, with the main weakness being naming/config inconsistency and the presence of local Airflow runtime files in the repo root.

In short, the organization mostly makes sense and gives the project room to grow, but it would benefit from cleaning up the repo/runtime boundary and standardizing the project name across paths and docs.
