# PolyMetrics Plan

## Goal

PolyMetrics should evolve from a single working Airflow DAG that loads one Polymarket data source into Snowflake into a team-friendly analytics project that supports:

1. reliable ingestion of multiple Polymarket data sources
2. warehouse tables that support market- and user-level analysis
3. shared development workflows for teammates working from their own accounts
4. downstream analysis and dashboard work

## Current State

Right now the repo already proves an important first step:

- Airflow can orchestrate a real pipeline
- Snowflake can receive and store Polymarket data
- the Gamma market-page path is working as a first ingestion slice

That is a good foundation, but it is still only a small fraction of the actual project objective in [PROPOSAL.md](/home/compute/l.d.stockbridge/Polymetrics/PROPOSAL.md).

## Guiding Principles

- Keep raw ingestion, transformation logic, warehouse SQL, and analytics outputs clearly separated.
- Treat Snowflake as the shared analytical source of truth.
- Make local Airflow runtime files reproducible, not something that must be committed.
- Document what every teammate must create locally after cloning the repo.
- Expand one source at a time, but design schemas so they can converge into unified market and trader analytics.

## Team Setup Model

Each group member should be able to:

1. clone the GitHub repo into their own home directory
2. reuse ideas and settings from their own `airflow25` folder
3. create local Airflow runtime files that are not stored in GitHub
4. point the repo at their own Snowflake connection and local filesystem paths

The important distinction is:

- GitHub should contain project code, docs, SQL, and templates
- each person must supply their own local runtime artifacts

## What Teammates Will Pull From GitHub

After cloning, teammates should receive:

- DAG code in [dags](/home/compute/l.d.stockbridge/Polymetrics/dags)
- reusable ETL code in [src/polymarket_etl](/home/compute/l.d.stockbridge/Polymetrics/src/polymarket_etl)
- SQL definitions in [sql](/home/compute/l.d.stockbridge/Polymetrics/sql)
- helper scripts in [scripts](/home/compute/l.d.stockbridge/Polymetrics/scripts)
- operational docs in [docs](/home/compute/l.d.stockbridge/Polymetrics/docs)
- setup templates such as [airflow_home.env.example](/home/compute/l.d.stockbridge/Polymetrics/airflow_home.env.example)

## What Teammates Must Supply Locally

After cloning, each teammate will still need to create or configure local items that should not be committed:

- their own Airflow metadata database
- their own Airflow-generated `airflow.cfg` if they run a standalone Airflow home
- their own `logs/`
- their own Airflow user account for the UI if needed
- their own Airflow connection entry named `Snowflake`
- their own Snowflake credentials or key file path
- their own local data files under `data/raw/`
- their own environment exports if their home path differs

In practice, each teammate will likely use their own version of the same pattern you used with `airflow25`:

- start from a working Airflow environment in their account
- inspect their own `~/airflow25/airflow.cfg`
- copy the needed ideas, not the entire runtime state
- point `AIRFLOW_HOME`, `DAGS_FOLDER`, `PLUGINS_FOLDER`, `PYTHONPATH`, and SQLite paths to their own clone

## Recommended Onboarding Flow For Group Members

1. Clone the repo from GitHub into the desired working directory.
2. Open their own local equivalent of `airflow25` and confirm Airflow plus the Snowflake provider already work there.
3. Copy [airflow_home.env.example](/home/compute/l.d.stockbridge/Polymetrics/airflow_home.env.example) into a local, untracked shell file if they need account-specific path changes.
4. Update path values so they point to that user’s clone location.
5. Run `airflow db migrate` in the chosen Airflow home.
6. Create or recreate the Airflow connection named `Snowflake`.
7. Confirm the DAG appears with `airflow dags list`.
8. Place or generate the needed raw data files locally.
9. Trigger the DAG and validate Snowflake output.

## Suggested Near-Term Roadmap

### Phase 1: Stabilize the current ingestion foundation

Objectives:

- make repo naming and path conventions consistent
- ensure setup docs work for any teammate who clones the repo
- standardize how the `Snowflake` connection is defined
- clarify which files are templates versus local runtime artifacts

Concrete work:

- keep [airflow_home.env.example](/home/compute/l.d.stockbridge/Polymetrics/airflow_home.env.example) as the template that teammates edit locally
- add a short teammate setup guide if needed
- decide whether the project should run as its own `AIRFLOW_HOME` or be mounted into an existing `airflow25`-style home
- document one preferred way instead of several partially overlapping ones

### Phase 2: Broaden raw data ingestion

Objectives:

- move beyond Gamma market-page files
- capture more of the raw Polymarket surface area needed by the proposal

Data sources to add:

- market details
- market tags
- CLOB market data
- user positions
- user fills or trade history
- live or near-real-time polling where necessary

Concrete work:

- create separate extractors or scripts for each source
- land each source into `data/raw/<source>/`
- add corresponding raw Snowflake tables
- create one DAG or task group per source rather than forcing everything into one task chain

### Phase 3: Build a warehouse model for analytics

Objectives:

- turn raw landed data into reusable analytical tables
- support the proposal’s questions about profitability, volume, strategy, and user behavior

Suggested warehouse layers:

- `RAW`
  - direct landed API payloads and append-oriented records
- `STAGING`
  - cleaned and normalized records
- `CURATED`
  - business-facing models for dashboards and analysis

Suggested curated entities:

- markets
- market outcomes
- traders or wallets
- positions
- fills or trades
- resolutions
- PnL summaries
- market liquidity and volume summaries

### Phase 4: Support analysis and dashboard work

Objectives:

- make the warehouse useful to the rest of the group
- create a clean handoff from data engineering work to analysis and frontend work

Concrete work:

- define a small set of trusted curated tables for teammates to query
- add example analysis notebooks or SQL queries
- decide whether the dashboard will live in this repo or in a separate app repo
- document the contract between warehouse tables and dashboard inputs

## Recommended Structural Changes Over Time

The current structure is good enough to continue with, but this would scale better:

- `scripts/`
  - ad hoc or batch extraction helpers
- `src/polymarket_etl/`
  - reusable extraction, parsing, transformation, and validation logic
- `dags/`
  - orchestration only
- `sql/`
  - warehouse DDL and transformation SQL
- `docs/`
  - operational and architecture documents
- `analysis/`
  - notebooks, exploratory SQL, and metric definitions
- `dashboard/` or `app/`
  - the eventual analytics UI if kept in the same repo

## Recommended Division Of Work Across Group Members

One useful split would be:

- one person owns Airflow orchestration and local reproducibility
- one person expands extraction coverage across Polymarket endpoints
- one person owns Snowflake modeling and curated tables
- one person focuses on analytics questions, metrics, and dashboard requirements

This does not need to be rigid, but clear ownership will keep the repo from becoming a mix of unrelated experiments.

## Definition Of Success

The project will be much closer to the proposal’s actual objective when the repo can support this workflow:

1. a teammate clones the repo
2. they wire up their own local Airflow runtime using their own `airflow25`-style environment
3. they ingest multiple Polymarket sources into Snowflake
4. curated tables answer trader- and market-level questions
5. those tables power analysis and a dashboard without one-off manual cleanup each time

## Short Version

The current DAG is a successful proof of concept, not the finished project.

The next step is not to add random new scripts; it is to turn this into a reproducible team project with:

- template-based local setup
- broader ingestion coverage
- warehouse models for user and market analytics
- clean handoff to analysis and dashboard development
