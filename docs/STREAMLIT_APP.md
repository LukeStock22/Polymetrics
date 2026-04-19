# Streamlit App

This repo now includes a local Streamlit app backed by a Snowflake analytics layer, plus a developer-focused data availability page.

The analytics page now supports a broader set of question-driven views selected from the left sidebar.

## Current Data Analysis questions

### Market-centered views

- `Most Popular Markets Today`
- `Markets With The Most Unique Traders Today`
- `Markets With The Largest Average Bet Today`
- `Whale-Dominated Markets Today`
- `Highest Volume Markets`
- `Largest Bets Placed Today`

These views are backed primarily by:

- `PANTHER_DB.ANALYTICS.TRACKED_MARKET_VOLUME_DAILY`
- `PANTHER_DB.ANALYTICS.FACT_MARKET_DAILY`
- `PANTHER_DB.ANALYTICS.HIGHEST_VOLUME_MARKETS`
- `PANTHER_DB.ANALYTICS.FACT_USER_ACTIVITY_TRADES`
- `PANTHER_DB.ANALYTICS.MARKET_CONCENTRATION_DAILY`

They demonstrate:

- day-level market aggregation
- breadth vs concentration of participation
- difference between listed volume and realized traded volume
- click-through market drill-down into trader rankings

### Trader-centered views

- `Top Traders Today`
- `Most Active Traders Today`
- `Who Are The Biggest Traders Overall?`
- `Largest Traders By Average Trade Size`
- `Most Diversified Traders`

These views are backed primarily by:

- `PANTHER_DB.ANALYTICS.FACT_WALLET_DAILY`
- `PANTHER_DB.ANALYTICS.DIM_TRADERS`

They demonstrate:

- large-scale wallet-level aggregation
- distinction between activity, size, and diversification
- how a dimensional trader profile can support multiple downstream questions

### Time-series and profile views

- `How Much Money Was Traded Each Day?`
- `What Separates Big Traders From Small Traders?`
- `Trader Profile Lookup`
- `Wallet Lifecycle / Trader Cohorts`

These views are backed primarily by:

- `PANTHER_DB.ANALYTICS.FACT_MARKET_DAILY`
- `PANTHER_DB.ANALYTICS.DIM_TRADERS`
- `PANTHER_DB.ANALYTICS.FACT_WALLET_DAILY`
- `PANTHER_DB.ANALYTICS.FACT_USER_ACTIVITY_TRADES`
- `PANTHER_DB.ANALYTICS.PLATFORM_DAILY_SUMMARY`
- `PANTHER_DB.ANALYTICS.TRADER_SEGMENT_SNAPSHOT`
- `PANTHER_DB.ANALYTICS.TRADER_COHORT_MONTHLY`

They demonstrate:

- warehouse-driven time-series aggregation
- segmentation analysis over all traders
- granular user lookup with daily history, market footprint, and recent trades

## Why this matters for the project

This question-driven layout is useful for the final presentation because it shows that the project is not just storing data in Snowflake. It is using a modeled analytics layer to answer different classes of questions at different grains:

- market/day
- wallet/day
- wallet/all-time
- market/trader/day
- market/trader/all-time
- raw trade event

That is a strong demonstration of both analytics modeling and scalable warehouse computation.

The newer marts are especially useful when presenting the project:

- `MARKET_CONCENTRATION_DAILY` shows that the warehouse can materialize richer market-structure metrics, not just simple rankings
- `PLATFORM_DAILY_SUMMARY` shows the difference between lower-level facts and app-facing presentation marts
- `TRADER_SEGMENT_SNAPSHOT` shows how the warehouse can turn trader history into reusable behavioral segments
- `TRADER_COHORT_MONTHLY` shows how very large wallet histories can be compressed into lifecycle analytics

Under the hood, those app-facing tables now come from a broader v2 analytics layer built from:

- `PANTHER_DB.CURATED.GAMMA_MARKETS` as the market source of truth
- `COYOTE_DB.PUBLIC.CURATED_POLYMARKET_USER_ACTIVITY` as the user-transaction source of truth
- `DOG_DB` tables as supplemental raw feeds and teammate-provided enrichments

The developer page shows:

- source/base table inventories across DOG, COYOTE, and PANTHER
- analytics-layer table inventories in `PANTHER_DB.ANALYTICS`
- row counts, timestamps, and storage size
- columns and example rows for selected tables
- summary statistics for selected analytics tables

## Files

- `streamlit_app/app.py`: the app
- `requirements-streamlit.txt`: app dependencies
- `.streamlit/secrets.toml.example`: sample local configuration

## Manual setup you must do

1. Install the app dependencies:

```bash
pip install -r requirements-streamlit.txt
```

2. Create a local Streamlit secrets file from the example:

```bash
mkdir -p .streamlit
cp .streamlit/secrets.toml.example .streamlit/secrets.toml
```

3. Edit `.streamlit/secrets.toml` with working Snowflake credentials.

At minimum, `snowflake` needs connection parameters that your account accepts, typically:

- `account`
- `user`
- `password` or another supported auth field such as `private_key_file`
- `warehouse`
- `database`
- `schema`
- optional `role`

4. Build the analytics layer before running the app:

```bash
python src/polymarket_etl/build_analytics_layer.py --config-path .streamlit/secrets.toml
```

5. Confirm the table names in the `[app]` section match your Snowflake objects.

Defaults:

- `user_activity_table = "COYOTE_DB.PUBLIC.CURATED_POLYMARKET_USER_ACTIVITY"`
- `markets_table = "PANTHER_DB.CURATED.GAMMA_MARKETS"`
- `tracked_market_volume_daily_table = "PANTHER_DB.ANALYTICS.TRACKED_MARKET_VOLUME_DAILY"`
- `highest_volume_markets_table = "PANTHER_DB.ANALYTICS.HIGHEST_VOLUME_MARKETS"`
- `market_top_traders_daily_table = "PANTHER_DB.ANALYTICS.MARKET_TOP_TRADERS_DAILY"`
- `market_top_traders_all_time_table = "PANTHER_DB.ANALYTICS.MARKET_TOP_TRADERS_ALL_TIME"`
- `market_concentration_daily_table = "PANTHER_DB.ANALYTICS.MARKET_CONCENTRATION_DAILY"`
- `platform_daily_summary_table = "PANTHER_DB.ANALYTICS.PLATFORM_DAILY_SUMMARY"`
- `trader_segment_snapshot_table = "PANTHER_DB.ANALYTICS.TRADER_SEGMENT_SNAPSHOT"`
- `trader_cohort_monthly_table = "PANTHER_DB.ANALYTICS.TRADER_COHORT_MONTHLY"`

Use fully qualified `DATABASE.SCHEMA.TABLE` names when the app reads from multiple databases. A two-part name such as `COYOTE_DB.CURATED_POLYMARKET_USER_ACTIVITY` is interpreted by Snowflake as `CURRENT_DATABASE.COYOTE_DB.CURATED_POLYMARKET_USER_ACTIVITY`, which is not what you want.

## Run the app

```bash
streamlit run streamlit_app/app.py
```

When Streamlit starts, use the page navigation to switch between:

- the analytics home page
- `Data Availability`

## Notes

- The app uses the Snowflake session timezone from `.streamlit/secrets.toml` to define "today".
- The app reads primarily from analytics tables rather than joining curated source tables live.
- The tracked-daily question only counts `TRADE` rows from the authoritative COYOTE user-activity table.
- The highest-volume question uses the curated market table's listed volume field during analytics-layer build time.
- The trader profile view includes trader market footprint, so a separate `Trader Market Footprint` dropdown item is not necessary.
- The market views already include trader drill-down, so `Top Traders In This Market Across All Time` is covered through selection rather than a separate top-level question.
- The newer concentration, platform summary, segmentation, and cohort views are intentionally materialized in the warehouse so Streamlit can stay presentation-focused.
- If the app returns no rows, rebuild the analytics layer and confirm the analytics tables exist.
