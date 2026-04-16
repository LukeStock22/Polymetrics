# Streamlit App

This repo now includes a local Streamlit app backed by a Snowflake analytics layer, with two questions:

- What are the most popular markets today in the tracked user-activity data?
- What are the highest volume markets in the curated markets table?

The app lets you choose the question from the left sidebar. It supports:

- a daily tracked-volume ranking based on `ANALYTICS.TRACKED_MARKET_VOLUME_DAILY`
- a highest-volume market ranking based on `ANALYTICS.HIGHEST_VOLUME_MARKETS`
- an `Active markets only` filter for the highest-volume ranking
- a click-through trader drill-down based on `ANALYTICS.MARKET_TOP_TRADERS_DAILY` or `ANALYTICS.MARKET_TOP_TRADERS_ALL_TIME`

Under the hood, those app-facing tables now come from a broader v2 analytics layer built from `DOG_DB` trade/orderbook data plus `PANTHER_DB.CURATED.GAMMA_MARKETS`.

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

Use fully qualified `DATABASE.SCHEMA.TABLE` names when the app reads from multiple databases. A two-part name such as `COYOTE_DB.CURATED_POLYMARKET_USER_ACTIVITY` is interpreted by Snowflake as `CURRENT_DATABASE.COYOTE_DB.CURATED_POLYMARKET_USER_ACTIVITY`, which is not what you want.

## Run the app

```bash
streamlit run streamlit_app/app.py
```

## Notes

- The app uses the Snowflake session timezone from `.streamlit/secrets.toml` to define "today".
- The app reads from analytics tables rather than joining curated source tables live.
- The tracked-daily question only counts `TRADE` rows from the user-activity table.
- The highest-volume question uses the curated market table's listed volume field during analytics-layer build time.
- If the app returns no rows, rebuild the analytics layer and confirm the analytics tables exist.
