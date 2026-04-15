# Streamlit App

This repo now includes a minimal local Streamlit app with two questions:

- What are the most popular markets today in the tracked user-activity data?
- What are the highest volume markets in the curated markets table?

The app lets you choose the question from the left sidebar. It supports:

- a daily tracked-volume ranking based on summed `USDCSIZE`
- a highest-volume market ranking based on `GAMMA_MARKETS.volume_num`
- an `Active markets only` filter for the highest-volume ranking
- a click-through trader drill-down for the selected market

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

4. Confirm the table names in the `[app]` section match your Snowflake objects.

Defaults:

- `user_activity_table = "COYOTE_DB.PUBLIC.CURATED_POLYMARKET_USER_ACTIVITY"`
- `markets_table = "PANTHER_DB.CURATED.GAMMA_MARKETS"`

Use fully qualified `DATABASE.SCHEMA.TABLE` names when the app reads from multiple databases. A two-part name such as `COYOTE_DB.CURATED_POLYMARKET_USER_ACTIVITY` is interpreted by Snowflake as `CURRENT_DATABASE.COYOTE_DB.CURATED_POLYMARKET_USER_ACTIVITY`, which is not what you want.

## Run the app

```bash
streamlit run streamlit_app/app.py
```

## Notes

- The app uses the Snowflake session timezone from `.streamlit/secrets.toml` to define "today".
- The tracked-daily question only counts `TRADE` rows from the user-activity table.
- The highest-volume question uses the market table's listed volume field rather than recomputing volume from trade rows.
- If the top markets table is empty, either there were no joined trades for that day or the configured table names are wrong.
