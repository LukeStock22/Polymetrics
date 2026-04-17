import pandas as pd
import tomllib
from snowflake.snowpark import Session
from snowflake.snowpark.functions import current_date, current_timestamp

def main():
    with open("C:/Users/Wandels/Documents/5114/PolyMarket Project/polymetrics/Polymetrics/.streamlit/secrets.toml", "rb") as f:
        secrets = tomllib.load(f)

    session = Session.builder.configs(secrets["snowflake"]).create()

    df = pd.read_csv("C:/Users/Wandels/Documents/5114/PolyMarket Project/data/polymarket_leaderboard_users_async.csv")


    df = df.rename(columns={"vol": "volume"})

    df.columns = [col.upper() for col in df.columns]

    # Snowpark DataFrame
    snow_df = session.create_dataframe(df)

    snow_df = snow_df.with_column("SNAPSHOT_DATE", current_date()) \
                     .with_column("LOADED_AT", current_timestamp())

    target_table = "COYOTE_DB.PUBLIC.LEADERBOARD_USERS"
    print(f"Uploading to {target_table}...")
    snow_df.write.mode("overwrite").save_as_table(target_table)

    print(f"Successfully uploaded users to {target_table}!")

if __name__ == "__main__":
    main()