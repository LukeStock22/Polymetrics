"""Load sample CSV data into Snowflake and create all streaming/anomaly tables.

Usage:
    python scripts/load_sample_data.py

Required env vars:
    SNOWFLAKE_USER      your Snowflake username
    SNOWFLAKE_PASSWORD   your Snowflake password  (or use SNOWFLAKE_PRIVATE_KEY_PATH)

Optional env vars (defaults match project config):
    SNOWFLAKE_ACCOUNT    default: UNB02139
    SNOWFLAKE_DATABASE   default: DOG_DB
    SNOWFLAKE_WAREHOUSE  default: PANTHER_WH
    SNOWFLAKE_ROLE       default: TRAINING_ROLE
"""

from __future__ import annotations

import csv
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import snowflake.connector

# ── paths ──────────────────────────────────────────────────────────────
REPO_ROOT = Path(__file__).resolve().parent.parent
SQL_DIR = REPO_ROOT / "sql" / "snowflake"
DATA_DIR = REPO_ROOT / "data" / "samples"

DDL_FILES = [
    "01_streaming_tables.sql",
    "02_anomaly_tables.sql",
    "03_trader_profiles.sql",
]

# ── connection ─────────────────────────────────────────────────────────

def get_connection() -> snowflake.connector.SnowflakeConnection:
    user = os.environ.get("SNOWFLAKE_USER", "")
    if not user:
        print("ERROR: SNOWFLAKE_USER env var is required.", file=sys.stderr)
        sys.exit(1)

    params = dict(
        account=os.environ.get("SNOWFLAKE_ACCOUNT", "UNB02139"),
        user=user,
        database=os.environ.get("SNOWFLAKE_DATABASE", "DOG_DB"),
        warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE", "DOG_WH"),
        role=os.environ.get("SNOWFLAKE_ROLE", "TRAINING_ROLE"),
    )

    key_path = os.environ.get("SNOWFLAKE_PRIVATE_KEY_PATH", "")
    if key_path:
        from cryptography.hazmat.primitives import serialization
        key_bytes = Path(key_path).read_bytes()
        private_key = serialization.load_pem_private_key(key_bytes, password=None)
        params["private_key"] = private_key.private_bytes(
            encoding=serialization.Encoding.DER,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption(),
        )
    else:
        password = os.environ.get("SNOWFLAKE_PASSWORD", "")
        if not password:
            print("ERROR: SNOWFLAKE_PASSWORD or SNOWFLAKE_PRIVATE_KEY_PATH is required.",
                  file=sys.stderr)
            sys.exit(1)
        params["password"] = password
        passcode = os.environ.get("SNOWFLAKE_MFA_PASSCODE", "")
        if not passcode:
            passcode = input("Enter Duo TOTP code: ").strip()
        params["passcode"] = passcode

    conn = snowflake.connector.connect(**params)
    print(f"Connected to {params['database']} as {params['user']} "
          f"(account={params['account']}, warehouse={params['warehouse']}, "
          f"role={params['role']})")
    return conn


# ── DDL ────────────────────────────────────────────────────────────────

def run_ddl(conn: snowflake.connector.SnowflakeConnection) -> list[str]:
    """Execute each DDL file. Returns list of tables created."""
    database = os.environ.get("SNOWFLAKE_DATABASE", "DOG_DB")
    tables_created: list[str] = []
    cur = conn.cursor()
    try:
        # Ensure schemas exist in our database
        for schema in ("DOG_SCHEMA", "DOG_TRANSFORM"):
            cur.execute(f"CREATE SCHEMA IF NOT EXISTS {database}.{schema}")
            print(f"  OK: schema {database}.{schema}")

        for filename in DDL_FILES:
            path = SQL_DIR / filename
            sql_text = path.read_text()
            print(f"\n── Running {filename} ──")
            # Strip comment lines before splitting to avoid semicolons in comments
            cleaned_lines = []
            for line in sql_text.splitlines():
                stripped = line.strip()
                if stripped.startswith("--"):
                    continue
                cleaned_lines.append(line)
            cleaned_sql = "\n".join(cleaned_lines)
            for stmt in cleaned_sql.split(";"):
                stmt = stmt.strip()
                if not stmt:
                    continue
                cur.execute(stmt)
                # extract table name from CREATE TABLE statements
                upper = stmt.upper()
                if "CREATE TABLE" in upper:
                    # parse "CREATE TABLE IF NOT EXISTS SCHEMA.TABLE_NAME ("
                    parts = upper.split("(")[0].split()
                    table_name = parts[-1]  # last word before "("
                    tables_created.append(table_name)
                    print(f"  OK: {table_name}")
    finally:
        cur.close()
    return tables_created


# ── CSV loading ────────────────────────────────────────────────────────

def epoch_to_timestamp(epoch_str: str) -> str:
    """Convert epoch seconds string to ISO timestamp string."""
    return datetime.fromtimestamp(int(epoch_str), tz=timezone.utc).strftime(
        "%Y-%m-%d %H:%M:%S"
    )


def load_trades(conn: snowflake.connector.SnowflakeConnection) -> int:
    """Load trades_sample_100.csv into DOG_SCHEMA.DATA_API_TRADES. Returns row count."""
    csv_path = DATA_DIR / "trades_sample_100.csv"
    if not csv_path.exists():
        print(f"  SKIP: {csv_path} not found")
        return 0

    print(f"\n── Loading {csv_path.name} → DOG_SCHEMA.DATA_API_TRADES ──")

    rows: list[tuple] = []
    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            raw_payload = json.dumps(row)
            rows.append((
                row["proxyWallet"],                  # proxy_wallet
                row["conditionId"],                  # condition_id
                row["asset"],                        # asset
                row["side"],                         # side
                row["outcome"],                      # outcome
                int(row["outcomeIndex"]),             # outcome_index
                float(row["price"]),                  # price
                float(row["size"]),                   # size
                row["title"],                        # title
                row["slug"],                         # slug
                row["eventSlug"],                    # event_slug
                row["transactionHash"],              # transaction_hash
                epoch_to_timestamp(row["timestamp"]), # trade_timestamp
                raw_payload,                          # raw_payload
            ))

    sql = """
        INSERT INTO DOG_SCHEMA.DATA_API_TRADES
            (proxy_wallet, condition_id, asset, side, outcome, outcome_index,
             price, size, title, slug, event_slug, transaction_hash,
             trade_timestamp, raw_payload)
        SELECT
            column1, column2, column3, column4, column5,
            column6::NUMBER(2,0),
            column7::NUMBER(18,6), column8::NUMBER(38,12),
            column9, column10, column11, column12,
            column13::TIMESTAMP_NTZ,
            PARSE_JSON(column14)
        FROM VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    cur = conn.cursor()
    try:
        cur.executemany(sql, rows)
        print(f"  Inserted {len(rows)} rows into DOG_SCHEMA.DATA_API_TRADES")
    finally:
        cur.close()
    return len(rows)


def load_leaderboard(conn: snowflake.connector.SnowflakeConnection) -> int:
    """Load leaderboard_sample_100.csv into DOG_SCHEMA.LEADERBOARD_USERS. Returns row count."""
    csv_path = DATA_DIR / "leaderboard_sample_100.csv"
    if not csv_path.exists():
        print(f"  SKIP: {csv_path} not found")
        return 0

    print(f"\n── Loading {csv_path.name} → DOG_SCHEMA.LEADERBOARD_USERS ──")

    rows: list[tuple] = []
    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append((
                row["proxyWallet"],                          # proxy_wallet
                int(row["rank"]),                             # rank
                row["userName"] or None,                     # user_name
                row["xUsername"] or None,                    # x_username
                row["verifiedBadge"].strip().lower() == "true",  # verified_badge
                float(row["vol"]) if row["vol"] else None,   # volume
                float(row["pnl"]) if row["pnl"] else None,   # pnl
                row["profileImage"] or None,                 # profile_image
            ))

    sql = """
        INSERT INTO DOG_SCHEMA.LEADERBOARD_USERS
            (proxy_wallet, rank, user_name, x_username, verified_badge,
             volume, pnl, profile_image)
        SELECT
            column1, column2::NUMBER(10,0), column3, column4,
            column5::BOOLEAN,
            column6::NUMBER(38,12), column7::NUMBER(38,12),
            column8
        FROM VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """

    cur = conn.cursor()
    try:
        cur.executemany(sql, rows)
        print(f"  Inserted {len(rows)} rows into DOG_SCHEMA.LEADERBOARD_USERS")
    finally:
        cur.close()
    return len(rows)


# ── summary ────────────────────────────────────────────────────────────

def print_summary(conn: snowflake.connector.SnowflakeConnection,
                  tables_created: list[str],
                  trades_loaded: int,
                  lb_loaded: int) -> None:
    """Print a summary suitable for sharing with the project lead."""
    user = os.environ.get("SNOWFLAKE_USER", "unknown")
    database = os.environ.get("SNOWFLAKE_DATABASE", "DOG_DB")
    role = os.environ.get("SNOWFLAKE_ROLE", "TRAINING_ROLE")

    # query row counts for loaded tables
    cur = conn.cursor()
    counts: dict[str, int] = {}
    for table in ["DOG_SCHEMA.DATA_API_TRADES", "DOG_SCHEMA.LEADERBOARD_USERS"]:
        try:
            cur.execute(f"SELECT COUNT(*) FROM {table}")
            counts[table] = cur.fetchone()[0]
        except Exception:
            counts[table] = 0
    cur.close()

    print("\n" + "=" * 64)
    print("  SNOWFLAKE SUMMARY — for project lead")
    print("=" * 64)
    print(f"  Database:   {database}")
    print(f"  User:       {user}")
    print(f"  Role:       {role}")
    print()
    print("  SCHEMAS:")
    print(f"    - {database}.RAW        (raw ingested data)")
    print(f"    - {database}.DOG_TRANSFORM    (transformed/aggregated data)")
    print()
    print("  TABLES CREATED (RAW schema — streaming & ingestion):")
    print(f"    - DOG_SCHEMA.CLOB_TRADES_STREAM      websocket trade events")
    print(f"    - DOG_SCHEMA.CLOB_BOOK_SNAPSHOTS     orderbook snapshots")
    print(f"    - DOG_SCHEMA.DATA_API_TRADES          Data API trades (wallet-attributed)")
    print(f"    - DOG_SCHEMA.LEADERBOARD_USERS        leaderboard snapshots")
    print(f"    - DOG_SCHEMA.DETECTED_ANOMALIES       real-time anomaly events")
    print()
    print("  TABLES CREATED (DOG_TRANSFORM schema — analytics):")
    print(f"    - DOG_TRANSFORM.MARKET_DAILY_STATS   daily per-market aggregations")
    print(f"    - DOG_TRANSFORM.WALLET_DAILY_STATS   daily per-wallet aggregations")
    print(f"    - DOG_TRANSFORM.ANOMALY_ATTRIBUTION  anomaly-to-wallet linkage")
    print(f"    - DOG_TRANSFORM.TRADER_RISK_PROFILES composite trader risk scores")
    print()
    print("  DATA LOADED:")
    print(f"    - DOG_SCHEMA.DATA_API_TRADES:    {trades_loaded} rows inserted "
          f"({counts.get('DOG_SCHEMA.DATA_API_TRADES', '?')} total in table)")
    print(f"    - DOG_SCHEMA.LEADERBOARD_USERS:  {lb_loaded} rows inserted "
          f"({counts.get('DOG_SCHEMA.LEADERBOARD_USERS', '?')} total in table)")
    print("=" * 64)


# ── main ───────────────────────────────────────────────────────────────

def main() -> None:
    conn = get_connection()
    try:
        tables_created = run_ddl(conn)
        trades_loaded = load_trades(conn)
        lb_loaded = load_leaderboard(conn)
        print_summary(conn, tables_created, trades_loaded, lb_loaded)
    finally:
        conn.close()
        print("\nConnection closed.")


if __name__ == "__main__":
    main()
