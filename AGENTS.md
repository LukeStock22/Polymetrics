# Polymarket Airflow Runbook

## Scope

This repo runs the DAG [gamma_markets_to_snowflake.py](/home/compute/l.d.stockbridge/polymarket/dags/gamma_markets_to_snowflake.py) to load Polymarket Gamma files into Snowflake (`PANTHER_DB`).

## Proven Rules

- Run everything in one Linuxlab compute session/host.
- Use `airflow standalone` for this project (not separate scheduler/processor startup).
- Port `8080` must be free before startup.
- If runs are stuck `queued`, treat it as runtime/process issue first.

## Environment

Source env first:

```bash
cd /home/compute/l.d.stockbridge/polymarket
source airflow_home.env.example
```

Expected key settings from env:

- `AIRFLOW_HOME=/home/compute/l.d.stockbridge/polymarket`
- `AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=sqlite:////tmp/polymarket_airflow.db`
- `POLYMARKET_SNOWFLAKE_CONN_ID=Snowflake`
- `POLYMARKET_SNOWFLAKE_DATABASE=PANTHER_DB`

## One-Time / Reconnect Setup

```bash
qlogin-airflow25 -c 4
cd /home/compute/l.d.stockbridge/polymarket
source airflow_home.env.example
airflow db migrate
```

## Reliable Clean Restart Procedure

Run this exact sequence before each fresh DAG trigger:

```bash
cd /home/compute/l.d.stockbridge/polymarket
source airflow_home.env.example

# 1) stop all airflow processes owned by current user
pkill -u "$USER" -f "airflow standalone|airflow scheduler|airflow dag-processor|airflow triggerer|airflow api-server" || true
sleep 2

# 2) force-clear 8080 listeners until free
for i in $(seq 1 20); do
  PIDS=$(netstat -ltnp 2>/dev/null | awk '/:8080/ && /LISTEN/ {split($7,a,"/"); if (a[1] ~ /^[0-9]+$/) print a[1]}' | sort -u)
  if [ -n "$PIDS" ]; then
    echo "attempt $i killing: $PIDS"
    for p in $PIDS; do
      kill -9 "$p" 2>/dev/null || true
    done
  fi

  python -c 'import socket,sys;s=socket.socket();
try:
 s.bind(("127.0.0.1",8080));print("8080 free");sys.exit(0)
except OSError as e:
 print("8080 busy:",e);sys.exit(1)
finally:
 s.close()'
  [ $? -eq 0 ] && break
  sleep 1
done

# 3) fail stale runs in local metadata db
sqlite3 /tmp/polymarket_airflow.db "
UPDATE dag_run
SET state='failed', end_date=CURRENT_TIMESTAMP
WHERE dag_id='gamma_markets_to_snowflake'
  AND state IN ('queued','running');
"

# 4) start stack
nohup airflow standalone > /tmp/polymarket_standalone.log 2>&1 &
sleep 20

# 5) health checks
curl -s http://127.0.0.1:8080/api/v2/monitor/health
airflow dags list | grep gamma_markets_to_snowflake
airflow dags list-import-errors
```

If `8080` remains busy after loop, reallocate node:

```bash
exit
qlogin-airflow25 -c 4
```

## Snowflake Connection (Key Auth)

```bash
airflow connections delete Snowflake || true
airflow connections add Snowflake \
  --conn-type snowflake \
  --conn-description 'Polymarket Snowflake key auth' \
  --conn-login PANTHER \
  --conn-schema PUBLIC \
  --conn-password '' \
  --conn-extra '{"account":"UNB02139","warehouse":"PANTHER_WH","database":"PANTHER_DB","role":"TRAINING_ROLE","private_key_file":"/home/compute/l.d.stockbridge/.snowflake_keys/rsa_key_airflow.p8"}'
airflow connections get Snowflake
```

## Trigger and Monitor

Normal rerun (skip upload step because files already staged):

```bash
airflow dags trigger gamma_markets_to_snowflake --conf '{"skip_upload": true}'
airflow dags list-runs gamma_markets_to_snowflake
```

Set run id and inspect task states:

```bash
RUN_ID='<latest_run_id>'
airflow tasks states-for-dag-run gamma_markets_to_snowflake "$RUN_ID"
```

## Fast Failure Triage

Identify failed task log quickly:

```bash
TASK_ID='<failed_task_id>'
find logs -type f | grep "run_id=$RUN_ID/task_id=$TASK_ID" | tail -n 1 | xargs tail -n 80
```

Runtime-level errors:

```bash
grep -n "ERROR\\|Traceback\\|execution/task-instances\\|Address already in use" /tmp/polymarket_standalone.log | tail -n 60
```

## Snowflake Validation Queries

```sql
SELECT COUNT(*) FROM PANTHER_DB.RAW.GAMMA_MARKET_PAGES;
SELECT COUNT(*) FROM PANTHER_DB.CURATED.GAMMA_MARKETS;
```

## SQL Compatibility Notes (Important)

For this Snowflake environment:

- Avoid `TRY_TO_VARCHAR(...)`.
- Avoid `TRY_CAST(variant AS STRING)` and `TRY_CAST(variant AS BOOLEAN)`.
- Use `variant_field::STRING` for text fields.
- Use `TRY_TO_BOOLEAN(variant_field::STRING)` for booleans.
