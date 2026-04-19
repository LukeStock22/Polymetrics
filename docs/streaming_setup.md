# Streaming Setup (Kafka + Flink)

This runbook gets the live streaming pipeline working end-to-end.
Target host is WUSTL `linuxlab.engr.wustl.edu`; the same commands
work on a laptop for local dev.

```
Polymarket CLOB WS
        │
        ▼
   Python producer           (polymarket_streaming.kafka_streamer)
        │
        ▼
   Kafka topics              polymarket.trades.raw
                             polymarket.books.raw
        │
        ├──► Python consumer  (polymarket_streaming.kafka_consumer_to_snowflake)
        │                      Milestone 1 — landing only
        │
        └──► PyFlink job      (polymarket_flink.job — Milestone 2)
                               detectors + sink
        │
        ▼
   Snowflake DOG_DB
```

## Prereqs

- `~/.snowflake_keys/rsa_key.p8` exists and auth is proven
  (see Snowflake key-pair section in AGENTS.md)
- Docker + Docker Compose available on the host
  (`docker --version && docker compose version`)
- Python 3.10+ with `pip install -r requirements-streaming.txt`

Copy the env template and fill it in:

```bash
cp .env.streaming.example .env.streaming
# no edits needed if DOG / DOG_WH / DOG_DB / TRAINING_ROLE match yours
set -a && source .env.streaming && set +a
```

## Milestone 1 — Kafka producer + Snowflake landing

### 1. Start the stack

```bash
docker compose up -d zookeeper kafka kafka-ui
# give Kafka ~10s to finish startup
docker compose logs kafka --tail=20 | grep "started (kafka.server"
```

Kafka UI: <http://localhost:8081> (or linuxlab equivalent with port forwarding)

### 2. Create the two topics

```bash
docker compose exec kafka kafka-topics \
  --create --bootstrap-server kafka:29092 \
  --topic polymarket.trades.raw \
  --partitions 1 --replication-factor 1 \
  --config retention.ms=604800000

docker compose exec kafka kafka-topics \
  --create --bootstrap-server kafka:29092 \
  --topic polymarket.books.raw \
  --partitions 1 --replication-factor 1 \
  --config retention.ms=604800000

docker compose exec kafka kafka-topics \
  --list --bootstrap-server kafka:29092
```

### 3. Start the producer (WS → Kafka)

In one terminal:

```bash
python -m polymarket_streaming --sink kafka --log-level INFO
```

Expected log lines within ~30s:
- `Loaded N asset IDs from M active markets`
- `Subscribing to N asset IDs`
- `Subscription sent`
- (nothing more until a trade arrives — busy markets produce
  trades every few seconds, sleepy markets can be silent for minutes)

Sanity check: in Kafka UI, open `polymarket.trades.raw` and watch the
message count tick up.

### 4. Start the consumer (Kafka → Snowflake)

In a second terminal (same env sourced):

```bash
python -m polymarket_streaming.kafka_consumer_to_snowflake \
  --flush-interval 30 --flush-max-rows 5000
```

Every 30s (or 5000 rows, whichever first) you should see:

```
Flushed trades=N books=M; offsets committed
```

Verify in Snowflake:

```sql
SELECT COUNT(*), MAX(streamed_at) FROM DOG_DB.DOG_SCHEMA.CLOB_TRADES_STREAM;
SELECT COUNT(*), MAX(streamed_at) FROM DOG_DB.DOG_SCHEMA.CLOB_BOOK_SNAPSHOTS;
```

### 5. Leave it running

For unattended 24/7 on linuxlab, wrap both processes in systemd units
(or use `nohup` + `disown` for a quick hack). See the
"Production deploy" section below.

## Milestone 2 — Flink detectors (in progress)

Work-in-progress. The PyFlink job in `src/polymarket_flink/` will:
- consume both Kafka topics
- run the 4 detectors (volume spike, whale, price impact, timing burst)
  as windowed operators keyed by `market_condition_id`
- sink detector hits to `DOG_SCHEMA.DETECTED_ANOMALIES`
- replace `kafka_consumer_to_snowflake` as the primary trade/book sink

Submit command (once the job is complete):

```bash
docker compose up -d flink-jobmanager flink-taskmanager
docker compose exec flink-jobmanager flink run -py /opt/polymarket_flink/job.py
```

Flink UI: <http://localhost:8082>

## Production deploy (linuxlab, 24/7)

Once Milestone 1 is smoke-tested locally, deploy on linuxlab.

1. `git pull` on linuxlab
2. `docker compose up -d` in the repo root
3. Create the two topics (step 2 above) — one-time
4. systemd units for the producer and consumer (examples under
   `docs/systemd/` once added — TODO). Minimum:

```ini
# ~/.config/systemd/user/polymetrics-producer.service
[Unit]
Description=PolyMetrics WS→Kafka producer
After=docker.service

[Service]
WorkingDirectory=/home/%u/Polymetrics
EnvironmentFile=/home/%u/Polymetrics/.env.streaming
ExecStart=/home/%u/Polymetrics/.venv/bin/python -m polymarket_streaming --sink kafka
Restart=always
RestartSec=10

[Install]
WantedBy=default.target
```

```bash
systemctl --user daemon-reload
systemctl --user enable --now polymetrics-producer polymetrics-consumer
systemctl --user status polymetrics-producer
```

## Troubleshooting

**Producer hangs at "Resolving markets..."** — Snowflake auth isn't
working. Re-test with the Python snippet in AGENTS.md.

**Kafka UI shows 0 messages but producer reports trades** — the
producer is connecting to `localhost:9092` inside a container where
that maps to the container itself, not Kafka. Run the producer on the
host (not inside a Flink container) or set
`KAFKA_BOOTSTRAP_SERVERS=kafka:29092`.

**Consumer commits no offsets** — usually means the flush to
Snowflake failed. Look at the Python traceback in the consumer log;
most likely your Snowflake role doesn't have INSERT on `DOG_SCHEMA.*`.

**Flink job exits immediately** — milestone 2 work; ignore for now.
