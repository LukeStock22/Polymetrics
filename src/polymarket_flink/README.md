# polymarket_flink

PyFlink streaming job — reference implementation of the detection pipeline.

## Why this exists alongside the Python consumer

Two implementations of the same logic live in the repo:

- **`polymarket_streaming.kafka_consumer_with_detectors`** (production) —
  single Python process, runs on linuxlab via `nohup`, actually fills
  `DOG_SCHEMA.DETECTED_ANOMALIES`. Simple, deployable on a box with no Docker
  and no systemd.

- **`polymarket_flink.job`** (reference / class content) — the same detection
  logic expressed as Flink operators with keyBy + managed state +
  checkpointing. Demonstrates the "proper" stream-processing architecture.
  Runs on Mac via the docker-compose stack or any real Flink cluster.

Both paths consume the same Kafka topics (`polymarket.trades.raw`,
`polymarket.books.raw`) and share the exact detector math via the
`polymarket_detection` package.

## Topology

```
Kafka(trades.raw)
  ├─► keyBy(market_condition_id) ─► RunDetectorsOnTrade ─► AnomalySink
  └─► Passthrough ──────────────────────────────────────► TradeSink

Kafka(books.raw)
  ├─► keyBy(market_condition_id) ─► UpdateBookState (side-effect)
  └─► Passthrough ──────────────────────────────────────► BookSink
```

`keyBy(market_condition_id)` ensures every event for a given market hits the
same task slot, so the per-market detector state stays coherent without a
distributed store.

## Files

- `job.py` — entry point, Kafka sources, topology, submit to Flink
- `snowflake_sink.py` — `TradeSink`, `BookSink`, `AnomalySink` — batching
  SinkFunctions that wrap `snowflake-connector-python` with key-pair auth
- `detectors_flink.py` — `RunDetectorsOnTrade`, `UpdateBookState`,
  `PassthroughForSink` — wraps the `polymarket_detection` detectors as
  Flink `KeyedProcessFunction`s

## Submitting the job

Stack must be running: `docker compose up -d`.

```bash
# Set Snowflake env in the submitting shell (Flink passes them through)
set -a && source .env.streaming && set +a

# Submit
docker compose exec flink-jobmanager flink run \
  -py /opt/polymarket_flink/job.py

# UI
open http://localhost:8082     # Flink
open http://localhost:8081     # Kafka
```

## Caveats

- **Detector state is in-memory per subtask.** A restart drops state. For
  production-grade correctness you'd port the rolling windows into Flink's
  `ValueState`/`MapState` so checkpoints preserve them. The current class-demo
  setup recovers quickly because all four detectors warm up in seconds.
- **`UpdateBookState` runs in a separate operator** from `RunDetectorsOnTrade`.
  A fully-correct Flink topology would use `connect()` + `CoProcessFunction`
  to share state. Fine for the demo, known-limited for production.
- **Checkpoint interval 60s**, state backend is RocksDB (configured in
  `docker-compose.yml`).
