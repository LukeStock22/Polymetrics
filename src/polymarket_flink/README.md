# polymarket_flink

PyFlink streaming job — **Milestone 2, under construction**.

This package will contain the Flink streaming job that consumes the
Kafka topics produced by `polymarket_streaming.kafka_streamer`, runs
the 4 anomaly detectors as windowed operators, and sinks results to
Snowflake `DOG_DB`.

Planned files:

- `job.py` — `StreamExecutionEnvironment` setup, Kafka sources, sink
  wiring, `env.execute()`
- `snowflake_sink.py` — Python `SinkFunction` wrapping
  `snowflake-connector-python` with per-subtask batched inserts
- `detectors_flink.py` — port of `polymarket_detection.*` detectors
  into Flink `KeyedProcessFunction` / `ProcessWindowFunction` operators

Until this package ships, the interim path
`polymarket_streaming.kafka_consumer_to_snowflake` handles the
Kafka → Snowflake sink (raw landing only, no detectors).
