"""PyFlink streaming job — reference implementation.

Topology:

  Kafka(trades.raw) ──► keyBy(market) ──► RunDetectorsOnTrade ──► AnomalySink
       │
       └────────────────────────────────► Passthrough ──► TradeSink

  Kafka(books.raw)  ──► keyBy(market) ──► UpdateBookState  (scorer side effect)
       │
       └────────────────────────────────► Passthrough ──► BookSink

This mirrors the production Python consumer (kafka_consumer_with_detectors)
logic, but expressed as Flink operators. Per-market state lives on task
slots via keyBy; checkpointing preserves state across restarts.

Submit:
  docker compose exec flink-jobmanager flink run -py /opt/polymarket_flink/job.py

Env vars read at runtime (passed through from the shell that submits the job):
  KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC_TRADES, KAFKA_TOPIC_BOOKS
  SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_ROLE, SNOWFLAKE_WAREHOUSE,
  SNOWFLAKE_DATABASE, SNOWFLAKE_PRIVATE_KEY_PATH
"""

from __future__ import annotations

import json
import logging
import os
import sys
from pathlib import Path

# Make sibling packages importable when Flink launches this file
_SRC = Path(__file__).resolve().parent.parent
if str(_SRC) not in sys.path:
    sys.path.insert(0, str(_SRC))

from pyflink.common import Types, WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaOffsetsInitializer, KafkaSource

from polymarket_flink.detectors_flink import (
    PassthroughForSink,
    RunDetectorsOnTrade,
    UpdateBookState,
)
from polymarket_flink.snowflake_sink import AnomalySink, BookSink, TradeSink

log = logging.getLogger(__name__)

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
TOPIC_TRADES = os.environ.get("KAFKA_TOPIC_TRADES", "polymarket.trades.raw")
TOPIC_BOOKS = os.environ.get("KAFKA_TOPIC_BOOKS", "polymarket.books.raw")


def _kafka_source(topic: str, group_id: str) -> KafkaSource:
    return (
        KafkaSource.builder()
        .set_bootstrap_servers(KAFKA_BOOTSTRAP)
        .set_topics(topic)
        .set_group_id(group_id)
        .set_starting_offsets(KafkaOffsetsInitializer.latest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )


def _market_key(json_str: str) -> str:
    """Key function — extracts market_condition_id from JSON string."""
    try:
        return json.loads(json_str).get("market_condition_id", "unknown")
    except Exception:
        return "unknown"


def run() -> None:
    env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(60_000)  # 60s
    env.set_parallelism(int(os.environ.get("FLINK_PARALLELISM", "2")))

    # ---- Trades branch ----
    trades = env.from_source(
        _kafka_source(TOPIC_TRADES, "polymetrics-flink-trades"),
        WatermarkStrategy.no_watermarks(),
        "trades-source",
    )

    # Run detectors on keyed-by-market trade stream
    trades_keyed = trades.key_by(_market_key, key_type=Types.STRING())
    anomalies = trades_keyed.process(
        RunDetectorsOnTrade(),
        output_type=Types.STRING(),
    ).name("run-detectors")

    # Raw trade landing sink
    trades.process(PassthroughForSink(), output_type=Types.PICKLED_BYTE_ARRAY()) \
          .add_sink(TradeSink()) \
          .name("trade-sink")

    # Anomaly sink
    anomalies.process(PassthroughForSink(), output_type=Types.PICKLED_BYTE_ARRAY()) \
             .add_sink(AnomalySink()) \
             .name("anomaly-sink")

    # ---- Books branch ----
    books = env.from_source(
        _kafka_source(TOPIC_BOOKS, "polymetrics-flink-books"),
        WatermarkStrategy.no_watermarks(),
        "books-source",
    )

    # Side-effecting operator: updates per-subtask book state for PriceImpact
    books_keyed = books.key_by(_market_key, key_type=Types.STRING())
    books_keyed.process(UpdateBookState(), output_type=Types.STRING()) \
               .name("update-book-state")
    # NB: in a fully-correct Flink topology we'd connect book and trade
    # streams so a single keyed operator owns both state updates. For the
    # class demo this parallel-branch setup is simpler and still works
    # because both operators are keyed by the same market.

    # Raw book landing sink
    books.process(PassthroughForSink(), output_type=Types.PICKLED_BYTE_ARRAY()) \
         .add_sink(BookSink()) \
         .name("book-sink")

    env.execute("polymetrics-streaming-detectors")


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-8s %(name)s: %(message)s",
    )
    run()
