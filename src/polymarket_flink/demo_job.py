"""Minimal PyFlink demo job for Mac docker-compose presentations.

Purpose: show Flink consuming the same Polymarket Kafka stream that the
production Python consumer uses on linuxlab, with a visible DAG in the
Flink UI (localhost:8082) — but with a print-only sink so it does NOT
double-write to Snowflake.

Topology:
    Kafka(polymarket.trades.raw)  ─►  JSON parse  ─►  print sink
    Kafka(polymarket.books.raw)   ─►  JSON parse  ─►  print sink

Submit from the Mac:
    docker compose exec flink-jobmanager \
        flink run -py /opt/polymarket_flink/demo_job.py

View:
    Flink UI      http://localhost:8082
    Kafka UI      http://localhost:8081
"""

from __future__ import annotations

import json
import os

from pyflink.common.serialization import SimpleStringSchema
from pyflink.common import WatermarkStrategy
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaOffsetsInitializer, KafkaSource

# Inside compose network, kafka is reachable as "kafka:29092"
KAFKA = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
TOPIC_TRADES = os.environ.get("KAFKA_TOPIC_TRADES", "polymarket.trades.raw")
TOPIC_BOOKS = os.environ.get("KAFKA_TOPIC_BOOKS", "polymarket.books.raw")


def _kafka_source(topic: str, group_id: str) -> KafkaSource:
    return (
        KafkaSource.builder()
        .set_bootstrap_servers(KAFKA)
        .set_topics(topic)
        .set_group_id(group_id)
        .set_starting_offsets(KafkaOffsetsInitializer.latest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )


def _summarize_trade(json_str: str) -> str:
    try:
        d = json.loads(json_str)
        return (
            f"TRADE "
            f"market={d.get('market_condition_id','?')[:14]}… "
            f"side={d.get('side','?')} "
            f"price={d.get('price','?'):.4f} "
            f"size={d.get('size','?'):.2f}"
        )
    except Exception:
        return f"TRADE(unparsed): {json_str[:120]}"


def _summarize_book(json_str: str) -> str:
    try:
        d = json.loads(json_str)
        return (
            f"BOOK  "
            f"market={d.get('market_condition_id','?')[:14]}… "
            f"best_bid={d.get('best_bid','?')} "
            f"best_ask={d.get('best_ask','?')} "
            f"spread={d.get('spread','?')}"
        )
    except Exception:
        return f"BOOK(unparsed): {json_str[:120]}"


def run() -> None:
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(int(os.environ.get("FLINK_PARALLELISM", "1")))

    trades = (
        env.from_source(
            _kafka_source(TOPIC_TRADES, "polymetrics-flink-demo-trades"),
            WatermarkStrategy.no_watermarks(),
            "kafka-trades",
        )
        .map(_summarize_trade)
        .name("summarize-trade")
    )
    trades.print().name("trades-print")

    books = (
        env.from_source(
            _kafka_source(TOPIC_BOOKS, "polymetrics-flink-demo-books"),
            WatermarkStrategy.no_watermarks(),
            "kafka-books",
        )
        .map(_summarize_book)
        .name("summarize-book")
    )
    books.print().name("books-print")

    env.execute("polymetrics-flink-demo")


if __name__ == "__main__":
    run()
