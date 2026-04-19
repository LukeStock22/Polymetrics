"""Dispatcher for `python -m polymarket_streaming`.

Pick the sink with `--sink kafka` (default, WS → Kafka) or
`--sink snowflake` (legacy direct-to-Snowflake path).
"""

from __future__ import annotations

import sys


def _pop_sink_flag() -> str:
    if "--sink" not in sys.argv:
        return "kafka"
    idx = sys.argv.index("--sink")
    if idx + 1 >= len(sys.argv):
        print("--sink requires a value (kafka|snowflake)", file=sys.stderr)
        sys.exit(2)
    value = sys.argv[idx + 1]
    del sys.argv[idx:idx + 2]
    return value


def main() -> None:
    sink = _pop_sink_flag()
    if sink == "kafka":
        from polymarket_streaming.kafka_streamer import main as run
    elif sink == "snowflake":
        from polymarket_streaming.ws_client import main as run
    else:
        print(f"Unknown sink: {sink} (expected kafka|snowflake)", file=sys.stderr)
        sys.exit(2)
    run()


if __name__ == "__main__":
    main()
