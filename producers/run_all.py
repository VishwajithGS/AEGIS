"""Publish all four synthetic sources from one process (one shared world state).

    python -m producers.run_all --rate 4 --duration 600
"""
from producers.base_producer import run
from producers.data_generator import TOPIC_KEYS

if __name__ == "__main__":
    run(set(TOPIC_KEYS), "AEGIS all-source producer")
