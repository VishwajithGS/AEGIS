"""Publish synthetic river-gauge readings to the ``water-level-events`` topic.

    python -m producers.water_level_producer --rate 1 --duration 60
"""
from producers.base_producer import run

if __name__ == "__main__":
    run({"water_level"}, "AEGIS water-level producer")
