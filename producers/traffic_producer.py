"""Publish synthetic traffic-segment observations to the ``traffic-events`` topic.

    python -m producers.traffic_producer --rate 2 --duration 60
"""
from producers.base_producer import run

if __name__ == "__main__":
    run({"traffic"}, "AEGIS traffic producer")
