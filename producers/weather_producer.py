"""Publish synthetic weather observations to the ``weather-events`` topic.

    python -m producers.weather_producer --rate 1 --duration 60
"""
from producers.base_producer import run

if __name__ == "__main__":
    run({"weather"}, "AEGIS weather producer")
