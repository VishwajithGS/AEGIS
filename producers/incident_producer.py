"""Publish synthetic public incident reports to the ``incident-reports`` topic.

Report volume follows the simulated hazard, so it rises during storms.

    python -m producers.incident_producer --duration 300
"""
from producers.base_producer import run

if __name__ == "__main__":
    run({"incidents"}, "AEGIS incident-report producer")
