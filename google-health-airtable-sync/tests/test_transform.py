import json
import os

from google_health_airtable_sync.transform import activity_columns, daily_columns

FIXTURES = os.path.join(os.path.dirname(__file__), "fixtures")


def _load(name):
    with open(os.path.join(FIXTURES, name), encoding="utf-8") as f:
        return json.load(f)


def test_activity_columns_maps_exercise_datapoints():
    cols = activity_columns(_load("exercise_datapoints.json")["dataPoints"])
    assert cols["activity_id"] == ["8896720705097069096", "5870930690409355408"]
    assert cols["type"] == ["RUNNING", "WALKING"]
    assert cols["display_name"] == ["Run", "Walk"]
    # civil date uses the session's own UTC offset (-05:00), not UTC
    assert cols["date"] == ["2026-08-22", "2026-08-23"]
    assert cols["start_time"] == ["2026-08-22T06:10:00Z", "2026-08-23T13:10:00Z"]
    assert cols["duration_min"] == [35.0, 15.0]
    assert cols["distance_km"] == [5.23, 1.609]
    assert cols["calories_kcal"] == [412, 16]
    assert cols["steps"] == [6210, 2038]
    assert cols["source"] == ["FITBIT", "FITBIT"]


def test_activity_columns_tolerates_missing_metrics():
    point = {
        "name": "users/me/dataTypes/exercise/dataPoints/42",
        "exercise": {
            "interval": {"startTime": "2026-08-20T10:00:00Z", "startUtcOffset": "0s",
                         "endTime": "2026-08-20T10:30:00Z", "endUtcOffset": "0s"},
            "exerciseType": "YOGA",
        },
    }
    cols = activity_columns([point])
    assert cols["activity_id"] == ["42"]
    assert cols["date"] == ["2026-08-20"]
    assert cols["duration_min"] == [30.0]  # falls back to interval length
    assert cols["distance_km"] == [0.0]
    assert cols["calories_kcal"] == [0]
    assert cols["steps"] == [0]


def test_daily_columns_outer_merges_steps_and_weight_by_date():
    cols = daily_columns(
        _load("steps_daily_rollup.json")["rollupDataPoints"],
        _load("weight_daily_rollup.json")["rollupDataPoints"],
    )
    # union of dates, sorted: steps on 22+23, weight on 22+24
    assert cols["date"] == ["2026-08-22", "2026-08-23", "2026-08-24"]
    assert cols["steps"] == [9425, 6376, None]
    assert cols["weight_kg"] == [78.45, None, 78.1]


def test_daily_columns_empty_inputs():
    cols = daily_columns([], [])
    assert cols["date"] == [] and cols["steps"] == [] and cols["weight_kg"] == []
