"""Pure transforms: Google Health API JSON → column dicts for aaiclick Objects.

Two shapes come out, matching the two natures of the data:

- ``activity_columns`` — one row per reported exercise session (run, walk, …)
  from a ``dataPoints`` list response.
- ``daily_columns`` — one row per civil day, outer-merging the ``steps`` and
  ``weight`` ``dailyRollUp`` responses; a day missing one metric keeps ``None``
  there so the Airtable upsert can skip the field instead of nulling it.
"""

from datetime import datetime, timedelta

ACTIVITY_FIELDS = [
    "activity_id", "date", "type", "display_name", "start_time", "end_time",
    "duration_min", "distance_km", "calories_kcal", "steps", "source",
]
DAILY_FIELDS = ["date", "steps", "weight_kg"]


def _parse_utc(ts: str) -> datetime:
    return datetime.fromisoformat(ts.replace("Z", "+00:00"))


def _offset_seconds(raw: str | None) -> int:
    """Parse a Duration string like ``"-18000s"`` (defaults to 0)."""
    if not raw:
        return 0
    return int(float(raw.rstrip("s")))


def _civil_date(ts: str, utc_offset: str | None) -> str:
    """The session's local calendar date: UTC instant shifted by its own offset."""
    local = _parse_utc(ts) + timedelta(seconds=_offset_seconds(utc_offset))
    return local.date().isoformat()


def _duration_min(exercise: dict, start: str, end: str) -> float:
    raw = exercise.get("activeDuration")
    if raw:
        return round(_offset_seconds(raw) / 60, 2)
    return round((_parse_utc(end) - _parse_utc(start)).total_seconds() / 60, 2)


def activity_columns(datapoints: list[dict]) -> dict[str, list]:
    """Map exercise ``DataPoint``s to activity rows (see ``ACTIVITY_FIELDS``)."""
    cols: dict[str, list] = {f: [] for f in ACTIVITY_FIELDS}
    for point in datapoints:
        exercise = point.get("exercise") or {}
        interval = exercise.get("interval") or {}
        start, end = interval.get("startTime", ""), interval.get("endTime", "")
        metrics = exercise.get("metricsSummary") or {}
        # the API spells it "distanceMillimiters"; accept the corrected form too
        distance_mm = metrics.get("distanceMillimiters", metrics.get("distanceMillimeters", 0))
        row = {
            "activity_id": point.get("name", "").rsplit("/", 1)[-1],
            "date": _civil_date(start, interval.get("startUtcOffset")),
            "type": exercise.get("exerciseType", ""),
            "display_name": exercise.get("displayName", ""),
            "start_time": start,
            "end_time": end,
            "duration_min": _duration_min(exercise, start, end),
            "distance_km": round(float(distance_mm) / 1_000_000, 3),
            "calories_kcal": int(metrics.get("caloriesKcal", 0)),
            "steps": int(metrics.get("steps", 0)),
            "source": (point.get("dataSource") or {}).get("platform", ""),
        }
        for key, value in row.items():
            cols[key].append(value)
    return cols


def _rollup_date(point: dict) -> str:
    civil = point.get("civilStartTime") or {}
    return f"{civil.get('year', 0):04d}-{civil.get('month', 0):02d}-{civil.get('day', 0):02d}"


def daily_columns(steps_rollup: list[dict], weight_rollup: list[dict]) -> dict[str, list]:
    """Outer-merge the two rollups by civil date (see ``DAILY_FIELDS``)."""
    steps_by_date = {
        _rollup_date(p): int(p["steps"]["countSum"])
        for p in steps_rollup if p.get("steps", {}).get("countSum") is not None
    }
    weight_by_date = {
        _rollup_date(p): round(float(p["weight"]["weightGramsAvg"]) / 1000, 2)
        for p in weight_rollup if p.get("weight", {}).get("weightGramsAvg") is not None
    }
    cols: dict[str, list] = {f: [] for f in DAILY_FIELDS}
    for day in sorted(steps_by_date.keys() | weight_by_date.keys()):
        cols["date"].append(day)
        cols["steps"].append(steps_by_date.get(day))
        cols["weight_kg"].append(weight_by_date.get(day))
    return cols
