"""Fetch tasks: pull Google Health data and land it in aaiclick ``Object``s."""

from datetime import date, timedelta

from aaiclick import FieldSpec, create_object_from_value
from aaiclick.data.object import Object
from aaiclick.orchestration import task

from .health_api import make_client
from .transform import activity_columns, daily_columns

_ACTIVITY_TYPES = {
    "duration_min": FieldSpec(type="Float64"),
    "distance_km": FieldSpec(type="Float64"),
    "calories_kcal": FieldSpec(type="Int64"),
    "steps": FieldSpec(type="Int64"),
}
_DAILY_TYPES = {
    "steps": FieldSpec(type="Int64", nullable=True),
    "weight_kg": FieldSpec(type="Float64", nullable=True),
}


async def new_activities_object(cols: dict[str, list], scope: str | None = "job") -> Object:
    return await create_object_from_value(
        cols, name="ghs_activities", scope=scope, fields=_ACTIVITY_TYPES)


async def new_daily_object(cols: dict[str, list], scope: str | None = "job") -> Object:
    return await create_object_from_value(
        cols, name="ghs_daily", scope=scope, fields=_DAILY_TYPES)


def window(days: int, today: date | None = None) -> tuple[date, date]:
    """Closed-open [start, end) civil-date range: the last ``days`` days
    including today."""
    end = (today or date.today()) + timedelta(days=1)
    return end - timedelta(days=days + 1), end


@task
async def fetch_activities(days: int) -> Object:
    start, _ = window(days)
    client = make_client()
    points = await client.list_exercise_datapoints(f"{start.isoformat()}T00:00:00")
    return await new_activities_object(activity_columns(points))


@task
async def fetch_daily_metrics(days: int) -> Object:
    start, end = window(days)
    client = make_client()
    steps = await client.daily_rollup("steps", start, end)
    weight = await client.daily_rollup("weight", start, end)
    return await new_daily_object(daily_columns(steps, weight))
