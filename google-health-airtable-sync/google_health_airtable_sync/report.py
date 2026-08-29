"""Report rendering for the Google Health → Airtable sync."""

import os
from pathlib import Path

from aaiclick.data.object import Object
from aaiclick.orchestration import task

from .models import AirtablePublishResult


async def render_report(
    activities: Object,
    daily: Object,
    activities_publish: AirtablePublishResult | None,
    daily_publish: AirtablePublishResult | None,
) -> str:
    lines: list[str] = ["# Google Health → Airtable Sync", ""]

    lines.append("## Activities")
    lines.append("")
    count = await (await activities["activity_id"].count()).data()
    if count:
        view = activities[["date", "type", "display_name", "start_time",
                           "duration_min", "distance_km", "calories_kcal", "steps"]]
        lines.append(await view.view(order_by="start_time").markdown())
    else:
        lines.append("_No activities reported in the window._")
    lines.append("")

    lines.append("## Daily Metrics")
    lines.append("")
    days = await (await daily["date"].count()).data()
    if days:
        lines.append(await daily.view(order_by="date").markdown())
    else:
        lines.append("_No daily rollups in the window._")
    lines.append("")

    lines.append("## Airtable")
    lines.append("")
    for label, pub in [("Activities", activities_publish), ("Daily metrics", daily_publish)]:
        if pub is None:
            lines.append(f"- {label}: not requested")
        elif pub.status == "published":
            lines.append(f"- {label}: upserted {pub.rows} rows into {pub.base}/{pub.table}")
        else:
            lines.append(f"- {label}: skipped ({pub.reason})")
    lines.append("")
    return "\n".join(lines)


@task
async def generate_report(
    activities: Object,
    daily: Object,
    activities_publish: AirtablePublishResult | None = None,
    daily_publish: AirtablePublishResult | None = None,
) -> dict:
    rendered = await render_report(activities, daily, activities_publish, daily_publish)
    report_file = os.environ.get("AAICLICK_REPORT_FILE")
    if report_file:
        Path(report_file).parent.mkdir(parents=True, exist_ok=True)
        Path(report_file).write_text(rendered)
    else:
        print(rendered)

    return {
        "activities": await (await activities["activity_id"].count()).data(),
        "days": await (await daily["date"].count()).data(),
        "activities_airtable": activities_publish.status if activities_publish else "skipped",
        "daily_airtable": daily_publish.status if daily_publish else "skipped",
    }
