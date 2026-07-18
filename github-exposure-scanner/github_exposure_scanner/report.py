"""Report rendering for the GitHub exposure scanner."""

import os
from pathlib import Path

from aaiclick.data.object import Object
from aaiclick.orchestration import task

from .models import AirtablePublishResult


async def render_report(
    repos: Object,
    findings: Object,
    summary: Object,
    findings_publish: AirtablePublishResult | None,
    summary_publish: AirtablePublishResult | None,
) -> str:
    lines: list[str] = ["# Company Cyber Profile", "", "## GitHub Exposure", ""]

    lines.append("### Exposure Summary")
    lines.append("")
    lines.append(await summary.markdown())
    lines.append("")

    lines.append("### Scanned Repositories")
    lines.append("")
    repos_view = repos[["org", "repo", "stars", "language", "files_to_scan", "list_error"]]
    lines.append(await repos_view.markdown(truncate={"list_error": 40}))
    lines.append("")

    lines.append("### Findings (redacted)")
    lines.append("")
    total_findings = await (await findings["org"].count()).data()
    if total_findings:
        findings_view = findings[
            ["org", "repo", "path", "line", "secret_type", "severity", "confidence", "context", "masked_value"]
        ].view(order_by="confidence DESC, repo_stars DESC", limit=100)
        lines.append(await findings_view.markdown(truncate={"path": 40, "context": 40}))
    else:
        lines.append("_No leaked secrets detected._")
    lines.append("")

    lines.append("### Airtable")
    lines.append("")
    for label, pub in [("Findings", findings_publish), ("Summary", summary_publish)]:
        if pub is None:
            lines.append(f"- {label}: not requested")
        elif pub.status == "published":
            lines.append(f"- {label}: published {pub.rows} rows to {pub.base}/{pub.table}")
        else:
            lines.append(f"- {label}: skipped ({pub.reason})")
    lines.append("")
    lines.append(
        "> Secrets are shown only as masked fingerprints. This scan reads public "
        "data for defensive attack-surface assessment."
    )
    return "\n".join(lines)


@task
async def generate_report(
    repos: Object,
    findings: Object,
    summary: Object,
    findings_publish: AirtablePublishResult | None = None,
    summary_publish: AirtablePublishResult | None = None,
) -> dict:
    rendered = await render_report(repos, findings, summary, findings_publish, summary_publish)
    report_file = os.environ.get("AAICLICK_REPORT_FILE")
    if report_file:
        Path(report_file).parent.mkdir(parents=True, exist_ok=True)
        Path(report_file).write_text(rendered)
    else:
        print(rendered)

    total_findings = await (await findings["org"].count()).data()
    return {
        "repos_scanned": await (await repos["org"].count()).data(),
        "total_findings": total_findings,
        "findings_airtable": findings_publish.status if findings_publish else "skipped",
        "summary_airtable": summary_publish.status if summary_publish else "skipped",
    }
