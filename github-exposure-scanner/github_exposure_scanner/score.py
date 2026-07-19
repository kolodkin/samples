"""Per-org exposure scoring — SQL aggregation plus a small scoring formula.

Heavy grouping runs in ClickHouse (via ``Object`` group-bys); the score and
risk band are computed in Python so the formula stays unit-testable.
"""

import math

from aaiclick import ORIENT_DICT, FieldSpec, create_object_from_value
from aaiclick.data.models import Computed
from aaiclick.data.object import Object
from aaiclick.orchestration import task

from .rules import SEVERITY_WEIGHT

SUMMARY_FIELDS = [
    "org", "repos_scanned", "files_scanned", "total_findings", "critical",
    "high", "medium", "low", "likely_test", "exposure_score", "risk_band",
    "top_secret_type", "scan_errors",
]

_SUMMARY_TYPES = {
    "repos_scanned": FieldSpec(type="Int64"),
    "files_scanned": FieldSpec(type="Int64"),
    "total_findings": FieldSpec(type="Int64"),
    "critical": FieldSpec(type="Int64"),
    "high": FieldSpec(type="Int64"),
    "medium": FieldSpec(type="Int64"),
    "low": FieldSpec(type="Int64"),
    "likely_test": FieldSpec(type="Int64"),
    "exposure_score": FieldSpec(type="Int64"),
    "scan_errors": FieldSpec(type="Int64"),
}


def risk_band(score: int) -> str:
    if score <= 0:
        return "Clean"
    if score < 10:
        return "Low"
    if score < 30:
        return "Medium"
    if score < 80:
        return "High"
    return "Critical"


def compute_score(weighted_base: float, flagged_stars: int) -> int:
    """Scale a confidence-weighted severity base by a star-based blast factor.

    ``weighted_base`` is ``Σ severity_weight × confidence`` across an org's
    findings — so a low-confidence (likely-test) finding contributes far less
    than a full-confidence production leak of the same severity.
    """
    return round(weighted_base * (1 + math.log10(1 + flagged_stars)))


async def score_exposure_impl(repos: Object, findings: Object, scope: str | None = "job") -> Object:
    # Per-org repo aggregates via SQL. A computed ``is_error`` flag (1 when
    # ``list_error`` is set) is summed per org to count scan errors — ``count``
    # would count rows, not the non-null errors.
    typed = repos.with_columns({"is_error": Computed("UInt8", "list_error IS NOT NULL")})
    repo_agg = await typed.group_by("org").agg(
        {"repo": "count", "files_to_scan": "sum", "is_error": "sum"}
    )
    repo_data = await repo_agg.data(orient=ORIENT_DICT)

    finding_data = await findings.data(orient=ORIENT_DICT)

    # Every finding comes from a scanned repo, so its org is always one of the
    # group-by keys below — no defensive fallback bucket is needed.
    per_org: dict[str, dict] = {
        org: {
            "repos_scanned": repo_data["repo"][idx],
            "files_scanned": repo_data["files_to_scan"][idx],
            "scan_errors": repo_data["is_error"][idx],
            "counts": {"Critical": 0, "High": 0, "Medium": 0, "Low": 0},
            "weighted_base": 0.0,
            "likely_test": 0,
            "types": {},
            "flagged_repos": set(),
        }
        for idx, org in enumerate(repo_data["org"])
    }

    for i in range(len(finding_data["org"])):
        bucket = per_org[finding_data["org"][i]]
        sev = finding_data["severity"][i]
        confidence = finding_data["confidence"][i]
        bucket["counts"][sev] += 1
        bucket["weighted_base"] += SEVERITY_WEIGHT[sev] * confidence
        if confidence < 1.0:
            bucket["likely_test"] += 1
        stype = finding_data["secret_type"][i]
        bucket["types"][stype] = bucket["types"].get(stype, 0) + 1
        bucket["flagged_repos"].add((finding_data["repo"][i], finding_data["repo_stars"][i]))

    cols: dict[str, list] = {f: [] for f in SUMMARY_FIELDS}
    for org, b in per_org.items():
        counts = b["counts"]
        total = sum(counts.values())
        flagged_stars = sum(stars for _, stars in b["flagged_repos"])
        score = compute_score(b["weighted_base"], flagged_stars)
        top_type = max(b["types"], key=b["types"].get) if b["types"] else ""
        cols["org"].append(org)
        cols["repos_scanned"].append(int(b["repos_scanned"]))
        cols["files_scanned"].append(int(b["files_scanned"]))
        cols["total_findings"].append(int(total))
        cols["critical"].append(counts["Critical"])
        cols["high"].append(counts["High"])
        cols["medium"].append(counts["Medium"])
        cols["low"].append(counts["Low"])
        cols["likely_test"].append(int(b["likely_test"]))
        cols["exposure_score"].append(score)
        cols["risk_band"].append(risk_band(score))
        cols["top_secret_type"].append(top_type)
        cols["scan_errors"].append(int(b["scan_errors"]))

    return await create_object_from_value(cols, name="ghx_summary", scope=scope, fields=_SUMMARY_TYPES)


@task
async def score_exposure(repos: Object, findings: Object) -> Object:
    return await score_exposure_impl(repos, findings)
