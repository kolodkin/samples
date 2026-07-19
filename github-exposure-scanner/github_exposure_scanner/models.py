"""Pydantic models and small parsers for the exposure scanner."""

import os

from pydantic import BaseModel


class Target(BaseModel):
    org: str
    repo: str | None = None


def parse_target(raw: str) -> Target:
    """Parse a target string: ``"org"`` or ``"org/repo"``."""
    cleaned = raw.strip()
    if not cleaned:
        raise ValueError("empty target")
    if "/" in cleaned:
        org, repo = cleaned.split("/", 1)
        return Target(org=org.strip(), repo=repo.strip())
    return Target(org=cleaned)


def parse_repos_env(value: str) -> list[str]:
    """Parse a ``GITHUB_REPOS`` value into pipeline target strings.

    Format: comma-separated entries, each ``"org|repo"`` (a single repo) or a
    bare ``"org"`` (enumerate that org's public repos). Returns strings in the
    internal ``"org/repo"`` / ``"org"`` form that :func:`parse_target` consumes,
    so the pipe format stays confined to the env var. Whitespace is trimmed and
    blank entries skipped, so a trailing comma or an empty value yields ``[]``.
    """
    targets: list[str] = []
    for entry in value.split(","):
        entry = entry.strip()
        if not entry:
            continue
        if "|" in entry:
            org, repo = entry.split("|", 1)
            targets.append(f"{org.strip()}/{repo.strip()}")
        else:
            targets.append(entry)
    return targets


def targets_from_env() -> list[str] | None:
    """Targets from the ``GITHUB_REPOS`` env var, or ``None`` when unset/empty.

    Returning ``None`` (rather than ``[]``) lets callers fall through to their
    own default with a plain ``targets or targets_from_env() or DEFAULT`` chain.
    """
    raw = os.environ.get("GITHUB_REPOS")
    if not raw:
        return None
    return parse_repos_env(raw) or None


class AirtableValidationResult(BaseModel):
    status: str  # "ok" | "skipped"
    base: str | None = None
    reason: str | None = None


class AirtablePublishResult(BaseModel):
    status: str  # "published" | "skipped"
    base: str | None = None
    table: str | None = None
    rows: int | None = None
    reason: str | None = None
