"""Pydantic models and small parsers for the exposure scanner."""

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
