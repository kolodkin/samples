"""Pydantic models for IMDb dataset builder task results."""

from pydantic import BaseModel


class RawProfile(BaseModel):
    total_titles: int
    by_type: dict[str, int]
    adult_count: int
    adult_pct: float


class QualityIssues(BaseModel):
    total_movies: int
    missing_runtime: int
    missing_runtime_pct: float
    short_runtime: int
    long_runtime: int
    year_from: int
    pre_year: int
    pre_year_pct: float


class HFPublishResult(BaseModel):
    status: str
    repo: str
    reason: str | None = None
    rows: int | None = None


class EnrichmentStats(BaseModel):
    total_clean: int
    matched: int
    matched_pct: float
    plots_usable: int
    plots_usable_pct: float
    avg_plot_chars: float


class AirtablePublishResult(BaseModel):
    status: str
    base: str | None = None
    table: str | None = None
    rows: int | None = None
    reason: str | None = None


class AirtableValidationResult(BaseModel):
    status: str  # "ok" | "skipped"
    scopes: list[str] | None = None
    base: str | None = None
    reason: str | None = None
