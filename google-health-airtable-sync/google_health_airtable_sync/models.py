"""Pydantic result models passed between tasks."""

from pydantic import BaseModel


class AirtableValidationResult(BaseModel):
    status: str  # "ok" | "skipped"
    base: str | None = None
    reason: str | None = None


class AirtablePublishResult(BaseModel):
    status: str  # "published" | "skipped"
    base: str | None = None
    table: str | None = None
    rows: int = 0
    reason: str | None = None
