"""Pydantic models for movie plot RAG task results."""

from pydantic import BaseModel


class CorpusStats(BaseModel):
    pool_size: int
    corpus_size: int
    avg_plot_chars: float


class EmbeddingInfo(BaseModel):
    model: str
    dims: int
    rows: int


class RagAnswer(BaseModel):
    query: str
    answer: str


class GenerationResult(BaseModel):
    status: str  # "generated" | "skipped"
    model: str | None = None
    reason: str | None = None
    answers: list[RagAnswer] = []
