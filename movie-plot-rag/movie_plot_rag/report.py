"""Movie plot RAG report generation."""

import os
import sys
from contextlib import redirect_stdout
from dataclasses import dataclass
from io import StringIO
from pathlib import Path

from aaiclick.data.models import ColumnInfo, Computed
from aaiclick.data.object import Object
from aaiclick.orchestration import task

from .constants import CORPUS_COLUMNS, EMBEDDED_COLUMNS, MIN_VOTES, QUERIES, TMDB_URL
from .models import CorpusStats, EmbeddingInfo, GenerationResult

# Renders the first few vector components so the report shows embeddings are
# real stored data, without dumping 384 floats per row.
_EMBEDDING_PREVIEW = (
    "concat('[', arrayStringConcat(arrayMap(x -> toString(round(x, 3)), arraySlice(embedding, 1, 4)), ', '), ', …]')"
)


def _fmt(value: object) -> str:
    """Format a numeric value for display."""
    if isinstance(value, float):
        return f"{value:,.2f}"
    return f"{value:,}" if isinstance(value, int) else str(value)


def _print_field_table(columns: dict[str, ColumnInfo]) -> None:
    """Print a markdown table of field names, types, and descriptions."""
    described = {f: c for f, c in columns.items() if c.description}
    if not described:
        return
    name_w = max(len("Field"), max(len(f) for f in described))
    type_w = max(len("Type"), max(len(c.ch_type()) for c in described.values()))
    desc_w = max(len("Description"), max(len(c.description) for c in described.values()))
    print(f"| {'Field':<{name_w}s} | {'Type':<{type_w}s} | {'Description':<{desc_w}s} |")
    print(f"|{'-' * (name_w + 2)}|{'-' * (type_w + 2)}|{'-' * (desc_w + 2)}|")
    for field, col in described.items():
        print(f"| {field:<{name_w}s} | {col.ch_type():<{type_w}s} | {col.description:<{desc_w}s} |")


@dataclass
class ReportContent:
    """Pre-rendered report sections passed into ``_print_report``."""

    stats: CorpusStats
    embedding_info: EmbeddingInfo
    top_k: int
    corpus_md: str
    embedded_md: str
    query_mds: dict[str, str]
    generation: GenerationResult | None


def _print_report(content: ReportContent) -> None:
    """Print the movie plot RAG report as markdown."""
    stats = content.stats
    info = content.embedding_info

    print("\n## Movie Plot RAG\n")

    print("### Corpus\n")
    print(f"URL: {TMDB_URL}")
    print(f"Pool (≥ {_fmt(MIN_VOTES)} votes, usable synopsis): {_fmt(stats.pool_size)} movies")
    print(f"Corpus (top by IMDb vote count): {_fmt(stats.corpus_size)} movies")
    print(f"Average plot length: {_fmt(stats.avg_plot_chars)} characters\n")

    print("#### Field Schema\n")
    _print_field_table(CORPUS_COLUMNS)

    print("\n#### Sample (top 5 by votes)\n")
    print(content.corpus_md)

    print("\n### Embeddings\n")
    print(f"- Model: `{info.model}` (local, CPU)")
    print(f"- Vectors stored: {_fmt(info.rows)} × {info.dims} dims, `Array(Float32)` column in ClickHouse")
    print("- Similarity: exact brute-force `1 - cosineDistance(embedding, query)` in SQL\n")

    print("#### Field Schema\n")
    _print_field_table(EMBEDDED_COLUMNS)

    print("\n#### Sample (first 3 rows)\n")
    print(content.embedded_md)

    print(f"\n### Semantic Search (top {content.top_k} per query)\n")
    for query in QUERIES:
        print(f'**"{query}"**\n')
        print(content.query_mds[query])
        print()

    print("### Generated Answers\n")
    generation = content.generation
    if generation is None:
        print("- Skipped: run with `--generate`. Needs an AI provider aaiclick can")
        print("  reach — `AAICLICK_AI_API_KEY` with a hosted `AAICLICK_AI_MODEL`, or a")
        print("  local Ollama server (`python -m aaiclick setup --ai`).")
    elif generation.status == "generated":
        print(f"Model: `{generation.model}`\n")
        for answer in generation.answers:
            print(f'**"{answer.query}"**\n')
            print(f"{answer.answer}\n")
    else:
        print(f"- Status: {generation.status}")
        if generation.reason:
            print(f"- Reason: {generation.reason}")


@task
async def generate_report(
    corpus: Object,
    embedded: Object,
    results: Object,
    stats: CorpusStats,
    embedding_info: EmbeddingInfo,
    top_k: int,
    generation: GenerationResult | None = None,
) -> dict:
    """Combine all pipeline outputs into a unified RAG report."""
    corpus_md = (
        await corpus[["title", "year", "genres", "numVotes", "plot"]]
        .view(order_by="numVotes DESC", limit=5)
        .markdown(truncate={"title": 30, "genres": 24, "plot": 90})
    )

    embedded_preview = embedded.with_columns({"embedding_preview": Computed("String", _EMBEDDING_PREVIEW)})
    embedded_md = (
        await embedded_preview[["title", "year", "embedding_preview"]]
        .view(order_by="numVotes DESC", limit=3)
        .markdown(truncate={"title": 30})
    )

    query_mds: dict[str, str] = {}
    for query in QUERIES:
        quoted = "'" + query.replace("\\", "\\\\").replace("'", "\\'") + "'"
        query_mds[query] = (
            await results.where(f"query = {quoted}")[["rank", "title", "year", "similarity", "plot"]]
            .view(order_by="rank", limit=top_k)
            .markdown(truncate={"title": 30, "plot": 90})
        )

    buf = StringIO()
    with redirect_stdout(buf):
        _print_report(
            ReportContent(
                stats=stats,
                embedding_info=embedding_info,
                top_k=top_k,
                corpus_md=corpus_md,
                embedded_md=embedded_md,
                query_mds=query_mds,
                generation=generation,
            )
        )
    rendered = buf.getvalue()

    report_file = os.environ.get("AAICLICK_REPORT_FILE")
    if report_file:
        Path(report_file).parent.mkdir(parents=True, exist_ok=True)
        with open(report_file, "w") as f:
            f.write(rendered)
    else:
        sys.stdout.write(rendered)

    return {
        "corpus_size": stats.corpus_size,
        "embedding_dims": embedding_info.dims,
        "queries": len(QUERIES),
        "generation_status": generation.status if generation is not None else "skipped",
    }
