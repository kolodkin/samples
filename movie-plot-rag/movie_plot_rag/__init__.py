"""
Movie Plot RAG - Embeddings + Vector Search Example

Demonstrates a full RAG (retrieval-augmented generation) pipeline on top of
aaiclick, with ClickHouse as the vector store:

- URL Data Loading (create_object_from_url, Parquet, pre-filtered at insert)
- SQL-side Corpus Curation (dedupe via group_by/any, order by vote count)
- Local Embeddings (sentence-transformers all-MiniLM-L6-v2, 384-dim, no API key)
- Vector Storage (embeddings as Array(Float32) columns in ClickHouse)
- Semantic Search (brute-force cosineDistance ORDER BY ... LIMIT k — exact
  top-k, no index needed at this scale)
- Generation (top-k plots fed to an LLM via aaiclick.ai's provider, which
  answers strictly from the retrieved context — the R, A and G of RAG)

Data source: HenryWaltson/TMDB-IMDB-Movies-Dataset (Hugging Face Parquet) —
TMDB plot synopses cross-referenced with IMDb vote counts.

Usage:
    # With a worker running, run the pipeline and block on its progress
    # (requires PostgreSQL or SQLite orchestration backend):
    python -m aaiclick execution-worker start &
    python -m aaiclick run-job movie_plot_rag.movie_plot_rag_pipeline \
        --set corpus_size=300 --progress

    # Or execute the whole DAG in-process against embedded chdb + SQLite, no
    # worker — handy for debugging. `setup` creates that local backend; add
    # --force if an older aaiclick left a local.db whose schema predates this
    # version (it refuses to reuse one, and --force is a no-op when current):
    python -m aaiclick setup [--force] --ai
    python -m movie_plot_rag --run --params '{"corpus_size": 300}'

    Either way an AI provider must be reachable — `setup --ai` pulls the
    configured Ollama model when a local server is running.

Environment variables (the AI pair is aaiclick's own, shared by every
project that uses ``aaiclick.ai``):
    AAICLICK_AI_MODEL         — LiteLLM model string for the generation step
                                (default ollama/llama3.1:8b — no key, but needs
                                a local Ollama server)
    AAICLICK_AI_API_KEY       — API key, required for hosted models
    MOVIE_RAG_EMBEDDING_MODEL — sentence-transformers model name
    MOVIE_RAG_TMDB_URL        — override the corpus Parquet URL
"""

import time

from aaiclick import ORIENT_DICT, create_object_from_url, create_object_from_value
from aaiclick.ai.ollama import get_configured_model
from aaiclick.data.models import Computed, FieldSpec, GB_ANY
from aaiclick.data.object import Object
from aaiclick.orchestration import job, task

from .constants import (
    EMBED_BATCH_SIZE,
    EMBEDDING_MODEL,
    MIN_VOTES,
    POOL_COLUMNS,
    QUERIES,
    TMDB_URL,
)
from .models import CorpusStats, EmbeddingInfo, GenerationResult, RagAnswer
from .report import generate_report

_AI_UNAVAILABLE_MSG = (
    "This pipeline generates grounded answers, so it needs an AI provider "
    "aaiclick can reach. Either start a local Ollama server and run "
    "`python -m aaiclick setup --ai` to pull the default ollama/llama3.1:8b "
    "(no API key needed), or set AAICLICK_AI_API_KEY with AAICLICK_AI_MODEL "
    "naming a hosted model (e.g. anthropic/claude-opus-5)."
)


def _require_ai_provider() -> None:
    """Fail fast when no AI provider is reachable.

    ``ai_available()`` covers both shapes aaiclick supports — a hosted model
    needs ``AAICLICK_AI_API_KEY``; an Ollama model needs the local server up
    with the model pulled — so this no longer hard-codes one vendor's key.

    Checked from both ``main()`` (CLI registration, fast feedback) and the
    ``@job`` body (authoritative — workers and catalog re-runs bypass main),
    so a missing provider costs a second at registration rather than a
    minute of embedding followed by a failed generation step.

    Imported lazily: ``aaiclick.ai.config`` pulls in litellm, which every
    other task in this pipeline can do without.
    """
    from aaiclick.ai.config import ai_available

    if not ai_available():
        raise ValueError(_AI_UNAVAILABLE_MSG)


def _sql_quote(text: str) -> str:
    """Escape a Python string as a ClickHouse single-quoted literal."""
    return "'" + text.replace("\\", "\\\\").replace("'", "\\'") + "'"


def _vector_literal(vec) -> str:
    """Render an embedding as a ClickHouse Array(Float32) literal."""
    return "[" + ",".join(f"{float(v):.8f}" for v in vec) + "]"


def _load_encoder():
    """Load the sentence-transformers model (cached on disk after first run)."""
    from sentence_transformers import SentenceTransformer

    return SentenceTransformer(EMBEDDING_MODEL)


# =============================================================================
# Tasks
# =============================================================================


@task
async def load_movie_pool() -> Object:
    """Load the TMDB/IMDb Parquet dump, pre-filtered at insert time.

    ClickHouse's ``url()`` table function streams the upstream file but only
    writes rows with a usable synopsis and at least ``MIN_VOTES`` IMDb votes —
    the ~190 MB upstream file lands as a few MB locally. Hugging Face
    302-redirects to a CDN host, so ``max_http_get_redirects`` is raised.
    """
    return await create_object_from_url(
        url=TMDB_URL,
        columns=list(POOL_COLUMNS),
        format="Parquet",
        column_types=POOL_COLUMNS,
        ch_settings={"max_http_get_redirects": 10},
        where=f"numVotes >= {MIN_VOTES} AND length(overview) >= 100 AND length(title) > 0",
    )


@task
async def curate_corpus(pool: Object, corpus_size: int = 1000) -> Object:
    """Curate the top-``corpus_size`` best-known movies, all in SQL.

    The dump has a few duplicate tconst rows (multiple TMDB entries per IMDb
    id); ``group_by(tconst).agg(any)`` collapses them. The corpus is then the
    top rows by IMDb vote count — famous movies, so the demo queries have
    real targets to hit. ``release_date`` is reduced to a year and
    ``overview`` renamed to ``plot`` for the public schema.

    Nothing leaves ClickHouse here: the whole shape-dedupe-rank chain is one
    query materialized by ``copy()``.
    """
    deduped = await pool.group_by("tconst").agg(
        {col: GB_ANY for col in POOL_COLUMNS if col != "tconst"}
    )
    shaped = deduped.with_columns(
        {"year": Computed("UInt16", "toUInt16OrZero(substring(release_date, 1, 4))")}
    )
    final = shaped[["tconst", "title", "year", "genres", "overview", "numVotes"]].rename(
        {"overview": "plot"}
    )
    top = final.view(order_by="numVotes DESC", limit=corpus_size)
    return await top.copy(name="corpus", scope="job")


@task
async def profile_corpus(pool: Object, corpus: Object) -> CorpusStats:
    """Small summary stats — all aggregation runs inside ClickHouse."""
    pool_size = await (await pool["tconst"].count()).data()
    corpus_size = await (await corpus["tconst"].count()).data()
    with_len = corpus.with_columns({"plot_len": Computed("UInt32", "length(plot)")})
    avg_chars = await (await with_len["plot_len"].mean()).data()
    return CorpusStats(
        pool_size=pool_size,
        corpus_size=corpus_size,
        avg_plot_chars=float(avg_chars or 0.0),
    )


@task
async def embed_plots(corpus: Object) -> Object:
    """Embed every plot locally and store vectors next to the metadata.

    This is the one step where data leaves ClickHouse: the corpus is small
    (~1k rows), so it is pulled into Python, encoded in batches on CPU, and
    written back as an ``Array(Float32)`` column. Embedding input is
    "title. plot" — the title anchors famous movies the plot alone
    undersells. Vectors are L2-normalized so cosine distance is well-behaved.
    """
    rows = await corpus.data(orient=ORIENT_DICT)
    texts = [f"{title}. {plot}" for title, plot in zip(rows["title"], rows["plot"], strict=True)]

    encoder = _load_encoder()
    started = time.perf_counter()
    vectors = encoder.encode(texts, batch_size=EMBED_BATCH_SIZE, normalize_embeddings=True)
    print(f"Embedded {len(texts)} plots in {time.perf_counter() - started:.1f}s ({EMBEDDING_MODEL})")

    records = [
        {
            "tconst": rows["tconst"][i],
            "title": rows["title"][i],
            "year": rows["year"][i],
            "genres": rows["genres"][i],
            "plot": rows["plot"][i],
            "numVotes": rows["numVotes"][i],
            "embedding": [float(v) for v in vectors[i]],
        }
        for i in range(len(texts))
    ]
    return await create_object_from_value(
        records,
        name="embedded_corpus",
        scope="job",
        fields={"embedding": FieldSpec(type="Float32")},
    )


@task
async def measure_embeddings(embedded: Object) -> EmbeddingInfo:
    """Read back vector shape from ClickHouse as a sanity check."""
    rows = await (await embedded["tconst"].count()).data()
    dims_obj = embedded.with_columns({"dims": Computed("UInt32", "length(embedding)")})
    dims = await (await dims_obj["dims"].max()).data()
    return EmbeddingInfo(model=EMBEDDING_MODEL, dims=int(dims or 0), rows=rows)


@task
async def search(embedded: Object, top_k: int = 3) -> Object:
    """Answer each demo query with exact brute-force vector search in SQL.

    Each query is embedded with the same model, then ClickHouse scores every
    corpus row with ``1 - cosineDistance(embedding, <query vector>)`` and
    keeps the top-k — a single ORDER BY ... LIMIT scan, no index required at
    this scale. Results from all queries land in one ranked results Object.
    """
    encoder = _load_encoder()
    query_vectors = encoder.encode(QUERIES, normalize_embeddings=True)

    results = []
    for query, vec in zip(QUERIES, query_vectors, strict=True):
        scored = embedded.with_columns(
            {"similarity": Computed("Float64", f"round(1 - cosineDistance(embedding, {_vector_literal(vec)}), 3)")}
        )
        top = scored[["title", "year", "genres", "similarity", "plot"]].view(
            order_by="similarity DESC", limit=top_k
        )
        rows = await top.data(orient=ORIENT_DICT)
        for rank in range(len(rows["title"])):
            results.append(
                {
                    "query": query,
                    "rank": rank + 1,
                    "title": rows["title"][rank],
                    "year": rows["year"][rank],
                    "genres": rows["genres"][rank],
                    "similarity": rows["similarity"][rank],
                    "plot": rows["plot"][rank],
                }
            )
    return await create_object_from_value(results, name="search_results", scope="job")


@task
async def generate_answers(results: Object) -> GenerationResult:
    """The "G" in RAG: ground an LLM answer in the retrieved plots.

    For each query, the top-k retrieved movies are passed as context to
    aaiclick.ai's LiteLLM provider, which answers strictly from that context.
    Always runs — grounded answers are the point of the pipeline, so a
    provider failure fails the job rather than yielding a retrieval-only
    report.

    ``get_ai_provider()`` reads ``AAICLICK_AI_MODEL`` / ``AAICLICK_AI_API_KEY``,
    so this project configures its model exactly like every other aaiclick
    one. Imported lazily to keep litellm off the other tasks' import path.
    """
    from aaiclick.ai.config import get_ai_provider

    provider = get_ai_provider()
    rows = await results.data(orient=ORIENT_DICT)

    by_query: dict[str, list[str]] = {}
    for i, query in enumerate(rows["query"]):
        by_query.setdefault(query, []).append(
            f"- {rows['title'][i]} ({rows['year'][i]}, similarity {rows['similarity'][i]}): {rows['plot'][i]}"
        )

    answers = []
    for query, hits in by_query.items():
        answer = await provider.query(
            prompt=f'Which of these movies best matches the search "{query}", and why? Answer in 2-3 sentences.',
            context="Retrieved movies (ranked by embedding similarity):\n" + "\n".join(hits),
            system="You are a movie search assistant. Ground your answer only in the provided retrieved movies.",
        )
        answers.append(RagAnswer(query=query, answer=answer.strip()))

    return GenerationResult(model=get_configured_model(), answers=answers)


# =============================================================================
# Job Definition
# =============================================================================


@job("movie_plot_rag")
def movie_plot_rag_pipeline(
    corpus_size: int = 1000,
    top_k: int = 3,
):
    """
    Movie Plot RAG Pipeline.

    Loads TMDB plot synopses, curates a corpus of well-known movies inside
    ClickHouse, embeds the plots locally, retrieves per query with cosine
    similarity in SQL, and grounds an LLM answer in what came back.

    Requires a reachable AI provider: the generation step is part of the
    pipeline, not an add-on, so registration fails fast without one.

    DAG Structure::

        load_movie_pool ─┬─► curate_corpus ─┬─► profile_corpus
                         │                  └─► embed_plots ─┬─► measure_embeddings
                         │                                   └─► search ─► generate_answers
                         └──────────────────────────────────────────────────┐
                                                                            ▼
        All terminal tasks fan in to generate_report.

    Args:
        corpus_size: Movies kept in the embedded corpus (top by vote count).
        top_k: Retrieved movies per query — the ``k`` of RAG. Nearly free
            on the retrieval side (the cost is embedding the query, not the
            ``LIMIT``); what it really sizes is the context handed to the LLM.
    """
    pool = load_movie_pool()
    corpus = curate_corpus(pool=pool, corpus_size=corpus_size)
    stats = profile_corpus(pool=pool, corpus=corpus)
    embedded = embed_plots(corpus=corpus)
    embedding_info = measure_embeddings(embedded=embedded)
    results = search(embedded=embedded, top_k=top_k)

    _require_ai_provider()
    generation = generate_answers(results=results)

    return generate_report(
        corpus=corpus,
        embedded=embedded,
        results=results,
        stats=stats,
        embedding_info=embedding_info,
        top_k=top_k,
        generation=generation,
    )


async def main(**kwargs):
    """Register the movie plot RAG pipeline job.

    ``**kwargs`` are forwarded to ``movie_plot_rag_pipeline`` (e.g.
    ``corpus_size``, ``generate``) so the shell runner can pass tuning via
    ``--params``.
    """
    _require_ai_provider()
    created_job = await movie_plot_rag_pipeline(**kwargs)
    print(f"Registered job: {created_job.name} (ID: {created_job.id})")
    return created_job
