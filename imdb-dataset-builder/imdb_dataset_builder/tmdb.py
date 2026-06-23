"""TMDB plot enrichment for the IMDb dataset builder.

A static Hugging Face Parquet dump that already cross-references IMDb's
``tconst``, eliminating the SPARQL → Wikipedia-dump → regex-extract chain
the project used originally. Two tasks:

1. ``load_tmdb_dump`` — fetch the single Parquet shard via ClickHouse
   ``url()``, pre-filtered at insert time to ``tconst`` IDs already in
   ``clean`` (so we materialize only ~10s of MB locally regardless of
   how big the upstream file gets).

2. ``enrich_with_tmdb`` — inner-join ``clean`` and ``tmdb`` on ``tconst``
   via ``Object.join``. Single 2-way hash join is the natural fit since
   TMDB cross-references IMDb's tconst directly. Renames ``overview`` →
   ``plot`` via a Computed alias so downstream consumers keep the same
   column name they used with the Wikipedia plot text.

Coverage: ~90% of clean IMDb rows pick up a non-null overview from TMDB,
typical length 200-400 chars (clean editorial single-paragraph synopses,
no Wikipedia-style templates or refs).
"""

import os

from aaiclick import create_object_from_url
from aaiclick.data.models import GB_ANY, ColumnInfo, Computed
from aaiclick.data.object import Object
from aaiclick.orchestration import task

from .models import EnrichmentStats

TMDB_URL = os.environ.get(
    "IMDB_TMDB_URL",
    "https://huggingface.co/api/datasets/HenryWaltson/TMDB-IMDB-Movies-Dataset/parquet/default/train/0.parquet",
)


@task
async def load_tmdb_dump(clean: Object) -> Object:
    """Fetch the TMDB-IMDB Parquet, pre-filtered to titles in ``clean``.

    ClickHouse's ``url()`` table function streams the upstream file but
    only writes rows matching ``tconst IN clean`` to the local table —
    on a ~190 MB upstream file we end up with a few MB locally for the
    demo dataset. Hugging Face 302-redirects ``/api/.../parquet/...``
    URLs to a CDN host, so ``max_http_get_redirects`` is raised.
    """
    return await create_object_from_url(
        url=TMDB_URL,
        columns=["tconst", "title", "overview"],
        format="Parquet",
        column_types={
            "tconst": ColumnInfo("String"),
            "title": ColumnInfo("String", nullable=True),
            "overview": ColumnInfo("String", nullable=True),
        },
        ch_settings={"max_http_get_redirects": 10},
        where=f"tconst IN (SELECT tconst FROM {clean.table})",
    )


@task
async def enrich_with_tmdb(clean: Object, tmdb: Object) -> Object:
    """Inner-join clean IMDb subset with TMDB overviews on ``tconst``.

    Simple 2-way hash join — TMDB cross-references IMDb's tconst directly
    so no AggregatingMergeTree-based merge is needed. The inner join
    naturally drops IMDb titles that TMDB doesn't have an overview for.

    TMDB has ~1.7% duplicate tconst rows (multiple TMDB entries for the
    same IMDb id); ``group_by(tconst).agg(any)`` collapses those before
    the join so we don't multiply IMDb rows.

    Returns ``(tconst, primaryTitle, startYear, genres, runtimeMinutes, plot)``
    where ``plot`` is renamed from TMDB's ``overview`` via a Computed alias.
    """
    deduped_tmdb = await tmdb.group_by("tconst").agg({"overview": GB_ANY})
    joined = await clean.join(deduped_tmdb, on="tconst", how="inner")

    aliased = joined.with_columns({"plot": Computed("String", "overview")})
    final = aliased[
        [
            "tconst",
            "primaryTitle",
            "startYear",
            "genres",
            "runtimeMinutes",
            "plot",
        ]
    ]
    return await final.copy(name="enriched", scope="job")


@task
async def measure_enrichment(clean: Object, plots: Object) -> EnrichmentStats:
    """Compute coverage stats for the TMDB enrichment."""
    total_clean = await (await clean["tconst"].count()).data()
    matched = await (await plots["tconst"].count()).data()

    plot_stats = await (await plots.count_if({"usable": "length(plot) >= 120"})).data()
    plots_usable = plot_stats["usable"]

    avg_obj = plots.with_columns({"plot_len": Computed("UInt32", "length(plot)")})
    avg = await (await avg_obj["plot_len"].mean()).data()

    def pct(n: int) -> float:
        return (n / total_clean * 100) if total_clean > 0 else 0.0

    return EnrichmentStats(
        total_clean=total_clean,
        matched=matched,
        matched_pct=pct(matched),
        plots_usable=plots_usable,
        plots_usable_pct=pct(plots_usable),
        avg_plot_chars=float(avg or 0.0),
    )
