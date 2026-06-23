"""
IMDb Dataset Builder - Large-Scale Data Curation Example

Demonstrates aaiclick's data curation capabilities using real IMDb title data
loaded directly from the official IMDb datasets URL:

- URL Data Loading (create_object_from_url, TSVWithNames format)
- String Filtering (titleType, isAdult, missing value detection via \\N)
- Computed Columns (type casting with toUInt32OrNull, array splitting with splitByChar)
- Array Explode (one genre per row from comma-separated strings)
- Group By Aggregations (genre distribution analysis)
- Data Quality Profiling (countIf for missing runtime and out-of-range detection)
- TMDB Plot Enrichment (always on)
  - Static Hugging Face Parquet (HenryWaltson/TMDB-IMDB-Movies-Dataset)
    cross-references IMDb's tconst directly — no SPARQL needed
  - Inner join on tconst via Object.join (clean ⋈ tmdb)
  - Computed alias renames TMDB's `overview` → `plot` for the public schema
- Hugging Face Publishing (opt-in via publish_hf=True, requires HF_TOKEN env var)
- Airtable Showcase Publishing (opt-in via publish_airtable=True, also
  requires AIRTABLE_API_KEY + AIRTABLE_BASE_ID; ~200-row sample stratified
  by genre)

Data source: IMDb Non-Commercial Datasets (title.basics)
https://datasets.imdbws.com/title.basics.tsv.gz

License: IMDb Non-Commercial Use — https://developer.imdb.com/non-commercial-datasets/

Usage:
    # Register job (requires PostgreSQL or SQLite orchestration backend)
    python -m imdb_dataset_builder

    # Then run worker to execute
    python -m aaiclick worker start

Environment variables:
    HF_TOKEN             — Hugging Face token for dataset publishing (optional)
    AIRTABLE_API_KEY     — Airtable personal access token (optional, gates upload)
    AIRTABLE_BASE_ID     — Airtable base id, e.g. appXXXXXXXX (optional, gates upload)
    AIRTABLE_TABLE_NAME  — Airtable table name (default "IMDB")
    IMDB_URL             — Override IMDb data URL (useful for local testing)
    IMDB_TMDB_URL        — Override TMDB enrichment Parquet URL
"""

import asyncio
import os
from pathlib import Path

from huggingface_hub import HfApi

from aaiclick import ORIENT_DICT, cast, create_object_from_url
from aaiclick.data.models import ColumnInfo, Computed
from aaiclick.data.object import Object
from aaiclick.orchestration import job, task

from .airtable import publish_to_airtable, sample_for_airtable, validate_airtable_credentials
from .constants import CLEAN_COLUMNS, HF_REPO_ID, IMDB_COLUMNS, IMDB_RAW_COLUMNS, IMDB_URL
from .models import HFPublishResult, QualityIssues, RawProfile
from .report import generate_report
from .tmdb import enrich_with_tmdb, load_tmdb_dump, measure_enrichment

# =============================================================================
# Tasks
# =============================================================================


@task
async def load_raw_data(limit: int | None = None) -> Object:
    """
    Load IMDb title.basics dataset from official URL.

    Data flows directly into ClickHouse via the url() table function —
    no Python memory used for the bulk data. The gzip TSV is streamed
    and parsed natively by ClickHouse.

    All columns are loaded as String because ClickHouse TSVWithNames
    format treats \\N as a literal string, not NULL.

    Args:
        limit: Optional row limit for fast demos. Set to None for full ~10M rows.
    """
    # Force all columns as String — TSV \N values must remain as the
    # literal string r"\N", not be cast to NULL or int by type inference.
    all_string = {col: ColumnInfo("String") for col in IMDB_COLUMNS}
    return await create_object_from_url(
        url=IMDB_URL,
        columns=IMDB_COLUMNS,
        format="TSVWithNames",
        limit=limit,
        column_types=all_string,
    )


@task
async def profile_raw(raw: Object) -> RawProfile:
    """
    Profile the raw dataset: title type breakdown, adult content rate.

    Uses count_if for a single-scan count of total rows and adult titles.
    All aggregations run inside ClickHouse — Python only receives the
    small summary dict.
    """
    counts_obj = await raw.count_if(
        {
            "total": "1",
            "adult_count": "isAdult = '1'",
        }
    )
    counts = await counts_obj.data()
    total = counts["total"]
    adult_count = counts["adult_count"]

    type_obj = await raw.group_by("titleType").agg({"tconst": "count"})
    type_data = await type_obj.data(orient=ORIENT_DICT)
    type_counts = dict(zip(type_data["titleType"], type_data["tconst"], strict=False))

    return RawProfile(
        total_titles=total,
        by_type=type_counts,
        adult_count=adult_count,
        adult_pct=(adult_count / total * 100) if total > 0 else 0.0,
    )


@task
async def filter_movies(raw: Object) -> Object:
    """
    Filter to non-adult movies with known genres and start year, and convert
    ``genres`` from a comma-separated ``String`` into a native ``Array(String)``.

    All filter conditions push down as SQL WHERE clauses — ClickHouse
    executes them as a single filtered SELECT. The genres conversion uses
    a Computed column with ``splitByChar(',', genres)``; downstream tasks
    can then use first-class array operators (``has``, ``arrayJoin``, ...).
    """
    with_arr = (
        raw.where("titleType = 'movie'")
        .where("isAdult = '0'")
        .where(r"genres != '\N'")
        .where(r"startYear != '\N'")
        .with_columns({"genres_array": Computed("Array(String)", "splitByChar(',', genres)")})
    )
    # Project to drop the original String genres, materialize so the
    # new schema has only genres_array, then rename to the public name.
    projected = with_arr[
        [
            "tconst",
            "titleType",
            "primaryTitle",
            "originalTitle",
            "isAdult",
            "startYear",
            "endYear",
            "runtimeMinutes",
            "genres_array",
        ]
    ]
    materialized = await projected.copy()
    return await materialized.rename({"genres_array": "genres"}).copy()


@task
async def detect_quality_issues(movies: Object, year_from: int = 1980) -> QualityIssues:
    """
    Detect data quality issues in the movie subset.

    Uses a single count_if pass for missing runtime, then adds typed
    Computed columns to count runtime range and year violations.
    All counting is done inside ClickHouse.
    """
    total = await (await movies["tconst"].count()).data()

    # Single-scan count for missing runtime
    missing_obj = await movies.count_if(r"runtimeMinutes = '\N'")
    missing_runtime = await missing_obj.data()

    # Add typed columns to count range violations
    typed = movies.with_columns(
        {
            "year_int": cast("startYear", "UInt32"),
            "runtime_int": cast("runtimeMinutes", "UInt32"),
        }
    )

    range_counts = await typed.count_if(
        {
            "short_runtime": r"runtimeMinutes != '\N' AND toUInt32OrNull(runtimeMinutes) < 40",
            "long_runtime": r"runtimeMinutes != '\N' AND toUInt32OrNull(runtimeMinutes) > 300",
            "pre_year": f"toUInt32OrNull(startYear) < {year_from}",
        }
    )
    range_data = await range_counts.data()

    return QualityIssues(
        total_movies=total,
        missing_runtime=missing_runtime,
        missing_runtime_pct=(missing_runtime / total * 100) if total > 0 else 0.0,
        short_runtime=range_data["short_runtime"],
        long_runtime=range_data["long_runtime"],
        year_from=year_from,
        pre_year=range_data["pre_year"],
        pre_year_pct=(range_data["pre_year"] / total * 100) if total > 0 else 0.0,
    )


@task
async def normalize_genres(movies: Object) -> Object:
    """
    Explode the ``genres`` array into one row per genre.

    ``filter_movies`` already converts ``genres`` to ``Array(String)``,
    so this task is a single ``explode`` on the existing array column.
    """
    exploded = movies.explode("genres")
    return await exploded.copy()


@task
async def analyze_genre_balance(exploded: Object) -> Object:
    """
    Compute genre distribution across all movies.

    Groups by the exploded ``genres`` element, counts titles per genre.
    Returns an Object with ``(genres, tconst_count)`` rows for the report.
    """
    return await exploded.group_by("genres").agg({"tconst": "count"})


@task
async def build_clean_dataset(movies: Object, year_from: int = 1980) -> Object:
    """
    Build the final curated dataset ready for publishing.

    Applies quality filters: removes missing runtime, clips to 40–300 min,
    filters to movies released in ``year_from`` or later, excludes
    Adult-genre movies. Returns the clean
    ``(tconst, primaryTitle, startYear, genres, runtimeMinutes)`` subset.
    """
    typed = movies.with_columns(
        {
            "year_int": cast("startYear", "UInt32"),
            "runtime_int": cast("runtimeMinutes", "UInt32"),
        }
    )
    clean = typed.where(r"runtimeMinutes != '\N'")
    clean = clean.where("runtime_int >= 40")
    clean = clean.where("runtime_int <= 300")
    clean = clean.where(f"year_int >= {year_from}")
    clean = clean.where("has(genres, 'Adult') = 0")
    clean = clean[["tconst", "primaryTitle", "startYear", "genres", "runtimeMinutes"]]
    return await clean.copy(name="clean", scope="job")


@task
async def publish_to_huggingface(enriched: Object) -> HFPublishResult:
    """
    Publish the enriched (plot-carrying) curated dataset to Hugging Face Hub.

    The published dataset contains the clean IMDb subset plus the resolved
    Wikipedia article title and plot text (``wp_title``, ``plot``). Rows
    with no Wikipedia match are naturally absent because the enrichment
    pipeline inner-joins on ``wp_title``.

    Requires ``HF_TOKEN`` — without it, returns a skipped result without
    raising.
    """
    token = os.environ.get("HF_TOKEN")
    if not token:
        return HFPublishResult(status="skipped", reason="HF_TOKEN not set", repo=HF_REPO_ID)

    parquet_path = "/tmp/imdb_curated.parquet"
    await enriched.export(parquet_path)
    rows = await enriched["tconst"].count().data()

    api = HfApi()
    api.create_repo(repo_id=HF_REPO_ID, repo_type="dataset", exist_ok=True)
    api.upload_file(
        path_or_fileobj=parquet_path,
        path_in_repo="data/imdb_curated.parquet",
        repo_id=HF_REPO_ID,
        repo_type="dataset",
        token=token,
    )

    return HFPublishResult(status="published", rows=rows, repo=HF_REPO_ID)


@task
async def export_dataset(enriched: Object, formats: list[str], out_dir: str) -> dict[str, str]:
    """Export the enriched (plot-carrying) dataset in each requested format.

    Exports run concurrently — ClickHouse streams each file independently.
    """
    Path(out_dir).mkdir(parents=True, exist_ok=True)
    paths = {fmt: str(Path(out_dir) / f"imdb_curated.{fmt}") for fmt in formats}
    await asyncio.gather(*(enriched.export(p) for p in paths.values()))
    for fmt, path in paths.items():
        print(f"Exported {fmt}: {path}")
    return paths


# =============================================================================
# Job Definition
# =============================================================================


@job("imdb_dataset_builder")
def imdb_dataset_pipeline(
    limit: int | None = 500_000,
    year_from: int = 1980,
    publish_hf: bool = False,
    publish_airtable: bool = False,
):
    """
    IMDb Movie Dataset Builder Pipeline.

    Loads IMDb title.basics from the official dataset URL, profiles the raw
    data, filters to quality movies, detects data quality issues, normalizes
    genres, builds a clean curated dataset, optionally publishes to Hugging
    Face, and generates a markdown report.

    All heavy computation stays inside ClickHouse — Python only orchestrates
    the SQL operations and receives small summary dicts.

    DAG Structure::

        load_raw_data ─┬─► profile_raw
                       └─► filter_movies ─┬─► detect_quality_issues
                                          ├─► normalize_genres ─► analyze_genre_balance
                                          └─► build_clean_dataset ─┬─► load_tmdb_dump ─┐
                                                                   │                   ▼
                                                                   ├─► enrich_with_tmdb ─► measure_enrichment
                                                                   └─► publish_to_huggingface

        validate_airtable_credentials ─┬─► sample_for_airtable ─► publish_to_airtable
                                       └────────────────────────►

        All terminal tasks fan in to generate_report.

    Args:
        limit: Row limit for demo runs. Set to None for the full ~10M-row dataset.
        year_from: Earliest ``startYear`` to keep in the curated output. Defaults
            to 1980 — older entries have spottier metadata.
        publish_hf: Opt in to Hugging Face publishing. Defaults to ``False``.
            When ``True``, ``HF_TOKEN`` is *required* — registration fails fast
            with a clear error if it is unset (rather than silently skipping).
        publish_airtable: Opt in to the Airtable showcase branch. Defaults to
            ``False`` — even with ``AIRTABLE_API_KEY`` / ``AIRTABLE_BASE_ID`` set,
            the Airtable tasks are skipped unless this is explicitly ``True``.

    Environment variables:
        HF_TOKEN              — publish curated dataset to Hugging Face Hub
                                (required when ``publish_hf=True``)
        AIRTABLE_API_KEY      — publish a 200-row showcase to Airtable
                                (also requires ``publish_airtable=True``)
        AIRTABLE_BASE_ID      — Airtable base id (required when AIRTABLE_API_KEY is set)
        AIRTABLE_TABLE_NAME   — Airtable table name (default "IMDB")
        IMDB_TMDB_URL         — override TMDB enrichment Parquet URL
        IMDB_DATASET_EXPORTS  — comma-separated export formats (parquet,csv,...)
    """
    raw = load_raw_data(limit=limit)
    profile = profile_raw(raw=raw)
    movies = filter_movies(raw=raw)

    quality_issues = detect_quality_issues(movies=movies, year_from=year_from)
    exploded = normalize_genres(movies=movies)
    clean = build_clean_dataset(movies=movies, year_from=year_from)
    genre_balance = analyze_genre_balance(exploded=exploded)

    tmdb = load_tmdb_dump(clean=clean)
    plots = enrich_with_tmdb(clean=clean, tmdb=tmdb)
    enrichment_stats = measure_enrichment(clean=clean, plots=plots)

    if publish_hf:
        if not os.environ.get("HF_TOKEN"):
            raise ValueError("publish_hf=True (--publish) requires the HF_TOKEN environment variable to be set")
        hf_result = publish_to_huggingface(enriched=plots)
    else:
        hf_result = None

    if publish_airtable:
        airtable_validation = validate_airtable_credentials()
        airtable_sample = sample_for_airtable(plots=plots, genre_balance=genre_balance, validation=airtable_validation)
        airtable_result = publish_to_airtable(sample=airtable_sample, validation=airtable_validation)
    else:
        airtable_result = None

    export_formats = [f.strip().lower() for f in os.environ.get("IMDB_DATASET_EXPORTS", "").split(",") if f.strip()]
    exports = (
        export_dataset(enriched=plots, formats=export_formats, out_dir=os.environ.get("IMDB_OUT_DIR", "./tmp"))
        if export_formats
        else None
    )

    return generate_report(
        raw=raw,
        movies=movies,
        clean=clean,
        genre_balance=genre_balance,
        plots=plots,
        tmdb=tmdb,
        profile=profile,
        quality_issues=quality_issues,
        hf_result=hf_result,
        exports=exports,
        enrichment_stats=enrichment_stats,
        airtable_result=airtable_result,
    )


async def main(**kwargs):
    """Register the IMDb dataset builder pipeline job.

    ``**kwargs`` are forwarded to ``imdb_dataset_pipeline`` (e.g. ``limit``,
    ``year_from``) so the shell runner can pass tuning via ``--params``.

    The ``publish_hf`` / ``HF_TOKEN`` invariant is checked here too so the CLI
    registration path fails immediately (before a worker picks up the job),
    not just when the entry-point task runs.
    """
    if kwargs.get("publish_hf") and not os.environ.get("HF_TOKEN"):
        raise ValueError("publish_hf=True (--publish) requires the HF_TOKEN environment variable to be set")
    created_job = await imdb_dataset_pipeline(**kwargs)
    print(f"Registered job: {created_job.name} (ID: {created_job.id})")
    return created_job


if __name__ == "__main__":
    asyncio.run(main())
