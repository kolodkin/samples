"""IMDb Dataset Builder report generation."""

import os
import sys
from contextlib import redirect_stdout
from dataclasses import dataclass
from io import StringIO
from pathlib import Path

from aaiclick.data.models import ColumnInfo, Computed
from aaiclick.data.object import Object
from aaiclick.orchestration import task

from .constants import CLEAN_COLUMNS, HF_REPO_ID, IMDB_RAW_COLUMNS, IMDB_URL, TMDB_COLUMNS
from .models import AirtablePublishResult, EnrichmentStats, HFPublishResult, QualityIssues, RawProfile
from .tmdb import TMDB_URL


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

    profile: RawProfile
    quality_issues: QualityIssues
    hf_result: HFPublishResult | None
    airtable_result: AirtablePublishResult | None
    raw_md: str
    clean_md: str
    genre_md: str
    genre_distinct: int
    genre_total: int
    exports: dict[str, str] | None
    enrichment_stats: EnrichmentStats
    plots_md: str
    tmdb_total: int
    tmdb_sample_md: str


def _print_report(content: ReportContent) -> None:
    """Print the IMDb dataset builder report as markdown."""
    profile = content.profile
    quality_issues = content.quality_issues
    hf_result = content.hf_result

    print("\n## IMDb Movie Dataset Builder\n")

    print("### Raw Data Profile\n")
    print(f"URL: {IMDB_URL}")
    print(f"Total titles: {_fmt(profile.total_titles)}")
    print(f"Adult titles: {_fmt(profile.adult_count)} ({_fmt(profile.adult_pct)}%)\n")

    print("#### Field Schema\n")
    _print_field_table(IMDB_RAW_COLUMNS)

    print("\n#### Sample (first 5 rows)\n")
    print(content.raw_md)

    print("\n#### Title Type Breakdown\n")
    for title_type, count in sorted(profile.by_type.items(), key=lambda x: -x[1]):
        print(f"- {title_type}: {_fmt(count)}")

    dropped = profile.total_titles - quality_issues.total_movies
    print("\n### Movie Filter\n")
    print(f"- Non-adult movies with genres + year: {_fmt(quality_issues.total_movies)}")
    print(f"- Dropped (non-movie, adult, missing genres/year): {_fmt(dropped)}")

    print("\n### Quality Issues Detected\n")
    print(
        f"- Missing runtime (`\\N`): {_fmt(quality_issues.missing_runtime)} ({_fmt(quality_issues.missing_runtime_pct)}%)"
    )
    print(f"- Runtime < 40 min: {_fmt(quality_issues.short_runtime)}")
    print(f"- Runtime > 300 min: {_fmt(quality_issues.long_runtime)}")
    print(
        f"- Pre-{quality_issues.year_from} movies: "
        f"{_fmt(quality_issues.pre_year)} ({_fmt(quality_issues.pre_year_pct)}%)"
    )

    if content.genre_distinct > 50:
        print(f"\n### Genre Distribution (top 50 of {_fmt(content.genre_distinct)})\n")
    else:
        print("\n### Genre Distribution\n")

    if content.genre_md:
        print(content.genre_md)
        print(f"\n- Total genre-title rows: {_fmt(content.genre_total)}")

    print("\n### Clean Dataset\n")
    print("#### Field Schema\n")
    _print_field_table(CLEAN_COLUMNS)

    print("\n#### Sample (first 5 rows)\n")
    print(content.clean_md)

    stats = content.enrichment_stats
    print("\n### TMDB Raw Data Profile\n")
    print(f"URL: {TMDB_URL}")
    print("Source: HenryWaltson/TMDB-IMDB-Movies-Dataset (Hugging Face Parquet)")
    print(f"Rows loaded (pre-filtered to clean tconsts): {_fmt(content.tmdb_total)}")

    print("\n#### Field Schema\n")
    _print_field_table(TMDB_COLUMNS)

    print("\n#### Sample (first 5 rows)\n")
    print(content.tmdb_sample_md)

    print("\n### TMDB Enrichment\n")
    print("- Source: `HenryWaltson/TMDB-IMDB-Movies-Dataset` (Hugging Face Parquet)")
    print("- Join key: `tconst` (direct, no SPARQL resolution needed)")
    print(f"- Rows matched: {_fmt(stats.matched)} ({_fmt(stats.matched_pct)}% of {_fmt(stats.total_clean)})")
    print(f"- Usable plot text (>= 120 chars): {_fmt(stats.plots_usable)} ({_fmt(stats.plots_usable_pct)}%)")
    print(f"- Average plot length: {_fmt(stats.avg_plot_chars)} characters")

    print("\n#### Sample (first 3 rows)\n")
    print(content.plots_md)

    if content.exports:
        print("\n### Local Exports\n")
        for fmt, path in content.exports.items():
            print(f"- {fmt}: `{path}`")

    print("\n### Published\n")
    if hf_result is None:
        print("- Skipped: HF_TOKEN not set")
        print(f"- Set `HF_TOKEN` to publish to: https://huggingface.co/datasets/{HF_REPO_ID}")
    elif hf_result.status == "published":
        print(f"- Hugging Face: https://huggingface.co/datasets/{hf_result.repo}")
        print(f"- Rows published: {_fmt(hf_result.rows)}")
    else:
        print(f"- Status: {hf_result.status}")

    airtable = content.airtable_result
    print("\n### Airtable Showcase\n")
    if airtable is None or airtable.status == "skipped":
        reason = airtable.reason if airtable else "task did not run"
        print(f"- Skipped: {reason}")
        print("- Set `AIRTABLE_API_KEY` and `AIRTABLE_BASE_ID` to publish a sample to Airtable")
    elif airtable.status == "published":
        print(f"- Base: `{airtable.base}`  Table: `{airtable.table}`")
        print(f"- Rows published: {_fmt(airtable.rows)}")
    else:
        print(f"- Status: {airtable.status}")
        if airtable.reason:
            print(f"- Reason: {airtable.reason}")


@task
async def generate_report(
    raw: Object,
    movies: Object,
    clean: Object,
    genre_balance: Object,
    plots: Object,
    tmdb: Object,
    profile: RawProfile,
    quality_issues: QualityIssues,
    enrichment_stats: EnrichmentStats,
    hf_result: HFPublishResult | None = None,
    exports: dict[str, str] | None = None,
    airtable_result: AirtablePublishResult | None = None,
) -> dict:
    """Combine all pipeline outputs into a unified IMDb dataset builder report."""
    raw_md = (
        await raw[["tconst", "titleType", "primaryTitle", "startYear", "genres", "runtimeMinutes"]]
        .view(limit=5)
        .markdown(truncate={"primaryTitle": 40})
    )

    clean_md = await clean.view(limit=5).markdown(truncate={"primaryTitle": 40})

    tmdb_total = await tmdb["tconst"].count().data()
    tmdb_sample_md = (
        await tmdb[["tconst", "title", "overview"]].view(limit=5).markdown(truncate={"title": 40, "overview": 120})
    )

    genre_with_pct = genre_balance.rename({"genres": "Genre", "tconst": "Count"}).with_columns(
        {
            "%": Computed("Float64", "round(Count * 100.0 / sum(Count) OVER(), 2)"),
        }
    )
    genre_md = await genre_with_pct.view(order_by="Count DESC", limit=50).markdown()
    genre_data_raw = await genre_balance.data()
    genre_distinct = len(genre_data_raw["genres"])
    genre_total = sum(genre_data_raw["tconst"])

    plots_md = (
        await plots.where("length(plot) >= 120")[["tconst", "primaryTitle", "plot"]]
        .view(limit=3)
        .markdown(truncate={"primaryTitle": 30, "plot": 200})
    )

    buf = StringIO()
    with redirect_stdout(buf):
        _print_report(
            ReportContent(
                profile=profile,
                quality_issues=quality_issues,
                hf_result=hf_result,
                airtable_result=airtable_result,
                raw_md=raw_md,
                clean_md=clean_md,
                genre_md=genre_md,
                genre_distinct=genre_distinct,
                genre_total=genre_total,
                exports=exports,
                enrichment_stats=enrichment_stats,
                plots_md=plots_md,
                tmdb_total=tmdb_total,
                tmdb_sample_md=tmdb_sample_md,
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
        "total_titles": profile.total_titles,
        "total_movies": quality_issues.total_movies,
        "hf_status": hf_result.status if hf_result is not None else "skipped",
        "airtable_status": airtable_result.status if airtable_result is not None else "skipped",
        "enrichment_plots_usable": enrichment_stats.plots_usable,
    }
