"""Shared constants for the IMDb Dataset Builder pipeline."""

import os

from aaiclick.data.models import ColumnInfo

IMDB_URL = os.environ.get("IMDB_URL", "https://datasets.imdbws.com/title.basics.tsv.gz")

IMDB_COLUMNS = [
    "tconst",
    "titleType",
    "primaryTitle",
    "originalTitle",
    "isAdult",
    "startYear",
    "endYear",
    "runtimeMinutes",
    "genres",
]

IMDB_RAW_COLUMNS: dict[str, ColumnInfo] = {
    "tconst": ColumnInfo("String", description="IMDb title identifier (e.g. tt0000001)"),
    "titleType": ColumnInfo("String", description="Type of title (movie, short, tvSeries, ...)"),
    "primaryTitle": ColumnInfo("String", description="Popular title used for promotion"),
    "originalTitle": ColumnInfo("String", description="Original language title"),
    "isAdult": ColumnInfo("String", description="'1' for adult content, '0' otherwise"),
    "startYear": ColumnInfo("String", description="Release year (or '\\N' if unknown)"),
    "endYear": ColumnInfo("String", description="End year for series ('\\N' for movies)"),
    "runtimeMinutes": ColumnInfo("String", description="Runtime in minutes ('\\N' if unknown)"),
    "genres": ColumnInfo("String", description="Comma-separated genres ('\\N' if unknown)"),
}

CLEAN_COLUMNS: dict[str, ColumnInfo] = {
    "tconst": ColumnInfo("String", description="IMDb title identifier (e.g. tt0000001)"),
    "primaryTitle": ColumnInfo("String", description="Popular title used for promotion"),
    "startYear": ColumnInfo("String", description="Release year (>= 1950)"),
    "genres": ColumnInfo("Array(String)", description="Genre tags (no Adult)"),
    "runtimeMinutes": ColumnInfo("String", description="Runtime in minutes (40-300)"),
}

HF_REPO_ID = os.environ.get("HF_REPO_ID", "aaiclick/imdb-wikipedia-enriched")

TMDB_COLUMNS: dict[str, ColumnInfo] = {
    "tconst": ColumnInfo("String", description="IMDb title identifier (direct join key)"),
    "title": ColumnInfo("String", description="TMDB display title"),
    "overview": ColumnInfo("String", description="Editorial plot summary (TMDB-curated)"),
}
