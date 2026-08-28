"""Constants for the movie plot RAG example."""

import os

from aaiclick.data.models import ColumnInfo

# Same HF Parquet dump imdb-dataset-builder uses for plot enrichment — it
# carries TMDB overviews plus IMDb vote counts, so the corpus can be curated
# to well-known movies entirely inside ClickHouse.
TMDB_URL = os.environ.get(
    "MOVIE_RAG_TMDB_URL",
    "https://huggingface.co/api/datasets/HenryWaltson/TMDB-IMDB-Movies-Dataset/parquet/default/train/0.parquet",
)

# Minimum IMDb vote count for the load-time pool filter. Not a pipeline
# parameter: it only bounds how much of the upstream dump is materialized —
# `corpus_size` is what actually selects the top-N.
MIN_VOTES = 500

# Local embedding model — free, offline after the first ~90 MB download.
EMBEDDING_MODEL = os.environ.get("MOVIE_RAG_EMBEDDING_MODEL", "sentence-transformers/all-MiniLM-L6-v2")

# Encoder batch size — an implementation detail of the embedding step.
EMBED_BATCH_SIZE = 64

# Natural-language "vibe" searches — none share keywords with the plots they
# should retrieve, which is exactly what embeddings solve over exact matching.
QUERIES = [
    "a toy comes alive when the humans leave the room",
    "a great white shark terrorizes a small beach town",
    "a hacker discovers the world he lives in is a simulation",
    "a gladiator seeks revenge against a corrupt roman emperor",
    "a clownfish father crosses the ocean to find his lost son",
    "dreams within dreams used to steal corporate secrets",
]

POOL_COLUMNS = {
    "tconst": ColumnInfo("String", description="IMDb title id (join key)"),
    "title": ColumnInfo("String", nullable=True, description="Movie title"),
    "release_date": ColumnInfo("String", nullable=True, description="TMDB release date (YYYY-MM-DD)"),
    "genres": ColumnInfo("String", nullable=True, description="Comma-separated genre list"),
    "overview": ColumnInfo("String", nullable=True, description="TMDB plot synopsis"),
    "numVotes": ColumnInfo("Int64", nullable=True, description="IMDb vote count (popularity proxy)"),
    "averageRating": ColumnInfo("Float64", nullable=True, description="IMDb average rating"),
}

CORPUS_COLUMNS = {
    "tconst": ColumnInfo("String", description="IMDb title id"),
    "title": ColumnInfo("String", description="Movie title"),
    "year": ColumnInfo("UInt16", description="Release year"),
    "genres": ColumnInfo("String", description="Comma-separated genre list"),
    "plot": ColumnInfo("String", description="TMDB plot synopsis"),
    "numVotes": ColumnInfo("Int64", description="IMDb vote count"),
}

EMBEDDED_COLUMNS = {
    **CORPUS_COLUMNS,
    "embedding": ColumnInfo("Float32", array=1, description="384-dim MiniLM sentence embedding"),
}
