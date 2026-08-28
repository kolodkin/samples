# Movie Plot RAG — Technical Notes

## Pipeline

```
load_movie_pool ─► curate_corpus ─┬─► profile_corpus ──────────────┐
                                  └─► embed_plots ─┬─► measure_embeddings ─► generate_report
                                                   └─► search ─► generate_answers (opt) ─┘
```

| Stage | Where it runs | What it does |
|---|---|---|
| `load_movie_pool` | ClickHouse | Streams the ~190 MB Parquet via `url()`, keeps only rows with `numVotes >= 500` and a usable synopsis (a few MB land locally) |
| `curate_corpus` | ClickHouse | Dedupes by `tconst` (`group_by`/`any`), derives `year`, renames `overview` → `plot`, keeps top-N by vote count |
| `embed_plots` | Python (CPU) | The one step where data leaves ClickHouse: encodes `"title. plot"` with MiniLM, writes vectors back as `Array(Float32)` |
| `search` | ClickHouse | Per query: `ORDER BY 1 - cosineDistance(embedding, [q…]) DESC LIMIT k` — exact scan |
| `generate_answers` | LiteLLM | Optional: answers each query grounded only in its retrieved plots |

## Design choices

- **Corpus = famous movies.** The demo queries describe well-known films
  without sharing keywords with their plots; ranking the pool by IMDb vote
  count guarantees the targets are in the corpus, so retrieval quality is
  visible in the report rather than hypothetical.
- **`"title. plot"` embedding input.** The title anchors movies whose short
  synopsis undersells them; queries are embedded with the same model and
  L2-normalized, so cosine distance is well-behaved.
- **Brute force over ANN.** At ~1k × 384-dim, an exact `cosineDistance` scan
  is sub-millisecond; ClickHouse's experimental `vector_similarity` (HNSW)
  index is the production path at millions of rows, but adds experimental
  settings and version constraints a demo doesn't need.
- **Query vectors as SQL literals.** Each 384-float query embedding is
  rendered into the `Computed` expression — no parameter binding needed, and
  the whole retrieval stays a single readable SQL scan.
- **Generation is opt-in and degrades gracefully.** Retrieval works with no
  key and no network (after the first model download). `--generate` fails
  fast at registration when the `anthropic/*` default model has no
  `ANTHROPIC_API_KEY`; any LiteLLM model string works via
  `MOVIE_RAG_LLM_MODEL` (e.g. `ollama/llama3.1:8b`).
- **CPU-only torch.** `[tool.uv.sources]` pins torch to the PyTorch CPU
  index, keeping the install at a few hundred MB; embedding ~1k short texts
  takes seconds on CPU.
- **Thin shell runner.** `movie-plot-rag.sh` starts the workers and hands off
  to `aaiclick run-job <entrypoint> --set K=V --progress`, which registers the
  job, streams per-task progress, blocks until it is terminal, and exits
  non-zero on failure. That removes the job-id scraping, poll loop, and status
  branching a runner otherwise needs; flags map straight to `--set` pairs
  rather than being assembled into a JSON string. Requires aaiclick >= 0.0.23.
