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
- **Generation is mandatory, and configured the framework's way.** This is
  a RAG sample, so the G is not an add-on: `generate_answers` always runs and
  a provider failure fails the job, rather than quietly yielding a
  retrieval-only report. The step goes through
  `aaiclick.ai.config.get_ai_provider()`, so model and key come from
  `AAICLICK_AI_MODEL` / `AAICLICK_AI_API_KEY` — the pair every aaiclick
  project uses — rather than a config surface invented for this one.
  `ai_available()` gates registration, covering both shapes: a hosted model
  missing its key, or an Ollama model whose server is down or whose weights
  were never pulled (`ollama pull ...`). That check costs a
  second at registration instead of a minute of embedding followed by a dead
  end. Both `aaiclick.ai` imports are function-local, keeping litellm off the
  import path of the seven tasks that never touch it.
- **Local model by default.** `ollama/llama3.1:8b` needs no API key, so the
  full RAG loop runs offline once the weights are pulled; a hosted model is a
  two-env-var swap. Answer quality tracks the model — a small local model
  will sometimes justify a lower-ranked hit — but the grounding contract
  (answer only from the retrieved plots) is the same either way.
- **CPU-only torch.** `[tool.uv.sources]` pins torch to the PyTorch CPU
  index, keeping the install at a few hundred MB; embedding ~1k short texts
  takes seconds on CPU.
- **Thin shell runner.** `movie-plot-rag.sh` starts the workers and hands off
  to `aaiclick run-job <entrypoint> --set K=V --progress`, which registers the
  job, streams per-task progress, blocks until it is terminal, and exits
  non-zero on failure. That removes the job-id scraping, poll loop, and status
  branching a runner otherwise needs; flags map straight to `--set` pairs
  rather than being assembled into a JSON string. Requires aaiclick >= 0.0.23.
