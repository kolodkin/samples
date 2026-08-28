Movie Plot RAG
---

RAG + embeddings pipeline using ClickHouse as the vector store: loads TMDB plot synopses (cross-referenced with IMDb vote counts) from a Hugging Face Parquet dump, curates the top ~1,000 best-known movies in SQL, embeds every plot locally with `sentence-transformers/all-MiniLM-L6-v2` (384-dim, no API key), stores the vectors as an `Array(Float32)` column, and answers natural-language "vibe" queries ("a toy comes alive when the humans leave the room") with exact brute-force `cosineDistance` top-k retrieval in SQL. Pass `--generate` to also ground LLM answers in the retrieved plots via aaiclick's own AI provider (`AAICLICK_AI_MODEL` / `AAICLICK_AI_API_KEY`, or a local Ollama server); retrieval itself runs fully offline.

```bash
# Auto-provision the databases locally, retrieval only
./movie-plot-rag.sh --local-setup

# Smaller corpus, more hits per query, plus grounded LLM answers
AAICLICK_AI_MODEL=anthropic/claude-opus-5 AAICLICK_AI_API_KEY=... \
  ./movie-plot-rag.sh --local-setup --movies 500 --top-k 5 --generate
```
