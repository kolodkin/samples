Movie Plot RAG
---

RAG + embeddings pipeline using ClickHouse as the vector store: loads TMDB plot synopses (cross-referenced with IMDb vote counts) from a Hugging Face Parquet dump, curates the top ~1,000 best-known movies in SQL, embeds every plot locally with `sentence-transformers/all-MiniLM-L6-v2` (384-dim, no API key), stores the vectors as an `Array(Float32)` column, and answers natural-language "vibe" queries ("a toy comes alive when the humans leave the room") with exact brute-force `cosineDistance` top-k retrieval in SQL. Every retrieval then feeds an LLM through aaiclick's own AI provider, which answers strictly from the plots that came back — retrieval, augmentation and generation, not retrieval with a bolt-on. The default `ollama/llama3.1:8b` needs no API key, just a local Ollama server; point `AAICLICK_AI_MODEL` / `AAICLICK_AI_API_KEY` at a hosted model instead if you prefer.

```bash
# Pull the model the generation step needs (any Ollama model works). Warm it
# once too — an 8B model loading cold can outlast Ollama's start timeout while
# the pipeline is also embedding.
ollama pull llama3.1:8b
ollama run llama3.1:8b 'hi' </dev/null

# Auto-provision ClickHouse + PostgreSQL, then run
./movie-plot-rag.sh --local-setup

# Smaller corpus, more retrieved plots per answer, against a hosted model
AAICLICK_AI_MODEL=anthropic/claude-opus-5 AAICLICK_AI_API_KEY=... \
  ./movie-plot-rag.sh --local-setup --movies 500 --top-k 5
```
