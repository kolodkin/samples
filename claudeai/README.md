Setup Scripts
---

Repo-wide scripts that provision an example project's dependencies **locally** (apt-based, no Docker) with the same connection contract our CI uses, so code and tests behave identically on a dev machine and in CI.

```bash
scripts/setup_aaiclick      # everything a run script needs (calls the others)
scripts/setup_clickhouse    # local ClickHouse server, CI-aligned
scripts/setup_postgres      # local PostgreSQL, aaiclick-ready
scripts/setup_ollama        # local Ollama server + the configured model
```

Each script is idempotent (safe to re-run) and uses `sudo` automatically when not run as root. Override defaults via environment variables (see each script's header).

## setup_aaiclick

The one entry point a run script calls, so provisioning is a single line:

```bash
scripts/setup_aaiclick            # whatever the AAICLICK_* environment asks for
scripts/setup_aaiclick --local    # override: embedded backend, whatever the URLs say
scripts/setup_aaiclick --ollama   # override: local Ollama, whatever the model says
```

It defines no contract of its own — the caller owns that. What to set up is read from the environment aaiclick itself reads (`backend.py`, `ai/ollama.py`), each variable independent:

| | asks for a server | otherwise |
|---|---|---|
| `AAICLICK_CH_URL` | `clickhouse://…` — provisioned | unset or `chdb://` — embedded |
| `AAICLICK_SQL_URL` | `postgresql://…` — provisioned + migrated | unset or `sqlite` — embedded |
| `AAICLICK_AI_MODEL` | `ollama/…` — server + weights | hosted — needs `AAICLICK_AI_API_KEY`; unset — no AI step |

So a run script exports what it needs and calls this with **no flags**: both URLs get ClickHouse + PostgreSQL + migrations, a bare shell gets chdb + SQLite, and only a project that exports a model pays for an AI provider.

Note the asymmetry that makes that work: an unset URL still means a backend (the embedded one), because aaiclick always needs somewhere to put state — but an unset model means *no AI step at all*, not aaiclick's default model, so `imdb-dataset-builder` and `github-exposure-scanner` never provision an LLM they don't use.

The two flags are escape hatches for overriding that environment, not the way to drive it. `--local` needs no branch of its own — it unsets both URLs, which is exactly how aaiclick spells "embedded". `--ollama` forces the local AI path even when `AAICLICK_AI_MODEL` names a hosted model.

Every step is **probe-first**: a server that already answers at its URL is left alone, so CI — where the databases are service containers — runs the same command as a laptop with nothing installed and pays only for the migrations. That is why the orchestration run scripts call it unconditionally instead of hiding it behind an opt-in flag.

## Connection contracts

| | ClickHouse | PostgreSQL | Ollama |
|---|---|---|---|
| host / ports | `localhost` 8123 (HTTP), 9000 (native) | `localhost` 5432 | `localhost` 11434 (`OLLAMA_HOST`) |
| user | `default` | `aaiclick` | — |
| password | `benchmark` (`CH_PASSWORD`) | `secret` (`POSTGRES_PASSWORD`) | — |
| database / model | — | `aaiclick` (`POSTGRES_DB`) | `llama3.1:8b` (`AAICLICK_AI_MODEL`) |

**ClickHouse** matches the GitHub Actions service container in `.github/workflows/clickhouse-low-card-snow-id.yml` (password `benchmark`). CI keeps using a Docker service; these scripts reproduce the same contract locally — they do not change CI.

**PostgreSQL** matches aaiclick's own `POSTGRES_*` defaults, so the orchestration backend works with no extra config. `setup_postgres` also creates the role + database and runs `aaiclick migrate`, so the orchestration tables exist. Point aaiclick at it with:

```bash
export AAICLICK_SQL_URL="postgresql+asyncpg://aaiclick:secret@localhost:5432/aaiclick"
```

**Ollama** backs aaiclick's default AI model, `ollama/llama3.1:8b`, which needs no API key. `aaiclick setup --ai` pulls the model but assumes a server is already running; `setup_ollama` installs one, starts it, then pulls and warms the model.

> aaiclick's local mode uses embedded chdb + SQLite and needs no server — that is what `setup_aaiclick` does when neither URL is exported, or under `--local`. Export both URLs when a run script starts a separate worker process, which chdb + SQLite cannot serve.

## Verify

```bash
clickhouse client --password benchmark -q 'SELECT version()'
psql "postgresql://aaiclick:secret@localhost:5432/aaiclick" -c '\dt'
ollama list
```
