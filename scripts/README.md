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
scripts/setup_aaiclick            # whatever AAICLICK_SQL_URL / AAICLICK_CH_URL ask for
scripts/setup_aaiclick --local    # ignore those and set up the embedded backend
scripts/setup_aaiclick --ollama   # local Ollama server holding the configured model
scripts/setup_aaiclick --ai       # AI provider only: pull the model / check the API key
```

It defines no connection contract of its own — the caller owns that. Which backend to set up is read from the same two variables aaiclick itself reads (`backend.py`), and each is independent:

| | points at a server | unset, or embedded scheme |
|---|---|---|
| `AAICLICK_CH_URL` | `clickhouse://…` — provisioned | `chdb://` — embedded |
| `AAICLICK_SQL_URL` | `postgresql://…` — provisioned + migrated | `sqlite` — embedded |

So a run script that exports both gets ClickHouse + PostgreSQL + migrations, and a bare shell gets chdb + SQLite. `--local` needs no branch of its own: it just unsets both, which is exactly how aaiclick spells "embedded". Flags combine (`--ollama` implies `--ai`).

Every step is **probe-first**: a server that already answers at its URL is left alone, so CI — where the databases are service containers — runs the same command as a laptop with nothing installed and pays only for the migrations. That is why the orchestration run scripts call it unconditionally instead of hiding it behind an opt-in flag.

`--ai` prepares the provider `AAICLICK_AI_MODEL` names: for an `ollama/*` model it pulls and warms the weights against a running server (and fails with a pointer to `setup_ollama` if there is none); for a hosted model it checks `AAICLICK_AI_API_KEY` is set.

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
