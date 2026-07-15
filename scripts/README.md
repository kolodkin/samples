Database Setup Scripts
---

Repo-wide scripts that provision databases **locally** (apt-based, no Docker) with the same connection contract our CI uses, so code and tests behave identically on a dev machine and in CI.

```bash
scripts/setup_clickhouse    # local ClickHouse server, CI-aligned
scripts/setup_postgres      # local PostgreSQL, aaiclick-ready
```

Each script is idempotent (safe to re-run) and uses `sudo` automatically when not run as root. Override defaults via environment variables (see each script's header).

## Connection contracts

| | ClickHouse | PostgreSQL |
|---|---|---|
| host / ports | `localhost` 8123 (HTTP), 9000 (native) | `localhost` 5432 |
| user | `default` | `aaiclick` |
| password | `benchmark` (`CH_PASSWORD`) | `secret` (`POSTGRES_PASSWORD`) |
| database | — | `aaiclick` (`POSTGRES_DB`) |

**ClickHouse** matches the GitHub Actions service container in `.github/workflows/clickhouse-low-card-snow-id.yml` (password `benchmark`). CI keeps using a Docker service; these scripts reproduce the same contract locally — they do not change CI.

**PostgreSQL** matches aaiclick's own `POSTGRES_*` defaults, so the orchestration backend works with no extra config. `setup_postgres` also creates the role + database and runs `aaiclick migrate`, so the orchestration tables exist. Point aaiclick at it with:

```bash
export AAICLICK_SQL_URL="postgresql+asyncpg://aaiclick:secret@localhost:5432/aaiclick"
```

> aaiclick's default local mode uses embedded chdb + SQLite and needs no server — run `python -m aaiclick setup` for that. Use `setup_postgres` only when you want a real PostgreSQL orchestration backend.

## Verify

```bash
clickhouse client --password benchmark -q 'SELECT version()'
psql "postgresql://aaiclick:secret@localhost:5432/aaiclick" -c '\dt'
```
