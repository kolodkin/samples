---
name: claudeai-run
description: Start the distributed aaiclick backend (ClickHouse, PostgreSQL) that distributed example projects need, by running claudeai/run.sh. Use at the start of a new session, or whenever the services are down (e.g. after a container suspend/resume) and a distributed project needs them.
---

# claudeai-run Skill

Brings up ClickHouse (`:8123`) and PostgreSQL (`:5432`, with aaiclick migrations) for distributed example projects (e.g. `imdb-dataset-builder`, `github-exposure-scanner`). Local-mode projects use chdb + SQLite and need none of this.

## Preflight: web environment only

```bash
[ "${CLAUDE_CODE_REMOTE:-}" = "true" ] || { echo "Not the Claude Code web environment — skipping claudeai-run."; exit 1; }
```

`claudeai/_guard.sh` enforces the same check; override a deliberate local run with `CLAUDEAI_ALLOW_LOCAL=1`.

## Start

```bash
claudeai/run.sh
```

Wraps `claudeai/setup_aaiclick` with the distributed URLs defaulted. Idempotent: installs via apt on a fresh container, restarts stopped services after a resume, skips anything already answering. Extra flags pass through (e.g. `--ollama`).
