# IMDb Dataset Builder — Manual Dispatch Workflow

Date: 2026-06-26

## Goal

Add a manually-triggered GitHub Actions workflow that runs the
`imdb-dataset-builder` pipeline on demand. The default path is a safe sample
run with no publishing; publishing to Hugging Face / Airtable is opt-in via
dispatch inputs.

## Motivation

The pipeline currently runs only via `imdb-dataset-builder.sh` on a developer
machine. There is no CI entry point. A `workflow_dispatch` workflow lets the
dataset be regenerated (and optionally published) from the GitHub UI without a
local environment.

## Design

### 1. Shell script: opt-in local DB provisioning

`imdb-dataset-builder.sh` currently always runs `scripts/setup_clickhouse` and
`scripts/setup_postgres` (apt-installs ClickHouse + PostgreSQL on the host).
That is correct for a dev machine but wrong for CI, where the databases are
provided as service containers.

Change: gate the setup block behind a new `--local-setup` flag (default
**off**).

- With `--local-setup`: run the two setup scripts as today.
- Without it: skip provisioning; assume ClickHouse + PostgreSQL already exist
  at `AAICLICK_CH_URL` / `AAICLICK_SQL_URL` (their existing defaults), and go
  straight to registering the job.

The usage comment is updated accordingly.

### 2. Workflow: `.github/workflows/imdb-dataset-builder.yml`

- **Trigger:** `workflow_dispatch` only.
- **Inputs** (defaults chosen for the safe path):
  - `sample` (boolean, default `true`) → adds `--sample` (500k-row limit).
  - `year_from` (string, default `1950`) → `--year-from <n>`.
  - `publish_hf` (boolean, default `false`) → `--publish`.
  - `publish_airtable` (boolean, default `false`) → `--airtable`.
- **Service containers** (connection contracts match the script's default
  URLs):
  - `clickhouse/clickhouse-server`: user `default`, password `benchmark`,
    ports 8123/9000, `CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT=1` to match the
    local setup's `access_management=1`.
  - `postgres`: user `aaiclick`, password `secret`, db `aaiclick`, port 5432.
- **Steps:**
  1. `actions/checkout`
  2. `astral-sh/setup-uv`
  3. `uv sync` (working directory `imdb-dataset-builder`)
  4. Run `aaiclick migrate upgrade head` against the Postgres service — the
     one piece of `setup_postgres` a bare container does not provide (the
     orchestration schema).
  5. Run `./imdb-dataset-builder.sh` with flags built from the inputs, **without**
     `--local-setup`.
  6. Publish the generated report markdown (`tmp/imdb_report.md`, via
     `AAICLICK_REPORT_FILE`) to `$GITHUB_STEP_SUMMARY`.
- **Secrets:** `HF_TOKEN` is passed to the run step only when `publish_hf` is
  true; `AIRTABLE_API_KEY` / `AIRTABLE_BASE_ID` only when `publish_airtable` is
  true.

### 3. README

Update `imdb-dataset-builder/README.md` so local users know provisioning is now
opt-in: pass `--local-setup` to auto-install ClickHouse + PostgreSQL locally
(or set the URLs to target an existing cluster).

## Out of scope

- No `push`/`schedule` triggers — manual dispatch only.
- No changes to the pipeline logic in `imdb_dataset_builder/`.
