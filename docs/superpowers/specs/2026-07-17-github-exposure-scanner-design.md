# GitHub Exposure Scanner — Design

**Project:** `github-exposure-scanner` (package `github_exposure_scanner`)
**Date:** 2026-07-17
**Status:** Approved design, pending implementation plan

## Purpose

Step 1 of a larger "cyber bot" that profiles a company's external cyber
risk. This step covers **GitHub attack-surface exposure**: given one or more
GitHub organizations, scan their public repositories for leaked secrets and
produce a per-org exposure profile.

It is a defensive / attack-surface-management use case — it reads only public
data, redacts every secret it finds, and never prints or stores raw secret
values. The report is structured as a company cyber profile with a "GitHub
Exposure" section so future steps (DNS, TLS, breach data, …) can be added as
new sections and scores without reshaping this one.

## Scope

**In scope:** org → repo enumeration, current-HEAD file scanning via the
GitHub API (no cloning), a built-in regex secret-detection rule library,
redacted findings, a weighted exposure score, a stdout report, and optional
publishing to two Airtable tables.

**Out of scope (future steps):** company-name → org discovery, full git
history scanning, secret verification (live-key testing), and non-GitHub risk
signals.

## Framework fit

Built on **aaiclick** (`@job` / `@task` DAG over `Object`s backed by
ClickHouse), following the repo conventions in `CLAUDE.md`. Closest existing
references: `imdb-dataset-builder` (external fetch → build `Object`s → SQL
analysis → report → optional Airtable) and aaiclick's own
`cyber_threat_feeds` example.

## Input

The job takes a list of **targets**, each either:

- a bare org login (e.g. `stripe`) — expands to the org's top-N public repos
  by star count, or
- an explicit `org/repo` (e.g. `stripe/stripe-python`) — scans only that repo.

Tuning parameters (passed via `.sh` flags → `--params`):

| Param | Default | Meaning |
|---|---|---|
| `targets` | a small demo set of well-known orgs | orgs and/or `org/repo` to scan |
| `max_repos` | 25 | max repos per bare org (top by stars) |
| `max_file_kb` | 512 | skip files larger than this |
| `publish_airtable` | `false` | opt in to Airtable publishing |

## External calls, auth & config

- Plain `httpx` (async) inside tasks for the GitHub REST/JSON API, then
  `create_object_from_value` to turn results into `Object`s. Adds `httpx` to
  `pyproject.toml`.
- Optional `GITHUB_TOKEN` env var → 5000 req/hr. Unauthenticated still works
  (60 req/hr) with a clear warning and conservative caps.
- **Fixture mode** via `GHX_FIXTURE_DIR` (mirrors imdb's `IMDB_URL` override):
  when set, all GitHub calls read canned JSON/content from that directory
  instead of the network, so tests and CI run offline and deterministically
  against a planted fake secret.

## Secret detection (built-in regex library)

A `rules.py` catalog. Each rule: `id`, `secret_type`, `severity`, compiled
`regex`. Planned rules:

| Rule | Secret type | Severity |
|---|---|---|
| AWS access key id (`AKIA…`) | AWS Key | Critical |
| GitHub PAT (`ghp_` / `gho_` / `ghu_` / `ghs_` / `ghr_`) | GitHub PAT | Critical |
| Slack token (`xox[baprs]-…`) | Slack Token | High |
| Google API key (`AIza…`) | Google API Key | High |
| Stripe live key (`sk_live_…`) | Stripe Key | Critical |
| Private key PEM header (`-----BEGIN … PRIVATE KEY-----`) | Private Key | Critical |
| JWT (`eyJ…\.eyJ…\.…`) | JWT | Medium |
| Generic high-entropy assignment (`secret/token/password = "…"`) | High-entropy | Low |

Detection is pure Python over fetched text. Redacted findings (never raw
values) are written into an `Object` via `create_object_from_value`, so all
counting, grouping, and scoring is done in SQL — the aaiclick showcase.
Redaction happens at detection time: `first4 + "••••" + last4`.

## DAG

```
list_repos(targets) ──► scan_repos ──► score_exposure ──► generate_report
        │                   │                ▲                  ▲
        └───────────────────┴────────────────┘                 │
                    (repos + findings feed scoring) ────────────┘
```

### Tasks

1. **`list_repos(targets, max_repos)` → repos `Object`.**
   For each target: bare org → list public repos, take top-N by stars;
   `org/repo` → fetch that repo's metadata. For each kept repo, fetch the git
   tree at HEAD and count scannable files. Repos that fail to list record a
   non-null `list_error` (the job does not abort).
   Columns: `org, repo, repo_url, default_branch, head_sha, stars, pushed_at,
   language, size_kb, files_to_scan, list_error`.

2. **`scan_repos(repos, max_file_kb)` → findings `Object`.**
   For each repo with no `list_error`: fetch raw content of each scannable
   file under the size cap, run every regex rule, and emit one redacted row
   per match. Content is scanned in Python and discarded — never stored.
   Columns: `org, repo, path, line, rule_id, secret_type, severity,
   masked_value, permalink, repo_stars, detected_at`.
   `permalink` deep-links to the exact line at `head_sha`.

3. **`score_exposure(repos, findings)` → summary `Object`.**
   Pure SQL join/aggregation over `repos` and `findings`, grouped by org.
   Exposure score = weighted sum of findings by severity (Critical/High/
   Medium/Low weights), scaled by a repo-popularity factor (stars as
   blast-radius proxy). Risk band derived from the score.
   Columns: `org, repos_scanned, files_scanned, total_findings,
   critical, high, medium, low, exposure_score, risk_band, top_secret_type,
   scan_errors`.

4. **Airtable branch (opt-in, gated).** Mirroring the imdb sample:
   `validate_airtable_credentials` → `publish_findings` + `publish_summary`.
   Skipped unless `publish_airtable=True` and credentials are present.
   - **`Findings` table** ← findings `Object`: Organization, Repository,
     File path, Line, Permalink (URL), Secret type (single select), Severity
     (single select), Masked value, Repo stars, Detected at (date), Status
     (single select, default "New").
   - **`Scan Summary` table** ← summary `Object`: Organization, Scan date,
     Repos scanned, Files scanned, Total findings, Critical, High, Medium,
     Low, Exposure score, Risk band (single select), Top secret type,
     Scan errors.

5. **`generate_report(...)` → terminal task.** Renders the stdout report
   using `Object.markdown()` for the repo inventory, findings (redacted), and
   per-org exposure summary, plus an Airtable publish status line. Always
   runs regardless of the Airtable branch.

## Error handling & safety

- Per-repo listing / fetch errors are caught and recorded (`list_error`,
  `scan_errors` count) rather than failing the whole job.
- Rate-limit responses produce a clear warning; unauthenticated runs use
  conservative caps.
- The report and both Airtable tables show only masked fingerprints. Raw
  secret values never leave the scanning task — not to stdout, not to
  ClickHouse, not to Airtable.
- README/SPEC include a responsible-use note framing this as defensive ASM
  over public data.

## Testing

`pytest`, run in fixture mode (offline, deterministic). The fixture set
includes a repo with a planted fake secret and a repo that triggers a
listing error. Tests assert:

- the planted secret is found and its value is masked (raw value absent from
  all outputs),
- scoring and risk-band computation are correct,
- a listing error is recorded as `scan_errors` without aborting the job,
- the report renders.

## Project layout

```
github-exposure-scanner/
├── github_exposure_scanner/
│   ├── __init__.py        # @job + task wiring
│   ├── __main__.py        # python -m entry point
│   ├── github_api.py      # httpx GitHub client + fixture-mode shim
│   ├── rules.py           # regex secret-detection catalog
│   ├── scan.py            # list_repos, scan_repos tasks
│   ├── score.py           # score_exposure task
│   ├── airtable.py        # validate + publish tasks
│   ├── models.py          # pydantic result models
│   ├── report.py          # generate_report task
│   └── pyproject.toml     # extra deps (httpx)
├── tests/
│   └── fixtures/          # canned GitHub responses + planted secret
├── github-exposure-scanner.sh
├── README.md
└── SPEC.md
```
