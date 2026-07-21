# GitHub Exposure Scanner — Technical Notes

Step 1 of a cyber-risk bot: profile a company's **GitHub attack-surface
exposure** by scanning its public repositories for leaked secrets. Reads only
public data, redacts every secret, and frames the output as a company cyber
profile that later steps (DNS, TLS, breach data, …) extend.

By default the scanner mirror-clones each repo and walks its **full git
history**, so a secret that was committed and later "removed" is still caught.
Every finding carries `commit_sha`, `commit_author`, `first_seen` (the
introducing commit's date), and `still_present_at_head`; the report groups
findings into live-at-HEAD vs historical-only. `--head-only` keeps the original
API-based current-HEAD scan for a fast pass. History details, including the
blob-dedup walk and safety caps, are in the sections below.

## Pipeline (aaiclick DAG — dynamic fan-out)

The `@job` entry task resolves the repo list at runtime, then **fans out one
`scan_one_repo` task per discovered repo**. All per-repo tasks append into a
shared findings `Object`; scoring/report/Airtable tasks fan back in via
`depends_on`, running only once every scan has completed.

```
entry (resolve repos inline)
   ├─► scan_one_repo(repo₁) ─┐
   ├─► scan_one_repo(repo₂) ─┤
   ├─► …                     ├─► score_exposure ─► generate_report
   └─► scan_one_repo(repoₙ) ─┤            │
                             └────────────┴─► publish_findings / publish_summary
```

- **entry** — resolves each target (`org` → top-N public repos by stars;
  `org/repo` → that repo) via `list_repos_impl` (a `list_error` is recorded per
  repo instead of aborting), then builds one child task per repo.
- **scan_one_repo** — one task per repository: fetches each scannable file at
  HEAD, runs the regex rules, and appends redacted findings (with a GitHub
  permalink) into the shared findings `Object`. File content is scanned in
  Python and discarded — never stored in ClickHouse or Airtable. Per-repo
  isolation means one repo's fetch error can't fail the others.
- **score_exposure** — depends on all `scan_one_repo` tasks; SQL group-by for
  repo/file/error aggregates plus the Python scoring formula and risk bands.
- **generate_report** — renders via `Object.markdown()`.

The inline (non-orchestrated) equivalent, `scan_repos_impl`, performs the same
scan in one pass and backs the offline unit tests; the dynamic DAG is covered
end-to-end by an in-process `ajob_test` run over the fixtures.

## Secret-detection rules

| Rule id | Secret type | Severity | Weight |
|---|---|---|---|
| aws-access-key | AWS Key | Critical | 10 |
| github-pat | GitHub PAT | Critical | 10 |
| stripe-live | Stripe Key | Critical | 10 |
| private-key | Private Key | Critical | 10 |
| slack-token | Slack Token | High | 5 |
| google-api-key | Google API Key | High | 5 |
| jwt | JWT | Medium | 2 |
| high-entropy-assignment | High-entropy | Low | 1 |

Values are redacted at detection time: `first4 + "••••" + last4` (fully hidden
when ≤ 8 chars).

## Context heuristics (test/demo vs real leak)

A committed private key is a true positive for the rule but often a *false
positive for risk* — e.g. a self-signed `localhost` dev cert. `classify_context`
assigns each finding a `context` label and a `confidence_real` in [0, 1] that
scales its risk weight. It applies only to the ambiguous classes (`Private
Key`, `High-entropy`, `JWT`); provider tokens (AWS/GitHub/Slack/Google/Stripe)
are live credentials and always stay at confidence 1.0.

| Signal | Example | confidence |
|---|---|---|
| Tier 2 — cert-gen script in the key's dir or an ancestor | `make-cert.sh` beside `certs/localhost.privkey.pem` | 0.05 |
| Tier 1 — test/example/dev path marker | `test/`, `fixtures/`, `localhost`, `/docs/`, … | 0.10 |
| none — treated as production | `deploy/prod/id_rsa` | 1.00 |

The summary reports a `likely_test` count of findings downgraded below full
confidence. Documented follow-ups (not yet implemented): Tier 3 — parse the
paired certificate and check CN/SAN for `localhost`/example domains or
self-signed issuers; Tier 4 — fingerprint against a blocklist of well-known
published sample keys.

## Scoring

Per org:

```
base  = Σ (severity_weight × confidence_real)   # per finding
score = round(base × (1 + log10(1 + flagged_stars)))
```

`flagged_stars` is the total stars of repos that had at least one finding — a
blast-radius proxy. Risk bands:

| Score | Band |
|---|---|
| 0 | Clean |
| 1–9 | Low |
| 10–29 | Medium |
| 30–79 | High |
| ≥ 80 | Critical |

## GitHub access

Live mode uses the GitHub REST API (`api.github.com`) for repo/tree metadata
and `raw.githubusercontent.com` for file content. `GITHUB_TOKEN`, when set,
raises the rate limit to 5000 req/hr (60 req/hr unauthenticated). A rate-limit
response raises `RateLimitError`; per-repo network errors are isolated.

Targets come from `--targets`/`params`; when neither is given, the
`GITHUB_REPOS` env var is the fallback (an explicit `--targets` overrides it).
It holds comma-separated entries, each `org|repo` (a single repo) or a bare
`org` (enumerate the org) — e.g. `GITHUB_REPOS="acme|widgets,octocat"`. The pipe
form is confined to this env var; `parse_repos_env` normalises it to the
internal `org/repo` targets before scanning. With nothing set anywhere the
default remains `octocat/Hello-World`.

**Fixture mode** (`GHX_FIXTURE_DIR`) reads canned files instead of the network
— used by the test suite and CI so runs are deterministic and offline:

```
<dir>/orgs/<org>/repos.json
<dir>/repos/<org>/<repo>/repo.json
<dir>/repos/<org>/<repo>/tree.json
<dir>/repos/<org>/<repo>/raw/<path>
```

## Airtable output (opt-in)

Gated on `--airtable` plus `AIRTABLE_API_KEY` / `AIRTABLE_BASE_ID`. Two tables
are published in replace mode: **GitHub Findings** (one row per redacted
finding) and **GitHub Exposure Summary** (one row per org). Table names can be
overridden via `AIRTABLE_FINDINGS_TABLE` / `AIRTABLE_SUMMARY_TABLE`.

## Responsible use

This tool performs external attack-surface assessment over **public** data. It
never displays raw secret values — only masked fingerprints and locations — so
its output is safe to commit and share. Use it to assess exposure you are
authorized to review (your own org, or a vendor as part of due diligence), and
follow responsible-disclosure practice for anything it surfaces.

History scanning surfaces secrets that were committed and later removed but
remain in public git history — treat every historical finding as compromised
and rotate it, regardless of whether it is still present at HEAD. The scanner
detects, redacts, attributes, and reports only; it **never validates a secret
against any live service**.
