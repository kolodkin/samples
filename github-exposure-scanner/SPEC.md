# GitHub Exposure Scanner — Technical Notes

Step 1 of a cyber-risk bot: profile a company's **GitHub attack-surface
exposure** by scanning its public repositories for leaked secrets. Reads only
public data, redacts every secret, and frames the output as a company cyber
profile that later steps (DNS, TLS, breach data, …) extend.

## Pipeline (aaiclick DAG)

```
list_repos ─► scan_repos ─┬─► score_exposure ─┐
                          │                    ├─► generate_report
validate_airtable_credentials ─┬─► publish_findings ─┘
                               └─► publish_summary ───┘
```

- **list_repos** — resolves each target (`org` → top-N public repos by stars;
  `org/repo` → that repo), records a `list_error` per repo instead of aborting.
- **scan_repos** — fetches each scannable file at HEAD, runs the regex rules,
  emits redacted findings with a GitHub permalink. File content is scanned in
  Python and discarded — never stored in ClickHouse or Airtable.
- **score_exposure** — SQL group-by for repo/file/error aggregates; Python
  scoring formula and risk bands.
- **generate_report** — renders via `Object.markdown()`.

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
