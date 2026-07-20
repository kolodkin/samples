# GitHub Exposure Scanner — Git History Scanning

**Date:** 2026-07-20
**Status:** Design — awaiting review
**Component:** `github-exposure-scanner`

## Purpose

Today the scanner reads only the **current HEAD** of each public repo. The
highest-value gap for authorized attack-surface assessment and responsible
disclosure is the classic real-world leak: a secret that was committed, later
"removed" in a follow-up commit, but still lives forever in git history. This
design adds **full git-history scanning** as the default behavior, attributes
each finding to the commit that introduced it, and flags whether the secret is
still present at HEAD.

Scope of this spec: history scanning for the existing GitHub Exposure Scanner.
It does **not** cover new signal sources (DNS/TLS/breach data) — those remain
separate future steps in the cyber-risk roadmap.

## Guiding constraints (unchanged philosophy)

- **Public data only**, read-only. Targets come from engagement authorization,
  never a built-in target list.
- **Redaction unchanged** — only masked fingerprints (`first4••••last4`) and
  locations ever leave the detection module.
- **No live validation.** The scanner will not call third-party services to
  test whether a detected secret still works. Detect, redact, attribute,
  report — nothing more. This is an explicit, permanent non-goal.
- **Per-repo isolation** — one repo's failure (clone error, cap exceeded)
  records a row and never aborts the run.

## Decisions (from brainstorming)

| Decision | Choice |
|---|---|
| Direction | Git history scanning |
| History access | Local mirror clone + git object walk |
| Depth | Full history + configurable safety caps |
| Mode | History is the **default**; `--head-only` restores the fast path |
| Walk strategy | **Blob-based, deduped** (scan each unique blob once) |
| Language | **Python now** (combined regex + binary/size skips); native acceleration is a documented future option |

## Architecture

A new module `git_history.py` owns everything git-specific. The detection layer
(`rules.py` — `scan_text`, `classify_context`) and the aaiclick fan-out DAG are
reused unchanged. Per-repo flow inside the existing `scan_one_repo` task:

```
size pre-check ──► mirror clone ──► walk (dedup blobs) ──► scan ──► attribute ──► cleanup
   (API meta)      (temp dir)       (rev-list/cat-file)   (rules)  (commits)    (finally)
```

### Clone

- `git clone --mirror <url> <tempdir>` — fetches all refs/branches and the full
  object database. Anonymous for public repos; inject `GITHUB_TOKEN` into the
  clone URL when present (higher rate limit).
- The temp clone is always removed in a `finally`.
- The clone step is **injectable** (a callable/parameter) so tests can point the
  walker at a pre-existing local repo without any network. This also yields a
  "scan an already-cloned local path" capability for free.

### Walk — blob-based, deduped

- Enumerate objects with `git rev-list --all --objects`.
- Scan each **unique blob exactly once** via `git cat-file` (streamed).
  Deduping by blob is the main efficiency win over a diff-based `git log -p`
  walk, which re-scans the same secret on every branch/merge.
- **Skip early**: blobs over a size cap, and binary blobs (NUL-byte / decode
  check), before running any regex.

### Detection (reused, one change)

- `scan_text` and `classify_context` are reused as-is.
- **Performance:** rules are combined into a **single regex pass per line**
  (one alternation with named groups) instead of iterating N compiled patterns.
  `scan_text`'s public signature and output stay identical — this is an internal
  optimization behind the same interface.
- `classify_context` uses the path list of the finding's commit tree (or the
  union of paths in the mirror) to keep the existing test-vs-real heuristics
  working for historical blobs.

### Attribution

For each blob that produced findings, compute:

| Field | Meaning |
|---|---|
| `commit_sha`, `commit_date`, `commit_author` | The commit that introduced the blob (first-seen) |
| `first_seen`, `last_seen` | Date range the secret existed in history |
| `still_present_at_head` | Whether the blob is in the current HEAD tree — the triage flag |

`still_present_at_head` drives the disclosure framing: **live at HEAD** = "still
public, rotate now"; **history-only** = "was exposed on `<date>`, rotate
regardless." `permalink` points at the blob at its commit SHA so history-only
findings remain viewable.

## Safety caps & isolation

Configurable, enforced per repo:

- `--max-repo-mb` — checked against the API's reported `size` **before** cloning,
  so oversized repos are skipped without downloading.
- `--max-commits` / `--max-blobs` — bound the walk.
- clone/walk **timeout**.

Exceeding any cap records a `partial`/`skipped` row (same shape as today's
`list_error`) and the run continues.

## Data model changes

`FINDING_FIELDS` gains: `commit_sha`, `commit_author`, `commit_date`,
`first_seen`, `last_seen`, `still_present_at_head`. Types added to
`_FINDING_TYPES` (`still_present_at_head` → `UInt8`/bool; dates → `String`).
Existing columns are unchanged, so the Airtable schema extends additively.

## Scoring & report

- **Scoring formula unchanged** for this iteration. `still_present_at_head` is
  surfaced in the report rather than reweighting the score (revisit weighting in
  a later iteration once we have real-run data).
- **Report** grows two groupings — **Live at HEAD** and **Historical-only** —
  each with first-seen dates, rendered via `Object.markdown()`. This is the
  actionable framing for a disclosure package.

## CLI / shell

- History scan is the **default**. `--head-only` restores today's fast
  HEAD-only path (still used by the quick demo).
- New flags: `--head-only`, `--max-repo-mb`, `--max-commits`, `--max-blobs`,
  `--clone-timeout`.
- `github-exposure-scanner.sh` unchanged except for documenting the new flags.

## Testing (offline, deterministic)

- The injectable clone step lets tests build a **real local git repo** in a temp
  dir: `git init`, a commit that adds a fake (redactable) secret, then a later
  commit that "removes" it. The walker runs against that path — no network.
- This directly exercises the **add-then-delete** case that is the entire reason
  for history scanning, asserting `still_present_at_head == False` for the
  deleted secret and `True` for one that survives to HEAD.
- Existing HEAD-only fixture tests remain valid under `--head-only`.
- The dynamic-DAG e2e test (`ajob_test`) is extended to run the history path
  over a local-repo fixture.

## Performance approach & future acceleration

Python is sufficient: the hot spots are network (clone) and git's own C code
(`rev-list`/`cat-file`); only regex scanning is our CPU, and `re` is C-backed.
The design closes the gap algorithmically — dedup by blob, single combined
regex, early binary/size skip.

**Future option (not now):** if profiling at engagement scale shows the regex
loop dominates, swap `re` for a faster library (`google-re2`/`pyre2` or
`python-hyperscan`) behind the unchanged `scan_text` interface; a native
(PyO3/maturin) hot-path is a last resort only if that is still insufficient.
No build-toolchain changes are introduced by this spec.

## Out of scope

- New signal sources (DNS, TLS, breach data).
- Live secret validation (permanent non-goal, see constraints).
- Scoring re-weighting for live-vs-historical (future iteration).
- Rust/native modules (future option only).
