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
| Language | **Python now** (blob dedup + binary/size skip); combined-regex / faster-lib / native are documented future options |

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

### Detection (reused, unchanged)

- `scan_text` and `classify_context` are reused as-is — no signature or
  behavior change.
- **Performance comes from the walk, not the regex:** scanning each unique blob
  **once** (dedup) and skipping binary/oversized blobs *before* any regex runs
  are the real wins and are correctness-safe. Combining the rule set into a
  single automaton is a marginal micro-optimization with real correctness risk
  (per-rule flags and value groups), so it is deferred to the future-acceleration
  list rather than done now.
- `classify_context` uses the union of introduced paths in the mirror to keep
  the existing test-vs-real heuristics working for historical blobs.

### Attribution

For each blob that produced findings, compute:

| Field | Meaning |
|---|---|
| `commit_sha`, `commit_author` | The commit that introduced the blob (first-seen) |
| `first_seen` | ISO date the blob first entered history (introducing commit's date) |
| `still_present_at_head` | Whether the blob is in the current HEAD tree — the triage flag |

The introducing commit and date come reliably from a single
`git log --all --reverse --raw` pass (oldest-first, so the first appearance of a
blob sha is its introduction). A precise "last seen" date would require tracking
each blob's *removal* commit — deferred as not worth the extra machinery for this
iteration; `first_seen` + `still_present_at_head` already carry the disclosure
signal.

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

`FINDING_FIELDS` gains: `commit_sha`, `commit_author`, `first_seen`,
`still_present_at_head`. Types added to `_FINDING_TYPES`
(`still_present_at_head` → `UInt8`; the rest `String`). Existing columns are
unchanged, so the Airtable schema extends additively. The HEAD-only path fills
these consistently: `commit_sha` = HEAD sha, `commit_author` = "",
`first_seen` = "", `still_present_at_head` = 1 (HEAD findings are by definition
present at HEAD).

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
The design closes the gap algorithmically — **dedup by blob** and **early
binary/size skip** — both correctness-safe.

**Future options (not now), in escalating order:** combine the rule set into a
single automaton; swap `re` for a faster library (`google-re2`/`pyre2` or
`python-hyperscan`) behind the unchanged `scan_text` interface; a native
(PyO3/maturin) hot-path only if that is still insufficient. No build-toolchain
changes are introduced by this spec.

## Out of scope

- New signal sources (DNS, TLS, breach data).
- Live secret validation (permanent non-goal, see constraints).
- Scoring re-weighting for live-vs-historical (future iteration).
- Rust/native modules (future option only).
