# nbstripout Enforcement — Design

**Date:** 2026-06-12

## Problem

Jupyter notebooks store cell outputs, execution counts, and volatile metadata
inside the `.ipynb` JSON. Committing these produces noisy diffs, bloats the
repo, and can leak data embedded in outputs. This repo currently has one
notebook (`imdb-genre-distilbert/notebook.ipynb`) — already output-free — but
nothing prevents a future commit from introducing outputs, here or in any new
notebook added anywhere in the tree.

## Goal

Keep notebook outputs and volatile metadata out of git across the entire repo,
enforced both locally (at commit time) and in CI (on pull requests and pushes).

## Design

Two cooperating pieces plus a single pinned tool version shared between them.

### 1. Local pre-commit hook

New file at repo root: `.pre-commit-config.yaml`

```yaml
repos:
  - repo: https://github.com/kynan/nbstripout
    rev: 0.8.1
    hooks:
      - id: nbstripout
```

Contributors run `pre-commit install` once. Thereafter every staged `*.ipynb`
is stripped of outputs, execution counts, and volatile metadata before the
commit is recorded. The hook applies repo-wide; no path configuration needed
(nbstripout's default `files` matches `*.ipynb`).

### 2. CI check

New file: `.github/workflows/nbstripout.yml`

- **Triggers:** `pull_request` and `push` (matching the convention of the four
  existing workflows in `.github/workflows/`).
- **Path filter:** `['**/*.ipynb', '.pre-commit-config.yaml']` — the job only
  runs when a notebook or the hook config changes.
- **Runner / setup:** `ubuntu-latest` with `astral-sh/setup-uv@v4`, matching the
  existing workflows' toolchain.
- **Check:** runs the *same* pinned hook via
  `uvx pre-commit run nbstripout --all-files`. Because the hook auto-strips,
  any notebook still carrying outputs is modified in the workspace, which makes
  `pre-commit` exit non-zero and **fail the build**, printing the offending
  file diff. Sharing the hook (rather than invoking `nbstripout` directly)
  guarantees CI and the local hook use byte-identical behavior and version.

### 3. Documentation

Intentionally none. The two config files are self-documenting, and adding a
"run `pre-commit install`" note to `CLAUDE.md` is out of scope (YAGNI).

## Non-goals

- Stripping notebooks that are already committed (none currently have outputs).
- Configuring nbstripout to keep specific outputs or metadata keys — defaults
  are sufficient.
- A root README section on contributor setup.

## Verification

- `uvx pre-commit run nbstripout --all-files` passes locally on the current
  tree (the existing notebook is already clean).
- Intentionally adding an output to a notebook makes the same command fail —
  confirming the gate works — then reverting restores green.
- Workflow YAML parses and triggers only on the intended paths.
