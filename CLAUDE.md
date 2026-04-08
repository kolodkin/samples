# Writing Guidelines

Many platforms (Reddit, DEV.to, Medium, Hacker News) use bot detection and spam filters. Posts that look automated, overly polished, or promotional get flagged or removed. Follow these guidelines to keep posts authentic.

## Make Posts Personal

- **Use first person** — "we found", "we were storing". Share real experience, not tutorials.
- **Ask a question** — End with a genuine question to invite discussion, not just "Happy to discuss!"
- **Reduce links** — Keep links to a minimum. Offer extras in comments if people ask.
- **Avoid jargon** — "Our analytics pipeline" not "our cyber enterprise solution."
- **No cross-posting** — Don't post the same content to multiple platforms at once. Adapt tone and format per platform.
- **Engage first** — Comment on other posts before publishing your own.
- **Tone** — Conversational, not polished. Communities reward authenticity over formatting.

## README Convention

Each example project MUST have a `README.md` with exactly this structure:

```markdown
Project Title
---

One-paragraph description of what the project does and which aaiclick features it demonstrates.

\```bash
./<name>.sh
\```
```

- **Title**: setext heading (underline with `---`)
- **Description**: one paragraph, concise — what it does, what data it uses
- **Run command**: bash code block with shell script invocation, plus any flags or env vars if applicable
- No additional sections or headings — keep it minimal

READMEs are included in the docs site via `docs/example_projects.md` using `pymdownx.snippets`.

## Project Structure

Each example project is a standalone directory containing a nested Python package with the same name:

```
example_projects/<name>/
├── <name>/              # Python package (runnable via `python -m <name>`)
│   ├── __init__.py      # Main logic: @job/@task definitions or standalone async workflow
│   ├── __main__.py      # Entry point for `python -m <name>`
│   ├── report.py        # Report rendering (rich tables, Object.markdown(), or print)
│   └── requirements.txt # Extra dependencies not in aaiclick core (optional)
├── <name>.sh            # Shell runner: sets env vars, calls python -m, manages workers
└── README.md            # Title, description, how to run (see README Convention above)
```

- The nested `<name>/` folder is the Python package — the outer folder is the project directory
- `__main__.py` imports and calls `main()` from `__init__.py`
- `<name>.sh` is the user-facing entry point — `cd`s to its own directory, runs `python -m <name>`
- Shell scripts use `PYTHON="${PYTHON:-uv run python}"` for dual-mode support (monorepo or standalone)
- Orchestration projects: `.sh` registers the job, starts worker, polls status, stops worker
- Each example project should have a `report.py` file containing final report printout logic
- The `@job` function returns the terminal task directly (e.g. `return report`) — the framework auto-discovers all upstream tasks via the dependency graph; `report.py` is only responsible for the printout
- Always prefer `Object.markdown()` for rendering tables in `report.py` — avoid custom table rendering logic
