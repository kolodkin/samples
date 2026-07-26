GitHub Exposure Scanner
---

Step 1 of a cyber-risk bot: given a GitHub organization (or specific `org/repo` targets), it enumerates the org's public repositories, mirror-clones each, and scans their **full git history** for leaked secrets using a built-in regex rule library — catching secrets that were committed and later removed but remain in history. Each finding is attributed to the commit that introduced it and flagged as live-at-HEAD or historical-only, then each org's exposure is scored and rendered as a redacted report. Secrets are never shown in full — only masked fingerprints and their location. Use `--head-only` for a fast current-HEAD scan. Findings and a per-org exposure summary can optionally be published to Airtable. It demonstrates aaiclick external-API ingestion, dynamic per-repo fan-out, `create_object_from_value`, SQL aggregation over `Object`s, and gated Airtable publishing.

```bash
./github-exposure-scanner.sh --targets "octocat/Hello-World"
```
