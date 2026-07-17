GitHub Exposure Scanner
---

Step 1 of a cyber-risk bot: given a GitHub organization (or specific `org/repo` targets), it enumerates the org's public repositories, scans their current file contents for leaked secrets using a built-in regex rule library, scores each org's exposure, and prints a redacted report. Secrets are never shown in full — only masked fingerprints and their location. Findings and a per-org exposure summary can optionally be published to Airtable. It demonstrates aaiclick external-API ingestion, `create_object_from_value`, SQL aggregation over `Object`s, and gated Airtable publishing.

```bash
./github-exposure-scanner.sh --targets "octocat/Hello-World"
```
