"""End-to-end tests of the dynamic fan-out job via the in-process runner.

Covers the default history-scan path (local git fixtures, no network) and the
legacy HEAD-only path (canned API fixtures).
"""

import os

from aaiclick.orchestration.execution.debug import ajob_test
from aaiclick.orchestration.models import JOB_COMPLETED

from github_exposure_scanner import exposure_pipeline

from gitutil import make_repo

FIXTURES = os.path.join(os.path.dirname(__file__), "fixtures")
AWS_KEY = "AKIAIOSFODNN7EXAMPLE"


async def test_head_only_fan_out(orch_ctx, monkeypatch, tmp_path):
    monkeypatch.setenv("GHX_FIXTURE_DIR", FIXTURES)
    report_file = tmp_path / "report.md"
    monkeypatch.setenv("AAICLICK_REPORT_FILE", str(report_file))

    job = await exposure_pipeline(targets=["acme", "acme/proxytool"], max_repos=25, head_only=True)
    await ajob_test(job)

    assert job.status == JOB_COMPLETED, f"Job failed: {job.error}"
    report = report_file.read_text()
    assert "AWS Key" in report
    assert "Private Key" in report
    assert AWS_KEY not in report  # redacted


async def test_history_fan_out(orch_ctx, monkeypatch, tmp_path):
    # Listing uses the canned API fixtures; scanning uses local git repos.
    monkeypatch.setenv("GHX_FIXTURE_DIR", FIXTURES)
    git_root = tmp_path / "gitfx"
    make_repo(str(git_root / "acme" / "widgets"), [
        {"message": "leak", "date": "2020-01-01",
         "files": {"src/config.py": f"AWS = '{AWS_KEY}'\n"}},
    ])
    make_repo(str(git_root / "acme" / "proxytool"), [
        {"message": "add key", "date": "2019-05-05",
         "files": {"id_rsa": "-----BEGIN RSA PRIVATE KEY-----\nabc\n"}},
        {"message": "scrub key", "date": "2019-05-06", "files": {"id_rsa": "cleaned\n"}},
    ])
    monkeypatch.setenv("GHX_GIT_FIXTURE_DIR", str(git_root))
    report_file = tmp_path / "report.md"
    monkeypatch.setenv("AAICLICK_REPORT_FILE", str(report_file))

    job = await exposure_pipeline(targets=["acme/widgets", "acme/proxytool"], max_repos=25)
    await ajob_test(job)

    assert job.status == JOB_COMPLETED, f"Job failed: {job.error}"
    report = report_file.read_text()
    assert "AWS Key" in report          # widgets, still live at HEAD
    assert "Private Key" in report      # proxytool, historical-only
    assert "Live at HEAD" in report     # grouping headings present
    assert "Historical-only" in report
    assert AWS_KEY not in report        # redacted
