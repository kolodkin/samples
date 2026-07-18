"""End-to-end test of the dynamic fan-out job via the in-process runner.

Runs the real ``@job`` DAG (entry task → list_repos → scan_repos expander →
one scan_one_repo per repo → score_exposure → generate_report) against the
offline fixture data, asserting the per-repo tasks fan out and their findings
fan back in correctly.
"""

import os

from aaiclick.orchestration.execution.debug import ajob_test
from aaiclick.orchestration.models import JOB_COMPLETED

from github_exposure_scanner import exposure_pipeline

FIXTURES = os.path.join(os.path.dirname(__file__), "fixtures")


async def test_dynamic_fan_out_job(orch_ctx, monkeypatch, tmp_path):
    monkeypatch.setenv("GHX_FIXTURE_DIR", FIXTURES)
    report_file = tmp_path / "report.md"
    monkeypatch.setenv("AAICLICK_REPORT_FILE", str(report_file))

    # 'acme' expands to widgets + docs; 'acme/proxytool' adds the demo-key repo.
    job = await exposure_pipeline(targets=["acme", "acme/proxytool"], max_repos=25)
    await ajob_test(job)

    assert job.status == JOB_COMPLETED, f"Job failed: {job.error}"

    report = report_file.read_text()
    assert "GitHub Exposure" in report
    # widgets' planted AWS key (production) and proxytool's demo key both appear,
    # so fan-out ran per repo and fan-in combined their findings.
    assert "AWS Key" in report
    assert "Private Key" in report
    assert "likely-test" in report  # proxytool key downgraded by the heuristic
    assert "AKIAIOSFODNN7EXAMPLE" not in report  # still redacted
