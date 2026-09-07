"""End-to-end test of the sync job via the in-process runner (fixture mode)."""

import os

from aaiclick.orchestration.execution.debug import ajob_test
from aaiclick.orchestration.models import JOB_COMPLETED

from google_health_airtable_sync import health_sync_pipeline

FIXTURES = os.path.join(os.path.dirname(__file__), "fixtures")


async def test_sync_job_end_to_end(orch_ctx, monkeypatch, tmp_path):
    monkeypatch.setenv("GHS_FIXTURE_DIR", FIXTURES)
    monkeypatch.delenv("AIRTABLE_API_KEY", raising=False)
    monkeypatch.delenv("AIRTABLE_BASE_ID", raising=False)
    report_file = tmp_path / "report.md"
    monkeypatch.setenv("AAICLICK_REPORT_FILE", str(report_file))

    job = await health_sync_pipeline(days=7, publish_airtable=True)
    await ajob_test(job)

    assert job.status == JOB_COMPLETED, f"Job failed: {job.error}"
    report = report_file.read_text()
    assert "RUNNING" in report and "WALKING" in report
    assert "6376" in report          # daily steps rolled up
    assert "78.45" in report         # weight in kg
    # Airtable creds absent -> both publishes skipped, and the report says so
    assert report.count("skipped") >= 2
