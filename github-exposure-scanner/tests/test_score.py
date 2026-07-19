import math
import os

from aaiclick import ORIENT_DICT
from aaiclick.data.data_context import data_context

from github_exposure_scanner.github_api import GitHubClient
from github_exposure_scanner.scan import list_repos_impl, scan_repos_impl
from github_exposure_scanner.score import compute_score, risk_band, score_exposure_impl

FIXTURES = os.path.join(os.path.dirname(__file__), "fixtures")


def test_risk_band_thresholds():
    assert risk_band(0) == "Clean"
    assert risk_band(5) == "Low"
    assert risk_band(10) == "Medium"
    assert risk_band(30) == "High"
    assert risk_band(80) == "Critical"


def test_compute_score_weights_and_popularity():
    # weighted_base is Σ severity_weight × confidence (one full-confidence Critical = 10)
    assert compute_score(10, flagged_stars=0) == 10
    assert compute_score(10, flagged_stars=1200) == round(10 * (1 + math.log10(1201)))
    # a likely-test Critical at confidence 0.05 barely moves the needle
    assert compute_score(10 * 0.05, flagged_stars=1200) < compute_score(10, flagged_stars=0)


async def test_context_downgrades_demo_key_end_to_end():
    # proxytool commits a localhost private key next to make-cert.sh — the
    # scanner should flag it (Critical rule) but score it as likely-test.
    client = GitHubClient(fixture_dir=FIXTURES)
    async with data_context():
        repos, _ = await list_repos_impl(["acme/proxytool"], 25, client, "2026-07-17", scope=None)
        findings = await scan_repos_impl(repos, 512, client, scope=None)
        fdata = await findings.data(orient=ORIENT_DICT)
        idx = fdata["secret_type"].index("Private Key")
        assert fdata["confidence"][idx] == 0.05
        assert "cert-gen" in fdata["context"][idx]

        summary = await score_exposure_impl(repos, findings, scope=None)
        sdata = await summary.data(orient=ORIENT_DICT)
        i = sdata["org"].index("acme")
        assert sdata["critical"][i] == 1  # still counted as a critical-pattern hit
        assert sdata["likely_test"][i] == 1
        assert sdata["risk_band"][i] in {"Clean", "Low"}  # but downgraded by confidence


async def test_score_exposure_summary():
    client = GitHubClient(fixture_dir=FIXTURES)
    async with data_context():
        repos, _ = await list_repos_impl(["acme/widgets"], 25, client, "2026-07-17", scope=None)
        findings = await scan_repos_impl(repos, 512, client, scope=None)
        summary = await score_exposure_impl(repos, findings, scope=None)
        data = await summary.data(orient=ORIENT_DICT)
        i = data["org"].index("acme")
        assert data["repos_scanned"][i] == 1
        assert data["critical"][i] >= 1
        assert data["exposure_score"][i] > 0
        assert data["risk_band"][i] in {"Low", "Medium", "High", "Critical"}
        assert data["top_secret_type"][i] == "AWS Key"
        assert data["scan_errors"][i] == 0
