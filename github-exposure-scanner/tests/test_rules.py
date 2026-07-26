from github_exposure_scanner.rules import (
    Finding,
    classify_context,
    finding_confidence,
    mask,
    scan_text,
    SEVERITY_WEIGHT,
)

REAL_PEM = (
    "-----BEGIN RSA PRIVATE KEY-----\n"
    + "\n".join(["MIIEow" + "A" * 58] * 25)
    + "\n-----END RSA PRIVATE KEY-----\n"
)
STUB_PEM = "-----BEGIN RSA PRIVATE KEY-----\nabc\n"


def _pk_finding(path, text):
    return next(f for f in scan_text(path, text) if f.secret_type == "Private Key")


def test_mask_hides_middle():
    assert mask("AKIAIOSFODNN7EXAMPLE") == "AKIA••••MPLE"


def test_mask_short_value_fully_hidden():
    assert mask("abcd") == "••••"


def test_detects_aws_key_with_location():
    text = "line one\nkey = AKIAIOSFODNN7EXAMPLE\n"
    findings = scan_text("config.py", text)
    aws = [f for f in findings if f.secret_type == "AWS Key"]
    assert len(aws) == 1
    assert aws[0].line == 2
    assert aws[0].path == "config.py"
    assert aws[0].severity == "Critical"
    assert "AKIAIOSFODNN7EXAMPLE" not in aws[0].masked_value


def test_detects_github_pat():
    text = "token: ghp_" + "a" * 36
    findings = scan_text("x.env", text)
    assert any(f.secret_type == "GitHub PAT" for f in findings)


def test_high_entropy_assignment_masks_only_value():
    text = 'password = "s3cr3tV4lue_ABCDEFGHIJ"'
    findings = scan_text("s.py", text)
    he = [f for f in findings if f.secret_type == "High-entropy"]
    assert len(he) == 1
    assert "s3cr3tV4lue_ABCDEFGHIJ" not in he[0].masked_value


def test_clean_text_no_findings():
    assert scan_text("ok.py", "def add(a, b):\n    return a + b\n") == []


def test_severity_weight_table():
    assert SEVERITY_WEIGHT == {"Critical": 10, "High": 5, "Medium": 2, "Low": 1}


def test_context_provider_token_always_full_confidence():
    # A real provider token is a leak wherever it sits — even in a test file.
    ctx, conf = classify_context("AWS Key", "tests/fixtures/creds.txt", ["tests/fixtures/creds.txt"])
    assert conf == 1.0
    assert ctx == "production"


def test_context_certgen_ancestor_downgrades_hardest():
    # make-cert.sh one level above the certs/ dir (the burnside layout) → 0.05
    tree = ["proxy/certs/localhost.privkey.pem", "proxy/certs/localhost.cert.pem", "proxy/make-cert.sh"]
    ctx, conf = classify_context("Private Key", "proxy/certs/localhost.privkey.pem", tree)
    assert conf == 0.05
    assert "cert-gen" in ctx

    # same directory also counts
    tree2 = ["certs/server.pem", "certs/gen-cert.sh"]
    ctx2, conf2 = classify_context("Private Key", "certs/server.pem", tree2)
    assert conf2 == 0.05


def test_context_path_marker_downgrades():
    ctx, conf = classify_context("Private Key", "test/data/id_rsa", ["test/data/id_rsa"])
    assert conf == 0.1
    assert "path marker" in ctx


def test_context_production_key_stays_full():
    ctx, conf = classify_context("Private Key", "deploy/prod/id_rsa", ["deploy/prod/id_rsa"])
    assert conf == 1.0
    assert ctx == "production"


def test_pem_stub_body_downgraded_even_in_prod_path():
    # A header with no real key body is a reference, not a leak — demote it
    # regardless of a production-looking path.
    f = _pk_finding("deploy/prod/id_rsa", STUB_PEM)
    ctx, conf = finding_confidence(f, ["deploy/prod/id_rsa"])
    assert conf <= 0.05
    assert "no key body" in ctx


def test_pem_real_body_stays_full_in_prod_path():
    f = _pk_finding("deploy/prod/id_rsa", REAL_PEM)
    ctx, conf = finding_confidence(f, ["deploy/prod/id_rsa"])
    assert conf == 1.0
    assert ctx == "production"


def test_finding_confidence_takes_min_of_path_and_body():
    # Real-bodied key in a test path is still demoted by the path heuristic.
    f = _pk_finding("tests/fixtures/id_rsa", REAL_PEM)
    ctx, conf = finding_confidence(f, ["tests/fixtures/id_rsa"])
    assert conf == 0.1
    assert "path marker" in ctx


def test_context_repo_root_docs_marker_matches():
    # A repo-root-relative docs/ path (no leading slash) is still a doc location.
    # Path is chosen to contain no other marker word — only the docs/ segment.
    ctx, conf = classify_context(
        "Private Key", "docs/architecture/prod-keys.md", ["docs/architecture/prod-keys.md"]
    )
    assert conf == 0.1
    assert "path marker" in ctx
