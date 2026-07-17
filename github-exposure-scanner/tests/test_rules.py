from github_exposure_scanner.rules import Finding, mask, scan_text, SEVERITY_WEIGHT


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
