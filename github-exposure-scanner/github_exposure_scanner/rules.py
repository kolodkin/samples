"""Built-in regex rule library for leaked-secret detection.

Detection is pure Python over file text. Every match is redacted at
detection time — raw secret values never leave this module.
"""

import re
from dataclasses import dataclass

SEVERITY_WEIGHT: dict[str, int] = {"Critical": 10, "High": 5, "Medium": 2, "Low": 1}


@dataclass(frozen=True)
class _Rule:
    id: str
    secret_type: str
    severity: str
    pattern: re.Pattern
    value_group: int = 0  # which capture group holds the secret (0 = whole match)


RULES: list[_Rule] = [
    _Rule("aws-access-key", "AWS Key", "Critical", re.compile(r"AKIA[0-9A-Z]{16}")),
    _Rule("github-pat", "GitHub PAT", "Critical", re.compile(r"gh[pousr]_[A-Za-z0-9]{36}")),
    _Rule("stripe-live", "Stripe Key", "Critical", re.compile(r"sk_live_[0-9A-Za-z]{24}")),
    _Rule(
        "private-key",
        "Private Key",
        "Critical",
        re.compile(r"-----BEGIN (?:RSA |EC |DSA |OPENSSH )?PRIVATE KEY-----"),
    ),
    _Rule("slack-token", "Slack Token", "High", re.compile(r"xox[baprs]-[A-Za-z0-9-]{10,}")),
    _Rule("google-api-key", "Google API Key", "High", re.compile(r"AIza[0-9A-Za-z_\-]{35}")),
    _Rule(
        "jwt",
        "JWT",
        "Medium",
        re.compile(r"eyJ[A-Za-z0-9_\-]+\.eyJ[A-Za-z0-9_\-]+\.[A-Za-z0-9_\-]+"),
    ),
    _Rule(
        "high-entropy-assignment",
        "High-entropy",
        "Low",
        re.compile(
            r"""(?ix)
            (?:secret|token|password|passwd|api[_-]?key|access[_-]?key)
            ['"]?\s*[:=]\s*['"]([A-Za-z0-9+/=_\-]{16,})['"]
            """
        ),
        value_group=1,
    ),
]


@dataclass(frozen=True)
class Finding:
    path: str
    line: int
    rule_id: str
    secret_type: str
    severity: str
    masked_value: str


# --- Context heuristics: distinguish a throwaway/demo secret from a real leak ---

# Secret types whose risk depends on context. Provider tokens (AWS/GitHub/
# Slack/Google/Stripe) are live credentials — a real one is a leak wherever it
# sits — so they are NOT demoted by path. Certs, generic high-entropy strings,
# and JWTs are the ambiguous classes this applies to.
CONTEXT_SENSITIVE_TYPES = {"Private Key", "High-entropy", "JWT"}

# Tier 1 — path/name markers that signal test/example/dev material.
_TEST_PATH_TOKENS = (
    "localhost", "test", "tests", "__tests__", "spec", "e2e", "fixture",
    "fixtures", "testdata", "example", "examples", "sample", "samples",
    "demo", "mock", "dummy", "/dev/", "/docs/", "/doc/",
)

# Tier 2 — a sibling script whose job is to (re)generate certs/keys, which
# means the committed key is disposable by design.
_CERTGEN_RE = re.compile(r"(make[-_]?cert|gen[-_]?cert|create[-_]?cert|mkcert|gencert|cert.*\.sh$)", re.IGNORECASE)


def _dirname(path: str) -> str:
    return path.rsplit("/", 1)[0] if "/" in path else ""


def _basename(path: str) -> str:
    return path.rsplit("/", 1)[-1]


def classify_context(secret_type: str, path: str, tree_paths: list[str]) -> tuple[str, float]:
    """Classify a finding's likely realness → ``(context_label, confidence_real)``.

    ``confidence_real`` in [0, 1] scales the finding's risk weight. Provider
    tokens always score 1.0. For the ambiguous classes, a sibling cert-gen
    script (Tier 2) is the strongest "disposable" signal, then a test/example
    path marker (Tier 1); otherwise it is treated as production.
    """
    if secret_type not in CONTEXT_SENSITIVE_TYPES:
        return ("production", 1.0)

    # Tier 2: a cert-gen script in the key's directory or any ancestor of it
    # (the common layout puts make-cert.sh one level above a certs/ subdir).
    key_dir = _dirname(path)
    for p in tree_paths:
        if not _CERTGEN_RE.search(_basename(p)):
            continue
        script_dir = _dirname(p)
        if key_dir == script_dir or key_dir.startswith(script_dir + "/") or script_dir == "":
            return ("likely-test: cert-gen script nearby", 0.05)

    lowered = path.lower()
    if any(tok in lowered for tok in _TEST_PATH_TOKENS):
        return ("likely-test: path marker", 0.1)

    return ("production", 1.0)


def mask(value: str) -> str:
    """Redact a secret to ``first4 + "••••" + last4`` (fully hidden if short)."""
    if len(value) <= 8:
        return "••••"
    return f"{value[:4]}••••{value[-4:]}"


def scan_text(path: str, text: str) -> list[Finding]:
    """Scan file text line-by-line against every rule; return redacted findings."""
    findings: list[Finding] = []
    for lineno, line in enumerate(text.splitlines(), start=1):
        for rule in RULES:
            for m in rule.pattern.finditer(line):
                value = m.group(rule.value_group)
                findings.append(
                    Finding(
                        path=path,
                        line=lineno,
                        rule_id=rule.id,
                        secret_type=rule.secret_type,
                        severity=rule.severity,
                        masked_value=mask(value),
                    )
                )
    return findings
