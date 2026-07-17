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
