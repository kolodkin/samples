"""End-to-end tests for the archer game (Playwright, Chromium)."""
from playwright.sync_api import expect

# Deterministic, menu-skipping boot used by most tests.
BOOT = "/?autostart=1&seed=42"


def _wait_ready(page):
    page.wait_for_function(
        "() => window.__ARCHER && window.__ARCHER.ready === true", timeout=30000
    )


def test_boot_renders(server_url, page):
    page.goto(server_url + BOOT)
    _wait_ready(page)
    state = page.evaluate("() => window.__ARCHER.state")
    assert state["screen"] == "playing"
    # The canvas actually drew something (sky background is excluded).
    assert page.evaluate("() => window.__ARCHER.visiblePixelCount()") > 500
