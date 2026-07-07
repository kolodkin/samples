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


def test_stage_param_and_determinism(server_url, page):
    # Each named stage builds a scene with obstacles; same seed → same layout.
    page.goto(server_url + "/?autostart=1&seed=7&stage=desert")
    _wait_ready(page)
    s1 = page.evaluate("() => window.__ARCHER.state")
    assert s1["stage"] == "desert"
    assert len(s1["obstacles"]) > 5
    assert page.evaluate("() => window.__ARCHER.visiblePixelCount()") > 500

    page.goto(server_url + "/?autostart=1&seed=7&stage=desert")
    _wait_ready(page)
    s2 = page.evaluate("() => window.__ARCHER.state")
    assert s1["obstacles"] == s2["obstacles"]


def test_each_stage_builds(server_url, page):
    for name in ("forest", "desert", "iceberg"):
        page.goto(server_url + f"/?autostart=1&seed=3&stage={name}")
        _wait_ready(page)
        state = page.evaluate("() => window.__ARCHER.state")
        assert state["stage"] == name
        assert len(state["obstacles"]) > 5


def test_bow_draw_charges_and_releases(server_url, page):
    page.goto(server_url + BOOT)
    _wait_ready(page)
    page.mouse.move(640, 360)
    page.mouse.down()
    page.wait_for_function("() => window.__ARCHER.state.drawPower > 0.4", timeout=5000)
    power = page.evaluate("() => window.__ARCHER.state.drawPower")
    assert 0.4 < power <= 1.0
    page.mouse.up()
    assert page.evaluate("() => window.__ARCHER.state.drawPower") == 0


def test_arrow_flies_and_lands(server_url, page):
    page.goto(server_url + BOOT)
    _wait_ready(page)
    page.evaluate("() => window.__ARCHER.fireAt(0, 1, 0)")
    assert page.evaluate("() => window.__ARCHER.state.arrowCount") == 1
    # Gravity brings it down; ground impact removes it.
    page.wait_for_function("() => window.__ARCHER.state.arrowCount === 0", timeout=10000)


def test_mouse_release_fires_arrow(server_url, page):
    page.goto(server_url + BOOT)
    _wait_ready(page)
    page.mouse.move(640, 360)
    page.mouse.down()
    page.wait_for_function("() => window.__ARCHER.state.drawPower > 0.5", timeout=5000)
    page.mouse.up()
    page.wait_for_function("() => window.__ARCHER.state.arrowCount === 1", timeout=2000)
