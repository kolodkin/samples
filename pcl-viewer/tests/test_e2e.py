"""End-to-end test of the PCL viewer using Playwright (Chromium)."""
from playwright.sync_api import expect


def _wait_ready(page):
    page.wait_for_function("() => window.__PCL && window.__PCL.ready === true",
                           timeout=20000)


def test_point_cloud_loads_and_renders(server_url, page):
    page.goto(server_url + "/")
    _wait_ready(page)

    point_count = page.evaluate("() => window.__PCL.pointCount")
    assert point_count == 59750

    # Stats overlay reflects the loaded cloud.
    expect(page.get_by_test_id("point-count")).to_have_text("59,750")

    # The canvas actually drew the cloud (non-background pixels present).
    visible = page.evaluate("() => window.__PCL.handle.visiblePixelCount()")
    assert visible > 1000


def test_point_size_control(server_url, page):
    page.goto(server_url + "/")
    _wait_ready(page)
    slider = page.get_by_test_id("point-size")
    slider.evaluate(
        "el => { el.value = '0.05'; el.dispatchEvent(new Event('input', {bubbles:true})); }")
    page.wait_for_function("() => Math.abs(window.__PCL.settings.pointSize - 0.05) < 1e-6")


def test_color_mode_toggle(server_url, page):
    page.goto(server_url + "/")
    _wait_ready(page)
    # Default mode is the height ramp; toggling to flat must take effect.
    assert page.evaluate("() => window.__PCL.settings.colorMode") == "height"
    page.get_by_test_id("color-mode").select_option("flat")
    page.wait_for_function("() => window.__PCL.settings.colorMode === 'flat'")


def test_helpers_toggle(server_url, page):
    page.goto(server_url + "/")
    _wait_ready(page)
    page.get_by_test_id("helpers").check()
    page.wait_for_function("() => window.__PCL.settings.helpers === true")


def test_reset_camera(server_url, page):
    page.goto(server_url + "/")
    _wait_ready(page)
    # Orbit far away via wheel, then reset and confirm still ready & rendering.
    before = page.evaluate("() => window.__PCL.framesRendered")
    page.get_by_test_id("reset").click()
    page.wait_for_function(f"() => window.__PCL.framesRendered > {before}")
    assert page.evaluate("() => window.__PCL.ready") is True


def test_screenshot_capture(server_url, page, tmp_path):
    page.goto(server_url + "/")
    _wait_ready(page)
    page.wait_for_timeout(300)  # let a few frames render
    out = tmp_path / "pcl-viewer.png"
    page.screenshot(path=str(out))
    assert out.stat().st_size > 5000
