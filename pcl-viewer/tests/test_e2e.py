"""End-to-end test of the PCL viewer using Playwright (Chromium)."""
from playwright.sync_api import expect


def _wait_ready(page):
    page.wait_for_function("() => window.__PCL && window.__PCL.ready === true",
                           timeout=20000)


def test_point_cloud_loads_and_renders(server_url, page):
    page.goto(server_url + "/")
    _wait_ready(page)

    point_count = page.evaluate("() => window.__PCL.pointCount")
    assert point_count == 115385
    assert page.evaluate("() => window.__PCL.scene") == "city"

    # Stats overlay reflects the loaded cloud.
    expect(page.get_by_test_id("point-count")).to_have_text("115,385")

    # The canvas actually drew the cloud (non-background pixels present).
    visible = page.evaluate("() => window.__PCL.handle.visiblePixelCount()")
    assert visible > 1000


def test_point_size_control(server_url, page):
    page.goto(server_url + "/")
    _wait_ready(page)
    page.get_by_test_id("menu-toggle").click()  # controls live in a modal
    slider = page.get_by_test_id("point-size")
    slider.evaluate(
        "el => { el.value = '0.05'; el.dispatchEvent(new Event('input', {bubbles:true})); }")
    page.wait_for_function("() => Math.abs(window.__PCL.settings.pointSize - 0.05) < 1e-6")


def test_color_mode_toggle(server_url, page):
    page.goto(server_url + "/")
    _wait_ready(page)
    # Default mode is the height ramp; toggling to flat must take effect.
    assert page.evaluate("() => window.__PCL.settings.colorMode") == "height"
    page.get_by_test_id("menu-toggle").click()  # controls live in a modal
    page.get_by_test_id("color-mode").select_option("flat")
    page.wait_for_function("() => window.__PCL.settings.colorMode === 'flat'")
    # Each scalar ramp mode applies and keeps the cloud rendering. Intensity
    # only resolves if the loader actually parsed the PCD's `intensity` field;
    # falling back to flat would leave colorMode == 'flat' and fail this loop.
    for mode in ("distance", "intensity", "height"):
        page.get_by_test_id("color-mode").select_option(mode)
        page.wait_for_function(
            f"() => window.__PCL.settings.colorMode === '{mode}'")
        assert page.evaluate("() => window.__PCL.handle.visiblePixelCount()") > 1000


def test_point_shape_toggle(server_url, page):
    page.goto(server_url + "/")
    _wait_ready(page)
    # Points render as 3D balls by default.
    assert page.evaluate("() => window.__PCL.settings.pointShape") == "ball"
    page.get_by_test_id("menu-toggle").click()  # controls live in a modal
    # Switching to the older square sprite takes effect and keeps rendering.
    page.get_by_test_id("point-shape").select_option("square")
    page.wait_for_function("() => window.__PCL.settings.pointShape === 'square'")
    assert page.evaluate("() => window.__PCL.handle.visiblePixelCount()") > 1000
    # And back to balls.
    page.get_by_test_id("point-shape").select_option("ball")
    page.wait_for_function("() => window.__PCL.settings.pointShape === 'ball'")
    assert page.evaluate("() => window.__PCL.handle.visiblePixelCount()") > 1000


def test_reset_camera(server_url, page):
    page.goto(server_url + "/")
    _wait_ready(page)
    # Orbit far away via wheel, then reset and confirm still ready & rendering.
    page.get_by_test_id("menu-toggle").click()  # reset button lives in a modal
    before = page.evaluate("() => window.__PCL.framesRendered")
    page.get_by_test_id("reset").click()
    page.wait_for_function(f"() => window.__PCL.framesRendered > {before}")
    assert page.evaluate("() => window.__PCL.ready") is True


def test_menu_toggle(server_url, page):
    page.goto(server_url + "/")
    _wait_ready(page)
    # Controls modal is closed by default for a clean view.
    expect(page.get_by_test_id("controls")).to_have_count(0)
    page.get_by_test_id("menu-toggle").click()
    expect(page.get_by_test_id("controls")).to_be_visible()
    # Tapping the backdrop closes it again.
    page.get_by_test_id("backdrop").click()
    expect(page.get_by_test_id("controls")).to_have_count(0)


def test_camera_readout(server_url, page):
    page.goto(server_url + "/")
    _wait_ready(page)
    page.wait_for_timeout(600)  # let the 500ms stats cadence tick at least once
    for tid in ("cam-eye", "cam-target"):
        parts = page.get_by_test_id(tid).inner_text().split()
        assert len(parts) == 3, f"{tid} should show x y z, got {parts!r}"
        for p in parts:
            float(p)  # each component parses as a number


def test_screenshot_capture(server_url, page, tmp_path):
    page.goto(server_url + "/")
    _wait_ready(page)
    page.wait_for_timeout(300)  # let a few frames render
    out = tmp_path / "pcl-viewer.png"
    page.screenshot(path=str(out))
    assert out.stat().st_size > 5000


def test_static_scene_from_url(server_url, page):
    # Point the "table" scene at the locally-served model so the static URL path
    # is tested offline (same loadStatic code path as the real PCL table scene).
    page.goto(server_url + "/?pclUrl=/models/kitti-velodyne-000000.pcd")
    _wait_ready(page)
    page.get_by_test_id("menu-toggle").click()
    page.get_by_test_id("scene").select_option("table")
    page.wait_for_function("() => window.__PCL.scene === 'table' && window.__PCL.ready === true",
                           timeout=20000)
    visible = page.evaluate("() => window.__PCL.handle.visiblePixelCount()")
    assert visible > 1000


def test_movie_scene_plays_and_pauses(server_url, page):
    page.goto(server_url + "/?movieBase=/fixtures/movie/&movieCount=4")
    _wait_ready(page)
    page.get_by_test_id("menu-toggle").click()
    page.get_by_test_id("scene").select_option("movie")
    # Generous timeout: the first Draco decode pays a one-time WASM-compile cost
    # that can be slow on cold CI runners / chrome-headless-shell.
    page.wait_for_function(
        "() => window.__PCL.scene === 'movie' && window.__PCL.ready === true && window.__PCL.frameCount === 4",
        timeout=60000)
    # It auto-plays: the frame index advances.
    page.wait_for_function("() => window.__PCL.playing === true")
    start = page.evaluate("() => window.__PCL.frameIndex")
    page.wait_for_function(f"() => window.__PCL.frameIndex !== {start}", timeout=5000)
    # The cloud renders.
    assert page.evaluate("() => window.__PCL.handle.visiblePixelCount()") > 500
    # Pause stops advancement.
    page.get_by_test_id("play-pause").click()
    page.wait_for_function("() => window.__PCL.playing === false")
    frozen = page.evaluate("() => window.__PCL.frameIndex")
    page.wait_for_timeout(700)
    assert page.evaluate("() => window.__PCL.frameIndex") == frozen


def test_movie_frame_step_and_stop(server_url, page):
    page.goto(server_url + "/?movieBase=/fixtures/movie/&movieCount=4")
    _wait_ready(page)
    page.get_by_test_id("menu-toggle").click()
    page.get_by_test_id("scene").select_option("movie")
    page.wait_for_function(
        "() => window.__PCL.scene === 'movie' && window.__PCL.ready === true && window.__PCL.frameCount === 4",
        timeout=60000)
    # Wait for the worker queue to decode every fixture frame so stepping never
    # lands on an undecoded (no-op) slot.
    page.wait_for_function(
        "() => window.__PCL.loadProgress.loaded === window.__PCL.loadProgress.total",
        timeout=60000)

    # Stepping forward pauses playback and advances exactly one frame.
    page.get_by_test_id("frame-next").click()
    page.wait_for_function("() => window.__PCL.playing === false")
    page.get_by_test_id("frame-next").click()
    one = page.evaluate("() => window.__PCL.frameIndex")
    page.get_by_test_id("frame-next").click()
    page.wait_for_function(f"() => window.__PCL.frameIndex === {(one + 1) % 4}")
    # Stepping back returns to the previous frame.
    page.get_by_test_id("frame-prev").click()
    page.wait_for_function(f"() => window.__PCL.frameIndex === {one}")

    # Stop resets to the first frame and stays paused.
    page.get_by_test_id("stop").click()
    page.wait_for_function(
        "() => window.__PCL.frameIndex === 0 && window.__PCL.playing === false")
    page.wait_for_timeout(400)
    assert page.evaluate("() => window.__PCL.frameIndex") == 0
    assert page.evaluate("() => window.__PCL.handle.visiblePixelCount()") > 500


def test_scene_switch_back_stops_movie(server_url, page):
    page.goto(server_url + "/?movieBase=/fixtures/movie/&movieCount=4")
    _wait_ready(page)
    page.get_by_test_id("menu-toggle").click()
    page.get_by_test_id("scene").select_option("movie")
    page.wait_for_function("() => window.__PCL.scene === 'movie' && window.__PCL.ready === true",
                           timeout=60000)  # cold Draco WASM compile can be slow
    page.get_by_test_id("scene").select_option("city")
    page.wait_for_function("() => window.__PCL.scene === 'city' && window.__PCL.ready === true",
                           timeout=20000)
    # Movie timer torn down: not playing, frameCount reset.
    assert page.evaluate("() => window.__PCL.playing") is False
    assert page.evaluate("() => window.__PCL.frameCount") == 0
    assert page.evaluate("() => window.__PCL.pointCount") == 115385
