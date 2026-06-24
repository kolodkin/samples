"""End-to-end test of the PCL viewer using Playwright (Chromium)."""
import time

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


def test_boot_loader_shows_until_first_render(server_url, page):
    # Until the first frame renders, the canvas is just background colour. With no
    # feedback that reads as a broken/black screen, so a boot loader must cover the
    # gap from page open to first render — for the static initial scene too, which
    # otherwise reports no loading state at all.
    def _delay_model(route):
        time.sleep(1.0)  # hold the city PCD so the not-ready window is observable
        route.continue_()

    page.route("**/kitti-velodyne-000000.pcd", _delay_model)
    page.goto(server_url + "/")
    # Before the cloud is ready, the boot loader is on screen.
    expect(page.get_by_test_id("boot-loading")).to_be_visible()
    _wait_ready(page)
    # Once the first render has happened, it clears.
    expect(page.get_by_test_id("boot-loading")).to_have_count(0)


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


def test_scene_switch_mid_load_clears_loading_indicator(server_url, page):
    # Switching scenes while the movie is still streaming must not leave the
    # "Loading X / Y…" indicator stuck on the now-superseded load: the static
    # scene that supersedes it never managed `loading`, so a stale movie load
    # used to freeze the HUD spinner forever even though the new scene is ready.
    page.goto(server_url + "/?movieBase=/fixtures/movie/&movieCount=4")
    _wait_ready(page)  # city ready
    page.get_by_test_id("menu-toggle").click()
    # Kick off the movie load, then immediately switch back before it finishes
    # streaming all frames — this leaves a movie load in flight (loading == true).
    page.get_by_test_id("scene").select_option("movie")
    page.get_by_test_id("scene").select_option("city")
    page.wait_for_function(
        "() => window.__PCL.scene === 'city' && window.__PCL.ready === true",
        timeout=20000)
    # The superseded movie load's loading flag must be cleared, and the HUD must
    # not show a leftover "Loading…" line.
    assert page.evaluate("() => window.__PCL.loading") is False
    expect(page.get_by_test_id("loading")).to_have_count(0)
