"""End-to-end tests for the "Load PCL" local-file menu (.pcd/.csv/.parquet)."""
import os

from playwright.sync_api import expect

# Boot the page offline (default movie scene points at local fixtures), then the
# tests immediately load a local file over it — loadFile cancels the movie load.
BOOT = "/?movieBase=/fixtures/movie/&movieCount=4"

FIXTURES = os.path.join(os.path.dirname(os.path.abspath(__file__)), "fixtures", "file")


def _open_menu(page, server_url):
    page.goto(server_url + BOOT)
    page.wait_for_function("() => window.__PCL && window.__PCL.handle", timeout=60000)
    page.get_by_test_id("menu-toggle").click()


def _load(page, filename):
    page.get_by_test_id("file-input").set_input_files(os.path.join(FIXTURES, filename))
    page.wait_for_function(
        "() => window.__PCL.scene === 'file' && window.__PCL.ready === true",
        timeout=30000,
    )


def _color_mode_options(page):
    return page.get_by_test_id("color-mode").evaluate(
        "el => Array.from(el.options).map(o => o.value)")


def test_load_csv_with_header(server_url, page):
    _open_menu(page, server_url)
    _load(page, "cloud.csv")
    # All 2100 rows parsed and rendered.
    assert page.evaluate("() => window.__PCL.pointCount") == 2100
    assert page.evaluate("() => window.__PCL.handle.visiblePixelCount()") > 500
    # x,y,z,i,c schema ⇒ class + intensity both offered; class is the default.
    assert _color_mode_options(page) == ["flat", "class", "height", "distance", "intensity"]
    assert page.evaluate("() => window.__PCL.settings.colorMode") == "class"
    # The class palette is vivid, so by-class renders chromatic pixels.
    assert page.evaluate("() => window.__PCL.handle.colorfulPixelCount()") > 100
    # Dynamic legend lists the three distinct classes, and the file name shows.
    expect(page.get_by_test_id("legend")).to_be_visible()
    assert page.get_by_test_id("legend").locator(".legend-item").count() == 3
    expect(page.get_by_test_id("file-name")).to_have_text("cloud.csv")


def test_load_csv_without_header_positional(server_url, page):
    _open_menu(page, server_url)
    _load(page, "cloud_noheader.csv")
    # Five positional columns inferred as x,y,z,i,c (4th numeric, 5th string).
    assert page.evaluate("() => window.__PCL.pointCount") == 2100
    assert _color_mode_options(page) == ["flat", "class", "height", "distance", "intensity"]
    assert page.evaluate("() => window.__PCL.handle.colorfulPixelCount()") > 100


def test_load_pcd(server_url, page):
    _open_menu(page, server_url)
    _load(page, "cloud.pcd")
    assert page.evaluate("() => window.__PCL.pointCount") == 2100
    assert page.evaluate("() => window.__PCL.handle.visiblePixelCount()") > 500
    # PCD carries x,y,z + intensity but no class column.
    assert _color_mode_options(page) == ["flat", "height", "distance", "intensity"]
    expect(page.get_by_test_id("legend")).to_have_count(0)


def test_load_parquet(server_url, page):
    _open_menu(page, server_url)
    _load(page, "cloud.parquet")
    assert page.evaluate("() => window.__PCL.pointCount") == 2100
    assert _color_mode_options(page) == ["flat", "class", "height", "distance", "intensity"]
    assert page.evaluate("() => window.__PCL.settings.colorMode") == "class"
    assert page.evaluate("() => window.__PCL.handle.colorfulPixelCount()") > 100


def test_unsupported_file_shows_error(server_url, page, tmp_path):
    _open_menu(page, server_url)
    bogus = tmp_path / "notes.xyz"
    bogus.write_text("hello world\n")
    page.get_by_test_id("file-input").set_input_files(str(bogus))
    # Parsing fails: an error surfaces and the scene never reaches ready.
    page.wait_for_function("() => window.__PCL.error && /Unsupported/.test(window.__PCL.error)",
                           timeout=10000)
    expect(page.get_by_test_id("error")).to_be_visible()
    # The viewer recovers from a bad load: a subsequent valid file renders and
    # clears the error (also leaves the final-state screenshot on a real cloud).
    _load(page, "cloud.csv")
    assert page.evaluate("() => window.__PCL.error") is None
    assert page.evaluate("() => window.__PCL.handle.visiblePixelCount()") > 500


def test_load_file_then_switch_to_builtin_scene(server_url, page):
    _open_menu(page, server_url)
    _load(page, "cloud.csv")
    expect(page.get_by_test_id("file-name")).to_be_visible()
    # Switching to a built-in scene (movie is offline via BOOT) clears the
    # loaded-file name + legend immediately.
    page.get_by_test_id("scene").select_option("movie")
    page.wait_for_function("() => window.__PCL.scene === 'movie' && window.__PCL.fileName === null",
                           timeout=60000)
    expect(page.get_by_test_id("file-name")).to_have_count(0)
