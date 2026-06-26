"""Pytest fixtures: ensure vendored libs exist, run the static server."""
from __future__ import annotations

import glob
import os
import shutil
import socket
import subprocess
import threading
import time
import urllib.request
from urllib.error import HTTPError, URLError

import pytest

HERE = os.path.dirname(os.path.abspath(__file__))
WEB = os.path.join(HERE, "web")
VENDOR = os.path.join(WEB, "vendor")
TESTS_FIXTURES = os.path.join(HERE, "tests", "fixtures")
WEB_FIXTURES = os.path.join(WEB, "fixtures")


def _stage_fixtures() -> None:
    """Copy tests/fixtures/ into web/fixtures/ so the test server serves them
    (e.g. /fixtures/movie/000000.drc). The served copy is gitignored."""
    if os.path.exists(WEB_FIXTURES):
        shutil.rmtree(WEB_FIXTURES)
    shutil.copytree(
        TESTS_FIXTURES, WEB_FIXTURES,
        ignore=shutil.ignore_patterns("*.py", "__pycache__"),
    )


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.fixture(scope="session", autouse=True)
def vendored():
    """Ensure web/vendor/ is populated (runs vendor.sh once if missing)."""
    sentinel = os.path.join(VENDOR, "three.module.js")
    if not os.path.exists(sentinel):
        subprocess.run(["bash", os.path.join(HERE, "vendor.sh")], check=True)
    assert os.path.exists(sentinel), "vendor.sh did not populate web/vendor/"
    _stage_fixtures()


@pytest.fixture(scope="session")
def browser_type_launch_args(browser_type_launch_args):
    """Use Playwright's own bundled Chromium when present; otherwise fall back to
    a pre-provisioned Chromium under PLAYWRIGHT_BROWSERS_PATH. CI/web-session
    images often ship a pinned Chromium at a different revision than the installed
    Playwright expects, which would otherwise fail with "Executable doesn't
    exist". On a normal dev machine the bundled browser exists, so this is a no-op."""
    args = dict(browser_type_launch_args)
    if "executable_path" in args:
        return args
    try:
        from playwright.sync_api import sync_playwright
        with sync_playwright() as p:
            bundled = p.chromium.executable_path
        if os.path.exists(bundled):
            return args
    except Exception:
        pass
    base = os.environ.get("PLAYWRIGHT_BROWSERS_PATH", "/opt/pw-browsers")
    found = sorted(glob.glob(os.path.join(base, "chromium-*", "chrome-linux", "chrome")))
    if found:
        args["executable_path"] = found[-1]
    return args


@pytest.fixture(autouse=True)
def _unobstructed_final_frame(request):
    """Leave each browser test on an unobstructed rendered cloud for its
    final-state screenshot: close the controls modal (if open) before
    pytest-playwright captures the PNG, so the e2e report shows the point cloud
    rather than the menu panel over it.

    Resolves `page` lazily via `request` so it does NOT drag a browser into the
    pure-Python unit tests (server / build) that never use one — forcing `page`
    there would spawn a blank tab and emit white screenshots."""
    yield
    if "page" not in request.fixturenames:
        return
    try:
        page = request.getfixturevalue("page")
        if page.get_by_test_id("controls").count() > 0:
            page.get_by_test_id("menu-toggle").click()
            page.get_by_test_id("controls").wait_for(state="detached", timeout=2000)
        page.wait_for_timeout(150)  # settle a frame so the cloud, not a transition, is caught
    except Exception:
        pass


@pytest.fixture()
def server_url(vendored):
    from serve import make_server  # imported here so HERE is on sys.path

    port = _free_port()
    httpd = make_server(WEB, port)
    thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    thread.start()
    base = f"http://127.0.0.1:{port}"
    # wait until it accepts a connection
    for _ in range(50):
        try:
            urllib.request.urlopen(base + "/", timeout=0.2)
            break
        except HTTPError:
            break  # server is up; an HTTP status still means it's listening
        except (URLError, OSError):
            time.sleep(0.05)
    try:
        yield base
    finally:
        httpd.shutdown()
        thread.join(timeout=2)
