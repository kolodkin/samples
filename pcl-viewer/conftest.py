"""Pytest fixtures: ensure vendored libs exist, run the static server."""
from __future__ import annotations

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
