import urllib.request


def test_serves_index_html(server_url):
    with urllib.request.urlopen(server_url + "/") as resp:
        body = resp.read().decode()
        assert resp.status == 200
        assert "<div id=\"app\">" in body


def test_serves_js_with_module_mime(server_url):
    # A .js file under web/ must be served with a JS MIME so <script type=module> loads.
    with urllib.request.urlopen(server_url + "/app.js") as resp:
        assert resp.status == 200
        assert "javascript" in resp.headers.get("Content-Type", "")


def test_serves_pcd_as_octet_stream(server_url):
    with urllib.request.urlopen(server_url + "/models/kitti-velodyne-000000.pcd") as resp:
        assert resp.status == 200
        assert resp.headers.get("Content-Type") == "application/octet-stream"
