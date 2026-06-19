"""Minimal static file server for the PCL viewer.

Serves the web/ directory with correct MIME types for ES modules and .pcd
assets, and disables caching so edits show up immediately.
"""
from __future__ import annotations

import argparse
import os
from functools import partial
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer


class ViewerHandler(SimpleHTTPRequestHandler):
    extensions_map = {
        **SimpleHTTPRequestHandler.extensions_map,
        ".js": "text/javascript",
        ".mjs": "text/javascript",
        ".pcd": "application/octet-stream",
        ".css": "text/css",
        ".html": "text/html",
    }

    def end_headers(self):
        self.send_header("Cache-Control", "no-store")
        super().end_headers()

    def log_message(self, *args):  # keep test output quiet
        pass


def make_server(directory: str, port: int) -> ThreadingHTTPServer:
    handler = partial(ViewerHandler, directory=directory)
    return ThreadingHTTPServer(("127.0.0.1", port), handler)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    here = os.path.dirname(os.path.abspath(__file__))
    parser.add_argument("--port", type=int, default=8000)
    parser.add_argument("--directory", default=os.path.join(here, "web"))
    args = parser.parse_args()
    httpd = make_server(args.directory, args.port)
    host, port = httpd.server_address
    print(f"PCL viewer serving {args.directory} at http://{host}:{port}/")
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        httpd.shutdown()


if __name__ == "__main__":
    main()
