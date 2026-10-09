"""Shared fixture: a live WebUI server on a temporary config dir."""

import os
import sys
import threading

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "app"))

import webui  # noqa: E402


class _TestServer(webui.ThreadedHTTPServer):
    # The default listen backlog of 5 resets a 12-connection burst on macOS.
    request_queue_size = 64


@pytest.fixture
def webui_server(tmp_path):
    """Base URL of a running WebUI whose config dir is tmp_path."""
    srv = _TestServer(
        ("127.0.0.1", 0), webui.WebUIHandler, str(tmp_path), str(tmp_path))
    thread = threading.Thread(
        target=srv.serve_forever, kwargs={"poll_interval": 0.05}, daemon=True)
    thread.start()
    yield f"http://127.0.0.1:{srv.server_address[1]}"
    srv.shutdown()
    srv.server_close()
