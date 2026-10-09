"""Request input: body field types on the POST endpoints, and stalled clients."""

import json
import os
import socket
import sys
import urllib.error
import urllib.request

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "app"))

import webui  # noqa: E402

PASSWORD = "right-password"
CONF = "lidarr_url = \"\"\n"


@pytest.fixture
def session(webui_server, tmp_path):
    """Base URL and session cookie of a WebUI with a login and a config file."""
    env = dict(os.environ)  # /api/config copies saved values into os.environ
    (tmp_path / "beatscheck.conf").write_text(CONF)
    webui._save_auth(str(tmp_path), "admin", PASSWORD)
    req = urllib.request.Request(
        webui_server + "/api/login",
        data=json.dumps({"username": "admin", "password": PASSWORD}).encode(),
        headers={"Content-Type": "application/json"}, method="POST")
    with urllib.request.urlopen(req, timeout=10) as resp:
        cookie = resp.headers["Set-Cookie"].split(";")[0]
    yield webui_server, cookie
    os.environ.clear()
    os.environ.update(env)
    webui._sessions.clear()
    with webui._login_attempts_lock:
        webui._login_attempts.clear()


def _post(session, path, body, raw=None):
    base, cookie = session
    data = raw if raw is not None else json.dumps(body).encode()
    req = urllib.request.Request(
        base + path, data=data,
        headers={"Content-Type": "application/json", "Cookie": cookie},
        method="POST")
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            return resp.status
    except urllib.error.HTTPError as e:
        return e.code


@pytest.mark.parametrize("path, body", [
    ("/api/ignore", {"files": "/data/a.flac"}),
    ("/api/ignore", {"files": [1, 2]}),
    ("/api/ignore", {"files": []}),
    ("/api/delete", {"files": "/data/a.flac"}),
    ("/api/delete", {"files": [None]}),
    ("/api/delete-files", {"files": [{"path": "/data/a.flac"}]}),
    ("/api/delete-albums", {"folders": [["/data/Album"]]}),
    ("/api/config", {"config": ["lidarr_url"]}),
    ("/api/config", {"config": "lidarr_url"}),
    ("/api/config", {"config": {}}),
    ("/api/config", {"config": {"lidarr_url": ["http://x"]}}),
    ("/api/config", {"config": {"lidarr_url": None}}),
    ("/api/rescan", {"mode": "report", "fresh": "false"}),
], ids=["ignore-string", "ignore-ints", "ignore-empty", "delete-string",
        "delete-null-item", "delete-files-object-item",
        "delete-albums-list-item", "config-list", "config-string",
        "config-empty", "config-list-value", "config-null-value",
        "rescan-fresh-string"])
def test_a_field_of_the_wrong_type_gets_400(session, path, body):
    status = _post(session, path, body)
    assert status == 400


@pytest.mark.parametrize("path, key", [
    ("/api/ignore", "files"), ("/api/delete", "files"),
    ("/api/delete-files", "files"), ("/api/delete-albums", "folders")])
@pytest.mark.parametrize("paths", [
    [""], ["   "], ["/data/a\x00b.flac"], ["/data/a.flac", ""],
], ids=["empty", "blank", "nul", "one-blank-of-two"])
def test_a_blank_or_nul_path_gets_400(session, path, key, paths):
    status = _post(session, path, {key: paths})
    assert status == 400


@pytest.mark.parametrize("path", [
    "/api/login", "/api/ignore", "/api/delete", "/api/delete-files",
    "/api/delete-albums", "/api/config", "/api/rescan"])
@pytest.mark.parametrize("body", [None, [], ["files"], "text", 1],
                         ids=["null", "empty-list", "list", "string", "number"])
def test_a_body_that_is_not_an_object_gets_400(session, path, body):
    status = _post(session, path, body)
    assert status == 400


def test_a_config_value_with_a_line_break_is_refused(session, tmp_path):
    status = _post(session, "/api/config", {"config": {
        "lidarr_url": "http://lidarr\nlidarr_api_key = \"planted\""}})

    assert status == 400
    assert (tmp_path / "beatscheck.conf").read_text() == CONF


@pytest.mark.parametrize("raw", [
    b'{"config": {"workers": 1e999}}',
    b'{"config": {"workers": NaN}}',
    b'{"config": {"workers": -Infinity}}',
], ids=["overflow", "nan", "minus-infinity"])
def test_a_config_number_that_is_not_finite_is_refused(session, tmp_path, raw):
    status = _post(session, "/api/config", None, raw=raw)

    assert status == 400
    assert (tmp_path / "beatscheck.conf").read_text() == CONF


def test_valid_config_values_are_still_written(session, tmp_path):
    status = _post(session, "/api/config", {"config": {
        "lidarr_url": "http://lidarr:8686", "workers": 4}})

    conf = (tmp_path / "beatscheck.conf").read_text()
    assert status == 200
    assert 'lidarr_url = "http://lidarr:8686"' in conf
    assert "workers = 4" in conf


def test_ignore_takes_a_list_of_paths(session, tmp_path):
    (tmp_path / "corrupt.txt").write_text("/data/a.flac\n/data/b.flac\n")

    status = _post(session, "/api/ignore", {"files": ["/data/a.flac"]})

    assert status == 200
    assert (tmp_path / "corrupt.txt").read_text() == "/data/b.flac\n"


def test_the_handler_has_a_socket_timeout():
    timeout = webui.WebUIHandler.timeout
    assert isinstance(timeout, (int, float))
    assert 0 < timeout <= 60


def test_a_stalled_body_frees_the_connection(webui_server, monkeypatch):
    monkeypatch.setattr(webui.WebUIHandler, "timeout", 0.5)
    port = int(webui_server.rsplit(":", 1)[1])
    with socket.create_connection(("127.0.0.1", port), timeout=5) as sock:
        # Promises 100 bytes, sends 1.
        sock.sendall(b"POST /api/setup HTTP/1.1\r\nHost: x\r\n"
                     b"Content-Type: application/json\r\n"
                     b"Content-Length: 100\r\n\r\n{")
        reply = sock.recv(1024)
    assert reply == b""
