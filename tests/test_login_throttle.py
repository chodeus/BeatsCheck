"""Login lockout: the client key, counting before verification, and the cap."""

import json
import os
import sys
import threading
import urllib.error
import urllib.request

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "app"))

import webui  # noqa: E402

PROXY_NET = "172.18.0.0/16"


@pytest.fixture(autouse=True)
def fresh_attempts():
    with webui._login_attempts_lock:
        webui._login_attempts.clear()
    yield
    with webui._login_attempts_lock:
        webui._login_attempts.clear()


@pytest.mark.parametrize("peer, xff, trusted, expected", [
    # No trusted proxies: the header is ignored.
    ("10.0.0.5", "203.0.113.9", "", "10.0.0.5"),
    # Peer is not a trusted proxy: a forged header is ignored.
    ("10.0.0.5", "203.0.113.9", PROXY_NET, "10.0.0.5"),
    # Trusted proxy that appended the real client after a forged value.
    ("172.18.0.2", "6.6.6.6, 203.0.113.9", PROXY_NET, "203.0.113.9"),
    # Chained trusted proxies are skipped from the right.
    ("172.18.0.2", "203.0.113.9, 172.18.0.7", PROXY_NET, "203.0.113.9"),
    # A garbage hop where the client should be falls back to the peer.
    ("172.18.0.2", "203.0.113.9, not-an-ip", PROXY_NET, "172.18.0.2"),
    # Trusted proxy with no header, or only trusted hops.
    ("172.18.0.2", "", PROXY_NET, "172.18.0.2"),
    ("172.18.0.2", "172.18.0.7", PROXY_NET, "172.18.0.2"),
    # IPv4-mapped IPv6 peer on a dual-stack socket.
    ("::ffff:172.18.0.2", "203.0.113.9", PROXY_NET, "203.0.113.9"),
    # A bad entry is skipped; the good one still applies.
    ("172.18.0.2", "203.0.113.9", "bogus, 172.18.0.2", "203.0.113.9"),
])
def test_resolve_client_ip(peer, xff, trusted, expected):
    assert webui._resolve_client_ip(peer, xff, trusted) == expected


def test_lockout_after_max_attempts():
    allowed = [webui._login_begin_attempt("k") for _ in range(
        webui._LOGIN_MAX_ATTEMPTS)]
    refused = webui._login_begin_attempt("k")

    assert allowed == [0] * webui._LOGIN_MAX_ATTEMPTS
    assert refused > 0


def test_success_clears_the_count():
    for _ in range(webui._LOGIN_MAX_ATTEMPTS - 1):
        webui._login_begin_attempt("k")
    webui._login_record_success("k")

    allowed = [webui._login_begin_attempt("k") for _ in range(
        webui._LOGIN_MAX_ATTEMPTS)]
    assert allowed == [0] * webui._LOGIN_MAX_ATTEMPTS


def test_parallel_burst_gets_only_max_attempts():
    results = []
    barrier = threading.Barrier(20)

    def attempt():
        barrier.wait()
        results.append(webui._login_begin_attempt("k"))

    threads = [threading.Thread(target=attempt) for _ in range(20)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert results.count(0) == webui._LOGIN_MAX_ATTEMPTS


def test_tracked_clients_are_pruned_at_the_cap(monkeypatch):
    monkeypatch.setattr(webui, "_LOGIN_MAX_TRACKED", 3)
    with webui._login_attempts_lock:
        for i in range(3):
            webui._login_attempts[f"old{i}"] = {
                "count": 1, "first": 0, "locked_until": 0}

    webui._login_begin_attempt("new")

    assert set(webui._login_attempts) == {"new"}


class _TestServer(webui.ThreadedHTTPServer):
    # The default listen backlog of 5 resets a 12-connection burst on macOS.
    request_queue_size = 64


@pytest.fixture
def server(tmp_path):
    webui._save_auth(str(tmp_path), "admin", "right-password")
    srv = _TestServer(
        ("127.0.0.1", 0), webui.WebUIHandler, str(tmp_path), str(tmp_path))
    thread = threading.Thread(target=srv.serve_forever, daemon=True)
    thread.start()
    yield f"http://127.0.0.1:{srv.server_address[1]}"
    srv.shutdown()
    srv.server_close()


def _login(base, password, xff=None):
    headers = {"Content-Type": "application/json"}
    if xff:
        headers["X-Forwarded-For"] = xff
    req = urllib.request.Request(
        base + "/api/login",
        data=json.dumps({"username": "admin", "password": password}).encode(),
        headers=headers, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            return resp.status
    except urllib.error.HTTPError as e:
        return e.code


def test_forged_forwarded_for_cannot_dodge_the_lockout(server, monkeypatch):
    monkeypatch.delenv("WEBUI_TRUSTED_PROXIES", raising=False)
    codes = [_login(server, "wrong", xff=f"198.51.100.{i}") for i in range(7)]

    assert codes[:5] == [401] * 5
    assert codes[5:] == [429, 429]
    assert _login(server, "right-password") == 429


def test_trusted_proxy_gives_each_client_its_own_lockout(server, monkeypatch):
    monkeypatch.setenv("WEBUI_TRUSTED_PROXIES", "127.0.0.1/32")
    for _ in range(5):
        _login(server, "wrong", xff="198.51.100.1")

    assert _login(server, "wrong", xff="198.51.100.1") == 429
    assert _login(server, "right-password", xff="198.51.100.2") == 200


def test_parallel_wrong_logins_lock_after_max_attempts(server, monkeypatch):
    monkeypatch.delenv("WEBUI_TRUSTED_PROXIES", raising=False)
    codes = []
    barrier = threading.Barrier(12)

    def attempt():
        barrier.wait()
        codes.append(_login(server, "wrong"))

    threads = [threading.Thread(target=attempt) for _ in range(12)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert codes.count(401) == webui._LOGIN_MAX_ATTEMPTS
    assert codes.count(429) == 12 - webui._LOGIN_MAX_ATTEMPTS
