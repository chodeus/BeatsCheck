"""Login state: the "off" state, turning it off, sessions, and request bodies."""

import json
import os
import sys
import threading
import time
import urllib.error
import urllib.request

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "app"))

import webui  # noqa: E402

PASSWORD = "right-password"


@pytest.fixture(autouse=True)
def fresh_state():
    webui._clear_sessions()
    with webui._login_attempts_lock:
        webui._login_attempts.clear()
    yield
    webui._clear_sessions()
    with webui._login_attempts_lock:
        webui._login_attempts.clear()


def _call(base, path, body=None, cookie=None, raw=None,
          content_type="application/json"):
    """POST *body* or *raw* bytes (GET when both are None). Returns
    (status, json, cookie pair)."""
    headers = {}
    if content_type:
        headers["Content-Type"] = content_type
    if cookie:
        headers["Cookie"] = cookie
    data = raw if raw is not None else (
        None if body is None else json.dumps(body).encode())
    req = urllib.request.Request(
        base + path, data=data, headers=headers,
        method="GET" if data is None else "POST")
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            status, raw_reply, got = resp.status, resp.read(), resp.headers
    except urllib.error.HTTPError as e:
        status, raw_reply, got = e.code, e.read(), e.headers
    set_cookie = got.get("Set-Cookie") or ""
    return status, json.loads(raw_reply or b"{}"), set_cookie.split(";")[0]


def _status(base, path, body=None, cookie=None):
    return _call(base, path, body, cookie)[0]


def _log_in(base):
    status, _, cookie = _call(
        base, "/api/login", {"username": "admin", "password": PASSWORD})
    assert status == 200
    return cookie


def _auth_file(config_dir):
    return webui._load_auth(str(config_dir))


def test_load_auth_reads_the_off_marker(tmp_path):
    (tmp_path / "webui_auth.json").write_text(json.dumps({"login": "off"}))
    assert _auth_file(tmp_path) is webui._AUTH_OFF


@pytest.mark.parametrize("content", [
    {"login": "on"},
    {"login": False},
    {"login": "off", "username": "admin"},
    ["login", "off"],
    "off",
])
def test_anything_but_the_exact_marker_stays_closed(tmp_path, content):
    (tmp_path / "webui_auth.json").write_text(json.dumps(content))
    assert _auth_file(tmp_path) is webui._AUTH_UNREADABLE


def test_first_run_can_choose_no_login(webui_server, tmp_path):
    status, _, _ = _call(webui_server, "/api/setup", {"login": False})
    assert status == 200

    api = _status(webui_server, "/api/status")
    _, state, _ = _call(webui_server, "/api/auth-status")
    assert _auth_file(tmp_path) is webui._AUTH_OFF
    assert api == 200
    assert state == {"setup_required": False, "login_required": False,
                     "authenticated": True}


def test_no_login_is_refused_once_a_login_exists(webui_server, tmp_path):
    webui._save_auth(str(tmp_path), "admin", PASSWORD)

    status = _status(webui_server, "/api/setup", {"login": False})
    api = _status(webui_server, "/api/status")

    assert status == 409
    assert isinstance(_auth_file(tmp_path), dict)
    assert api == 401


def test_choosing_no_login_twice_is_a_conflict(webui_server, tmp_path):
    webui._save_auth_off(str(tmp_path))
    status = _status(webui_server, "/api/setup", {"login": False})
    assert status == 409


def test_logging_in_while_the_login_is_off(webui_server, tmp_path):
    webui._save_auth_off(str(tmp_path))
    status = _status(
        webui_server, "/api/login", {"username": "admin", "password": "x"})
    assert status == 409


def test_an_unreadable_auth_file_keeps_the_api_closed(webui_server, tmp_path):
    (tmp_path / "webui_auth.json").write_text("{ not json")

    api = _status(webui_server, "/api/status")
    setup = _status(webui_server, "/api/setup", {"login": False})

    assert api == 401
    assert setup == 503


def test_turning_off_needs_a_session(webui_server, tmp_path):
    webui._save_auth(str(tmp_path), "admin", PASSWORD)

    status = _status(webui_server, "/api/auth/disable", {"password": PASSWORD})

    assert status == 401
    assert isinstance(_auth_file(tmp_path), dict)


def test_turning_off_rejects_a_wrong_password(webui_server, tmp_path):
    webui._save_auth(str(tmp_path), "admin", PASSWORD)
    cookie = _log_in(webui_server)

    status = _status(
        webui_server, "/api/auth/disable", {"password": "wrong"}, cookie)
    api = _status(webui_server, "/api/status", cookie=cookie)

    assert status == 403
    assert isinstance(_auth_file(tmp_path), dict)
    assert api == 200


def test_turning_off_counts_attempts_before_the_password(webui_server, tmp_path):
    webui._save_auth(str(tmp_path), "admin", PASSWORD)
    cookie = _log_in(webui_server)
    codes = [_status(webui_server, "/api/auth/disable", {"password": "wrong"},
                     cookie)
             for _ in range(webui._LOGIN_MAX_ATTEMPTS)]

    status = _status(
        webui_server, "/api/auth/disable", {"password": PASSWORD}, cookie)

    assert codes == [403] * webui._LOGIN_MAX_ATTEMPTS
    assert status == 429
    assert isinstance(_auth_file(tmp_path), dict)


def test_parallel_wrong_passwords_lock_turning_off(webui_server, tmp_path):
    webui._save_auth(str(tmp_path), "admin", PASSWORD)
    cookie = _log_in(webui_server)
    codes = []
    barrier = threading.Barrier(12)

    def attempt():
        barrier.wait()
        codes.append(_status(webui_server, "/api/auth/disable",
                             {"password": "wrong"}, cookie))

    threads = [threading.Thread(target=attempt) for _ in range(12)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert codes.count(403) == webui._LOGIN_MAX_ATTEMPTS
    assert codes.count(429) == 12 - webui._LOGIN_MAX_ATTEMPTS


def test_turning_off_opens_the_api_and_ends_sessions(webui_server, tmp_path):
    webui._save_auth(str(tmp_path), "admin", PASSWORD)
    cookie = _log_in(webui_server)

    status, _, cleared = _call(
        webui_server, "/api/auth/disable", {"password": PASSWORD}, cookie)
    api = _status(webui_server, "/api/status")

    assert status == 200
    assert cleared == f"{webui._SESSION_COOKIE}="
    assert _auth_file(tmp_path) is webui._AUTH_OFF
    assert webui._sessions == {}
    assert api == 200


def test_an_old_session_dies_after_off_then_on(webui_server, tmp_path):
    webui._save_auth(str(tmp_path), "admin", PASSWORD)
    old = _log_in(webui_server)
    off = _status(webui_server, "/api/auth/disable", {"password": PASSWORD}, old)
    assert off == 200

    status, _, new = _call(webui_server, "/api/setup",
                           {"username": "admin", "password": "a-new-password"})
    old_api = _status(webui_server, "/api/status", cookie=old)
    new_api = _status(webui_server, "/api/status", cookie=new)

    assert status == 200
    assert old_api == 401
    assert new_api == 200


def test_resetting_the_login_ends_existing_sessions(webui_server, tmp_path):
    webui._save_auth(str(tmp_path), "admin", PASSWORD)
    old = _log_in(webui_server)
    # What reset-webui-password does, from another process.
    (tmp_path / "webui_auth.json").unlink()

    after_reset = _status(webui_server, "/api/status", cookie=old)
    status, _, new = _call(webui_server, "/api/setup",
                           {"username": "admin", "password": "a-new-password"})
    old_api = _status(webui_server, "/api/status", cookie=old)
    new_api = _status(webui_server, "/api/status", cookie=new)

    assert after_reset == 401
    assert status == 200
    assert old_api == 401
    assert new_api == 200


def test_an_unreadable_auth_file_refuses_existing_sessions(
        webui_server, tmp_path):
    webui._save_auth(str(tmp_path), "admin", PASSWORD)
    cookie = _log_in(webui_server)
    (tmp_path / "webui_auth.json").write_text("{ not json")

    api = _status(webui_server, "/api/status", cookie=cookie)
    status, state, _ = _call(webui_server, "/api/auth-status", cookie=cookie)

    assert api == 401
    assert status == 503
    assert state["authenticated"] is False


def test_status_reports_whether_a_login_is_required(webui_server, tmp_path):
    webui._save_auth(str(tmp_path), "admin", PASSWORD)
    cookie = _log_in(webui_server)
    on = _call(webui_server, "/api/status", cookie=cookie)[1]

    webui._save_auth_off(str(tmp_path))
    off = _call(webui_server, "/api/status")[1]

    assert on["login_required"] is True
    assert off["login_required"] is False


def test_concurrent_setups_leave_one_login(webui_server, tmp_path, monkeypatch):
    webui._save_auth_off(str(tmp_path))
    write = webui._write_auth_file

    def slow_write(config_dir, data):
        time.sleep(0.3)
        write(config_dir, data)

    monkeypatch.setattr(webui, "_write_auth_file", slow_write)
    results = []
    barrier = threading.Barrier(2)

    def setup(name):
        barrier.wait()
        try:
            results.append(_call(webui_server, "/api/setup",
                                 {"username": name, "password": PASSWORD}))
        except OSError as e:
            results.append((repr(e), None, None))

    threads = [threading.Thread(target=setup, args=(n,))
               for n in ("first", "second")]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    statuses = sorted(r[0] for r in results)
    winner = next(r[2] for r in results if r[0] == 200)
    api = _status(webui_server, "/api/status", cookie=winner)
    assert statuses == [200, 409]
    assert isinstance(_auth_file(tmp_path), dict)
    assert api == 200


@pytest.mark.parametrize("content_type", [
    "text/plain", "application/x-www-form-urlencoded",
    "multipart/form-data; boundary=x", None,
])
def test_a_post_that_is_not_json_is_refused(webui_server, tmp_path,
                                            content_type):
    webui._save_auth_off(str(tmp_path))
    body = json.dumps({"username": "someone", "password": PASSWORD}).encode()

    status = _call(webui_server, "/api/setup", raw=body,
                   content_type=content_type)[0]

    assert status == 415
    assert _auth_file(tmp_path) is webui._AUTH_OFF


def test_json_with_a_charset_is_accepted(webui_server, tmp_path):
    status = _call(webui_server, "/api/setup", raw=b'{"login": false}',
                   content_type="application/json; charset=utf-8")[0]
    assert status == 200


def _stored_login(base, config_dir):
    webui._save_auth(config_dir, "admin", PASSWORD)


def _session(base, config_dir):
    webui._save_auth(config_dir, "admin", PASSWORD)
    return _log_in(base)


# Each path in a state where it reads the body: first run, a stored login,
# and login off (so /api/config needs no session).
@pytest.mark.parametrize("path, prepare", [
    ("/api/setup", lambda base, d: None),
    ("/api/login", _stored_login),
    ("/api/config", lambda base, d: webui._save_auth_off(d)),
], ids=["setup", "login", "config"])
@pytest.mark.parametrize("body", [[], "off", 1])
def test_a_json_body_that_is_not_an_object_gets_400(
        webui_server, tmp_path, path, prepare, body):
    prepare(webui_server, str(tmp_path))
    status = _status(webui_server, path, body)
    assert status == 400


@pytest.mark.parametrize("path, prepare, body", [
    ("/api/setup", lambda base, d: None,
     {"username": 123, "password": PASSWORD}),
    ("/api/setup", lambda base, d: None,
     {"username": "admin", "password": 12345678}),
    ("/api/setup", lambda base, d: None,
     {"username": "admin", "password": ["x"] * 8}),
    ("/api/setup", lambda base, d: None,
     {"username": "admin\n2026-10-09 | ERROR | CORRUPT: /data/x.flac",
      "password": PASSWORD}),
    ("/api/setup", lambda base, d: None,
     {"username": "a" * (webui._MAX_USERNAME_LENGTH + 1),
      "password": PASSWORD}),
    ("/api/setup", lambda base, d: None,
     {"username": "admin",
      "password": "x" * (webui._MAX_PASSWORD_LENGTH + 1)}),
    ("/api/setup", lambda base, d: None,
     {"username": "admin", "password": " " * 8}),
    ("/api/login", _stored_login, {"username": 123, "password": PASSWORD}),
    ("/api/login", _stored_login, {"username": "admin", "password": True}),
    ("/api/auth/disable", _session, {"password": 12345678}),
    ("/api/auth/disable", _session, {"password": {}}),
], ids=["setup-username-int", "setup-password-int", "setup-password-list",
        "setup-username-newline", "setup-username-long",
        "setup-password-long", "setup-password-spaces", "login-username-int",
        "login-password-bool", "disable-password-int",
        "disable-password-object"])
def test_a_field_that_is_not_usable_text_gets_400(
        webui_server, tmp_path, path, prepare, body):
    cookie = prepare(webui_server, str(tmp_path))
    status = _status(webui_server, path, body, cookie)
    assert status == 400


def test_deeply_nested_json_gets_400(webui_server):
    status = _call(webui_server, "/api/setup", raw=b"[" * 100_000)[0]
    assert status == 400


@pytest.mark.parametrize("login", ["false", 0, None])
def test_the_login_choice_must_be_a_boolean(webui_server, tmp_path, login):
    status = _status(webui_server, "/api/setup", {
        "login": login, "username": "admin", "password": PASSWORD})

    assert status == 400
    assert _auth_file(tmp_path) is None


@pytest.mark.parametrize("path, prepare, field", [
    ("/api/setup", lambda base, d: None, "password"),
    ("/api/login", _stored_login, "password"),
    ("/api/auth/disable", _session, "password"),
    ("/api/ignore", lambda base, d: webui._save_auth_off(d), "files"),
], ids=["setup", "login", "disable", "ignore"])
def test_a_lone_surrogate_gets_400(webui_server, tmp_path, path, prepare,
                                   field):
    cookie = prepare(webui_server, str(tmp_path))
    value = b'"\\ud800-password"'
    if field == "files":
        value = b"[" + value + b"]"
    raw = b'{"username": "admin", "' + field.encode() + b'": ' + value + b"}"

    status = _call(webui_server, path, cookie=cookie, raw=raw)[0]

    assert status == 400
