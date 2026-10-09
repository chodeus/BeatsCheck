"""The .scanning lock: scans, deletes and ignores never overlap."""

import json
import os
import sys
import threading
import time
import types
import urllib.error
import urllib.request

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "app"))

import main  # noqa: E402
import webui  # noqa: E402


@pytest.fixture
def held(tmp_path):
    """The scan lock on tmp_path, held for the test."""
    lock = main._acquire_scan_lock(str(tmp_path))
    yield lock
    if not lock.closed:
        main._release_scan_lock(lock)


def _lock_is_free(config_dir):
    lock = main._acquire_scan_lock(str(config_dir))
    if lock is None:
        return False
    main._release_scan_lock(lock)
    return True


def test_a_held_lock_refuses_a_second_taker(tmp_path, held):
    second = main._acquire_scan_lock(str(tmp_path))
    assert held is not None
    assert second is None


def test_release_frees_the_lock_and_keeps_the_file(tmp_path, held):
    inode = os.stat(tmp_path / ".scanning").st_ino
    main._release_scan_lock(held)

    assert (tmp_path / ".scanning").exists()
    assert os.stat(tmp_path / ".scanning").st_ino == inode
    free = _lock_is_free(tmp_path)
    assert free


def test_waiting_returns_the_lock_once_freed_and_keeps_the_heartbeat(
        tmp_path, held, monkeypatch):
    monkeypatch.setattr(main, "_SCAN_LOCK_POLL_SECONDS", 0.05)
    heartbeat = tmp_path / ".heartbeat"
    threading.Timer(0.3, main._release_scan_lock, args=(held,)).start()

    got = main._wait_for_scan_lock(str(tmp_path), "waiting", str(heartbeat))

    assert got is not None
    main._release_scan_lock(got)
    assert heartbeat.exists()


def test_waiting_gives_up_on_shutdown(tmp_path, held, monkeypatch):
    monkeypatch.setattr(main, "shutdown_requested", True)
    got = main._wait_for_scan_lock(str(tmp_path), "waiting")
    assert got is None


def test_a_scan_holds_the_lock_and_releases_it(tmp_path, monkeypatch):
    seen = []
    monkeypatch.setattr(main, "_run_scan_inner",
                        lambda *a, **k: seen.append(_lock_is_free(tmp_path)))

    main.run_scan(str(tmp_path), str(tmp_path), str(tmp_path / "log"),
                  str(tmp_path), "report", 1)

    assert seen == [False]
    free = _lock_is_free(tmp_path)
    assert free
    assert (tmp_path / ".scanning").exists()


def test_a_scan_waits_for_a_delete_and_stops_on_shutdown(
        tmp_path, held, monkeypatch):
    ran = []
    monkeypatch.setattr(main, "_run_scan_inner", lambda *a, **k: ran.append(1))
    monkeypatch.setattr(main, "shutdown_requested", True)

    main.run_scan(str(tmp_path), str(tmp_path), str(tmp_path / "log"),
                  str(tmp_path), "report", 1)

    assert ran == []


def test_interactive_delete_holds_the_lock_throughout(tmp_path, monkeypatch):
    seen = []
    monkeypatch.setattr(main, "_run_delete_mode_locked",
                        lambda *a: seen.append(_lock_is_free(tmp_path)))

    main.run_delete_mode(str(tmp_path / "corrupt.txt"), str(tmp_path / "log"),
                         str(tmp_path), input_folder=str(tmp_path))

    assert seen == [False]
    free = _lock_is_free(tmp_path)
    assert free


def test_auto_delete_holds_the_lock(tmp_path, monkeypatch):
    seen = []
    monkeypatch.setattr(main, "run_auto_delete",
                        lambda *a: seen.append(_lock_is_free(tmp_path)))
    cfg = types.SimpleNamespace(
        log_dir=str(tmp_path), log_file=str(tmp_path / "log"),
        delete_after=7, max_auto_delete=50, lidarr_url=None,
        lidarr_api_key=None, lidarr_search=False, lidarr_blocklist=False)

    main._auto_delete_after_scan(cfg)

    assert seen == [False]
    free = _lock_is_free(tmp_path)
    assert free


def test_the_redownload_check_holds_the_lock(tmp_path, monkeypatch):
    (tmp_path / "pending_redownloads.json").write_text(json.dumps(
        {"7": {"deletedAtTs": time.time(), "albumName": "Artist — Album"}}))
    seen = []
    monkeypatch.setattr(main, "_lidarr_get_album_history",
                        lambda *a: seen.append(_lock_is_free(tmp_path)) or [])
    cfg = types.SimpleNamespace(
        log_dir=str(tmp_path), log_file=str(tmp_path / "log"),
        lidarr_url="http://lidarr.invalid", lidarr_api_key="not-a-key")

    main._poll_pending_redownloads(cfg)

    free = _lock_is_free(tmp_path)
    assert seen == [False]
    assert free


@pytest.mark.parametrize("trigger, mode, fresh", [
    ("fresh:report", "report", True),
    ("fresh:", "report", True),
    ("move", "move", False),
    ("", "report", False),
])
def test_setup_idle_keeps_the_fresh_request(monkeypatch, trigger, mode, fresh):
    monkeypatch.setattr(main, "_idle_wait", lambda *a: trigger)
    cfg = types.SimpleNamespace(log_dir="/x", lidarr_url=None,
                                lidarr_api_key=None, mode="setup",
                                fresh_rescan=False)

    got = main._run_setup_idle(cfg)

    assert got == mode
    assert cfg.fresh_rescan is fresh


# --- WebUI -----------------------------------------------------------------

def _post(base, path, body):
    req = urllib.request.Request(
        base + path, data=json.dumps(body).encode(),
        headers={"Content-Type": "application/json"}, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            return resp.status, json.loads(resp.read())
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read() or b"{}")


@pytest.fixture
def open_webui(webui_server, tmp_path):
    """A WebUI with the login off, so no session is needed."""
    webui._save_auth_off(str(tmp_path))
    return webui_server


@pytest.mark.parametrize("path, body", [
    ("/api/ignore", {"files": ["/data/a.flac"]}),
    ("/api/delete-files", {"files": ["/data/a.flac"]}),
    ("/api/delete-albums", {"folders": ["/data/Album"]}),
], ids=["ignore", "delete-files", "delete-albums"])
def test_a_held_lock_gets_409(open_webui, tmp_path, held, path, body):
    (tmp_path / "corrupt.txt").write_text("/data/a.flac\n")

    status, _ = _post(open_webui, path, body)

    assert status == 409
    assert (tmp_path / "corrupt.txt").read_text() == "/data/a.flac\n"


def test_ignore_releases_the_lock(open_webui, tmp_path):
    (tmp_path / "corrupt.txt").write_text("/data/a.flac\n/data/b.flac\n")

    status, _ = _post(open_webui, "/api/ignore", {"files": ["/data/a.flac"]})

    assert status == 200
    assert (tmp_path / "corrupt.txt").read_text() == "/data/b.flac\n"
    free = _lock_is_free(tmp_path)
    assert free


def test_a_delete_job_holds_the_lock_until_it_ends(open_webui, tmp_path,
                                                   monkeypatch):
    release = threading.Event()
    seen = []

    def slow_job(job_id, *args):
        seen.append(_lock_is_free(tmp_path))
        release.wait(5)
        webui._update_delete_job(job_id, finished=True, phase="done")

    monkeypatch.setattr(webui, "_run_delete_files_job", slow_job)

    status, reply = _post(open_webui, "/api/delete-files",
                          {"files": ["/data/a.flac"]})
    again, _ = _post(open_webui, "/api/delete-files",
                     {"files": ["/data/a.flac"]})
    release.set()
    deadline = time.time() + 5
    while not _lock_is_free(tmp_path) and time.time() < deadline:
        time.sleep(0.05)

    assert status == 202
    assert again == 409
    assert seen == [False]
    free = _lock_is_free(tmp_path)
    assert free


def test_the_old_delete_endpoint_is_gone(open_webui):
    status, _ = _post(open_webui, "/api/delete", {"files": ["/data/a.flac"]})
    assert status == 404
