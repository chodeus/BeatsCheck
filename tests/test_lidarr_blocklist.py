"""A failed Lidarr history lookup is a blocklist failure, so the album is not deleted."""

import json
import os
import sys
import time
import types

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "app"))

import main  # noqa: E402

URL = "http://lidarr.invalid"
KEY = "not-a-key"
GRAB = {"records": [{"id": 41}]}


def _lidarr(monkeypatch, history):
    """Stub Lidarr; *history* answers the history lookup. Returns the requests made."""
    calls = []

    def fake(url, api_key, method="GET", data=None, timeout=30):
        path = url.split("/api/v1/", 1)[1]
        calls.append(f"{method} {path.split('?', 1)[0]}")
        if path.startswith("history?"):
            return history
        if path.startswith("album/"):
            return {"title": "Album", "artist": {"artistName": "Artist"}}
        if path.startswith("trackfile?"):
            return []
        return {}
    monkeypatch.setattr(main, "_lidarr_request", fake)
    return calls


def _album(tmp_path):
    """One corrupt file matched to a Lidarr track file; returns its path."""
    track = tmp_path / "music" / "Artist" / "Album" / "01.flac"
    track.parent.mkdir(parents=True)
    track.write_bytes(b"x")
    (tmp_path / "corrupt_details.json").write_text(json.dumps(
        {str(track): {"reason": "damaged", "trackfileId": 5, "albumId": 7}}))
    return str(track)


def _delete(tmp_path, track):
    return main._lidarr_delete_corrupt(
        URL, KEY, [track], str(tmp_path / "log.txt"),
        log_dir=str(tmp_path), blocklist=True, search=False)


def test_a_failed_history_lookup_counts_as_a_blocklist_failure(monkeypatch):
    _lidarr(monkeypatch, None)
    counts = main._lidarr_blocklist_albums(URL, KEY, [7])
    assert counts == (0, 1)


def test_an_album_with_no_grab_history_is_not_a_failure(monkeypatch):
    _lidarr(monkeypatch, {"records": []})
    counts = main._lidarr_blocklist_albums(URL, KEY, [7])
    assert counts == (0, 0)


def test_a_failed_history_lookup_stops_the_delete(tmp_path, monkeypatch):
    track = _album(tmp_path)
    calls = _lidarr(monkeypatch, None)
    result = _delete(tmp_path, track)
    assert result == (0, [])
    assert not [c for c in calls if c.startswith("DELETE")]


def test_a_found_grab_is_marked_failed_before_the_delete(tmp_path, monkeypatch):
    track = _album(tmp_path)
    calls = _lidarr(monkeypatch, GRAB)
    result = _delete(tmp_path, track)
    assert result == (1, [7])
    assert calls.index("POST history/failed/41") < calls.index("DELETE trackfile/bulk")


def test_the_redownload_check_waits_out_a_failed_history_lookup(tmp_path, monkeypatch):
    pending = tmp_path / "pending_redownloads.json"
    pending.write_text(json.dumps(
        {"7": {"deletedAtTs": time.time(), "albumName": "Artist — Album"}}))
    _lidarr(monkeypatch, None)
    cfg = types.SimpleNamespace(
        log_dir=str(tmp_path), log_file=str(tmp_path / "log.txt"),
        lidarr_url=URL, lidarr_api_key=KEY)
    main._poll_pending_redownloads(cfg)
    assert list(json.loads(pending.read_text())) == ["7"]
