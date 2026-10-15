"""A folder delete sends files Lidarr tracks through Lidarr even when Lidarr doesn't track the flagged file."""

import builtins
import json
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "app"))

import main  # noqa: E402
import pytest  # noqa: E402

URL = "http://lidarr.invalid"
KEY = "not-a-key"
SIBLING = "/music/Artist/Album/02.flac"


def _lidarr(monkeypatch, tracks=(SIBLING,), fail_second=None, twin=False):
    """Stub Lidarr; request 2 to *fail_second* fails, *twin* adds a same-named artist in a 2nd root. Returns deleted IDs."""
    deleted, seen = [], []

    def fake(url, api_key, method="GET", data=None, timeout=30):
        path = url.split("/api/v1/", 1)[1]
        seen.append(path)
        if path == fail_second and seen.count(path) == 2:
            return None
        if path == "artist":
            twins = [{"id": 3, "path": "/music2/Artist"}] if twin else []
            return [{"id": 1, "path": "/music/Artist"}, {"id": 2, "path": "/music/Other"}] + twins
        if path == "trackfile?artistId=3":
            return [{"id": 10, "path": "/music2/Artist/Album/02.flac", "albumId": 8}]
        if path == "trackfile?artistId=1":
            return [{"id": 9, "path": t, "albumId": 7} for t in tracks]
        if path.startswith("trackfile?"):
            return []
        if path == "trackfile/bulk":
            deleted.extend(data["trackFileIds"])
            return {}
        if path.startswith("album/"):
            return {"title": "Album", "artist": {"artistName": "Artist"}}
        if path == "command":
            return []
        return {}
    monkeypatch.setattr(main, "_lidarr_request", fake)
    monkeypatch.setattr(main, "_log_delete_and_search", lambda *a, **k: None)
    monkeypatch.setattr(main, "_lidarr_wait_for_search", lambda *a, **k: "")
    return deleted


def _folder(tmp_path):
    """A flagged file Lidarr doesn't track beside a healthy one; returns (config dir, folder)."""
    folder = tmp_path / "data" / "Artist" / "Album"
    folder.mkdir(parents=True)
    (folder / "01.flac").write_bytes(b"x")
    (folder / "02.flac").write_bytes(b"x")
    cfg = tmp_path / "config"
    cfg.mkdir()
    (cfg / "corrupt.txt").write_text(f"{folder / '01.flac'}\n")
    (cfg / "corrupt_details.json").write_text(json.dumps({str(folder / "01.flac"): {"reason": "damaged"}}))
    return cfg, folder


def _webui_delete(tmp_path, monkeypatch, cfg, folder):
    monkeypatch.setenv("LIDARR_URL", URL)
    monkeypatch.delenv("LIDARR_BLOCKLIST", raising=False)
    monkeypatch.setattr(main, "_load_lidarr_api_key", lambda: KEY)
    return main.delete_album_folders([str(folder)], str(cfg), music_dir=str(tmp_path / "data"), mode="whole")


def test_webui_folder_delete_sends_a_tracked_file_through_lidarr(tmp_path, monkeypatch):
    cfg, folder = _folder(tmp_path)
    deleted = _lidarr(monkeypatch)
    result = _webui_delete(tmp_path, monkeypatch, cfg, folder)
    assert deleted == [9]
    assert 7 in result["albums"]
    assert not folder.exists()


def test_interactive_folder_delete_sends_a_tracked_file_through_lidarr(tmp_path, monkeypatch):
    cfg, folder = _folder(tmp_path)
    deleted = _lidarr(monkeypatch)
    answers = iter(["i", "y"])
    monkeypatch.setattr(builtins, "input", lambda *a: next(answers))
    main._run_delete_mode_locked(str(cfg / "corrupt.txt"), str(cfg / "log.txt"), str(cfg),
                                 str(tmp_path / "data"), URL, KEY, False)
    assert deleted == [9]


def test_interactive_folder_delete_asks_lidarr_with_no_saved_details(tmp_path, monkeypatch):
    cfg, folder = _folder(tmp_path)
    (cfg / "corrupt_details.json").write_text("{}")
    deleted = _lidarr(monkeypatch)
    answers = iter(["i", "y"])
    monkeypatch.setattr(builtins, "input", lambda *a: next(answers))
    main._run_delete_mode_locked(str(cfg / "corrupt.txt"), str(cfg / "log.txt"), str(cfg),
                                 str(tmp_path / "data"), URL, KEY, False)
    assert deleted == [9]


def test_a_same_named_track_in_another_album_is_not_matched(tmp_path, monkeypatch):
    cfg, folder = _folder(tmp_path)
    deleted = _lidarr(monkeypatch, tracks=("/music/Artist/Other Album/02.flac",))
    _webui_delete(tmp_path, monkeypatch, cfg, folder)
    assert deleted == []
    assert not folder.exists()


def test_a_file_two_artists_match_is_not_matched(tmp_path, monkeypatch):
    cfg, folder = _folder(tmp_path)
    deleted = _lidarr(monkeypatch, twin=True)
    _webui_delete(tmp_path, monkeypatch, cfg, folder)
    assert deleted == []


@pytest.mark.parametrize("failing", ["artist", "trackfile?artistId=1"])
def test_folder_delete_keeps_the_folder_when_the_artist_lookup_fails(tmp_path, monkeypatch, failing):
    cfg, folder = _folder(tmp_path)
    deleted = _lidarr(monkeypatch, fail_second=failing)
    result = _webui_delete(tmp_path, monkeypatch, cfg, folder)
    assert deleted == []
    assert (folder / "01.flac").exists() and (folder / "02.flac").exists()
    assert result["errors"]
