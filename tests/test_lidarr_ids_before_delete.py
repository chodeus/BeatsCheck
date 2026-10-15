"""A file with no Lidarr ID is looked up again before a delete, and kept if Lidarr can't be read."""

import builtins
import json
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "app"))

import main  # noqa: E402

URL = "http://lidarr.invalid"
KEY = "not-a-key"
LIDARR_TRACK = "/music/Artist/Album/01.flac"


def _lidarr(monkeypatch, up=True, tracks=(LIDARR_TRACK,), album_up=True):
    """Stub Lidarr: *up* False fails every request, *album_up* False only per-album ones. Returns the requests made."""
    calls = []

    def fake(url, api_key, method="GET", data=None, timeout=30):
        path = url.split("/api/v1/", 1)[1]
        calls.append(f"{method} {path.split('?', 1)[0]}")
        if not up or (not album_up and path.startswith("trackfile?albumId")):
            return None
        if path == "artist":
            return [{"id": 1, "path": "/music/Artist"}]
        if path.startswith("trackfile?"):
            return [{"id": 5, "path": t, "albumId": 7} for t in tracks]
        if path.startswith("album/"):
            return {"title": "Album", "artist": {"artistName": "Artist"}}
        if path == "command":
            return []
        return {}
    monkeypatch.setattr(main, "_lidarr_request", fake)
    monkeypatch.setattr(main, "_log_delete_and_search", lambda *a, **k: None)
    return calls


def _library(tmp_path):
    """One flagged file with no Lidarr ID, known long enough to auto-delete; returns (config dir, track)."""
    track = tmp_path / "data" / "Artist" / "Album" / "01.flac"
    track.parent.mkdir(parents=True)
    track.write_bytes(b"x")
    cfg = tmp_path / "config"
    cfg.mkdir()
    (cfg / "corrupt.txt").write_text(f"{track}\n")
    (cfg / "corrupt_details.json").write_text(json.dumps({str(track): {"reason": "damaged"}}))
    (cfg / "corrupt_tracking.json").write_text(json.dumps({str(track): "2020-01-01T00:00:00"}))
    return cfg, track


def _folder_with_healthy_track(tmp_path):
    cfg, track = _library(tmp_path)
    healthy = track.parent / "02.flac"
    healthy.write_bytes(b"x")
    return cfg, track, healthy


def _auto_delete(cfg, monkeypatch):
    monkeypatch.setattr(main, "_reverify_before_delete", lambda to_delete, *a: to_delete)
    main.run_auto_delete(str(cfg), str(cfg / "log.txt"), 1, lidarr_url=URL, lidarr_api_key=KEY)


def _webui_env(monkeypatch):
    monkeypatch.setenv("LIDARR_URL", URL)
    monkeypatch.delenv("LIDARR_BLOCKLIST", raising=False)
    monkeypatch.setattr(main, "_load_lidarr_api_key", lambda: KEY)


def test_auto_delete_keeps_an_unmatched_file_when_lidarr_cannot_be_read(tmp_path, monkeypatch, caplog):
    cfg, track = _library(tmp_path)
    _lidarr(monkeypatch, up=False)
    _auto_delete(cfg, monkeypatch)
    assert track.exists()
    assert str(track) in (cfg / "corrupt.txt").read_text()
    assert "Lidarr lookup failed" in caplog.text
    assert "Lidarr API failed" not in caplog.text


def test_auto_delete_reports_a_failed_lidarr_delete_as_an_api_failure(tmp_path, monkeypatch, caplog):
    cfg, track = _library(tmp_path)
    _lidarr(monkeypatch)
    monkeypatch.setattr(main, "_lidarr_delete_corrupt", lambda *a, **k: None)
    _auto_delete(cfg, monkeypatch)
    assert track.exists()
    assert "Lidarr API failed" in caplog.text
    assert "Lidarr lookup failed" not in caplog.text


def test_auto_delete_sends_a_late_match_through_lidarr(tmp_path, monkeypatch):
    cfg, track = _library(tmp_path)
    calls = _lidarr(monkeypatch)
    _auto_delete(cfg, monkeypatch)
    assert "DELETE trackfile/bulk" in calls
    assert track.exists()
    assert json.loads((cfg / "corrupt_details.json").read_text())[str(track)]["trackfileId"] == 5


def test_a_file_lidarr_does_not_track_is_still_deleted_directly(tmp_path, monkeypatch):
    cfg, track = _library(tmp_path)
    _lidarr(monkeypatch, tracks=())
    _auto_delete(cfg, monkeypatch)
    assert not track.exists()


def test_webui_file_delete_keeps_the_file_when_lidarr_cannot_be_read(tmp_path, monkeypatch):
    cfg, track = _library(tmp_path)
    _lidarr(monkeypatch, up=False)
    _webui_env(monkeypatch)
    result = main.delete_corrupt_files([str(track)], str(cfg), music_dir=str(tmp_path / "data"))
    assert track.exists()
    assert result["count"] == 0 and result["errors"]


def test_webui_folder_delete_keeps_the_folder_when_lidarr_cannot_be_read(tmp_path, monkeypatch):
    cfg, track = _library(tmp_path)
    _lidarr(monkeypatch, up=False)
    _webui_env(monkeypatch)
    result = main.delete_album_folders(
        [str(track.parent)], str(cfg), music_dir=str(tmp_path / "data"), mode="whole")
    assert track.exists()
    assert result["count"] == 0 and result["errors"]


def test_webui_folder_delete_keeps_the_folder_when_the_album_lookup_fails(tmp_path, monkeypatch):
    cfg, track, healthy = _folder_with_healthy_track(tmp_path)
    _lidarr(monkeypatch, album_up=False)
    _webui_env(monkeypatch)
    result = main.delete_album_folders(
        [str(track.parent)], str(cfg), music_dir=str(tmp_path / "data"), mode="whole")
    assert healthy.exists() and track.exists()
    assert result["count"] == 0 and result["errors"]


def test_interactive_folder_delete_keeps_the_folder_when_the_album_lookup_fails(tmp_path, monkeypatch):
    cfg, track, healthy = _folder_with_healthy_track(tmp_path)
    _lidarr(monkeypatch, album_up=False)
    answers = iter(["i", "y"])
    monkeypatch.setattr(builtins, "input", lambda *a: next(answers))
    main._run_delete_mode_locked(str(cfg / "corrupt.txt"), str(cfg / "log.txt"), str(cfg),
                                 str(tmp_path / "data"), URL, KEY, False)
    assert healthy.exists() and track.exists()


def test_webui_folder_delete_looks_up_a_listed_file_with_no_saved_details(tmp_path, monkeypatch):
    cfg, track = _library(tmp_path)
    (cfg / "corrupt_details.json").write_text("{}")
    _lidarr(monkeypatch, up=False)
    _webui_env(monkeypatch)
    result = main.delete_album_folders(
        [str(track.parent)], str(cfg), music_dir=str(tmp_path / "data"), mode="whole")
    assert track.exists()
    assert result["count"] == 0 and result["errors"]


def test_corrupt_only_folder_delete_skips_a_file_missing_from_corrupt_txt(tmp_path, monkeypatch):
    cfg, track, unlisted = _folder_with_healthy_track(tmp_path)
    details = json.loads((cfg / "corrupt_details.json").read_text())
    details[str(unlisted)] = {"reason": "damaged"}
    (cfg / "corrupt_details.json").write_text(json.dumps(details))
    _lidarr(monkeypatch, tracks=())
    _webui_env(monkeypatch)
    main.delete_album_folders([str(track.parent)], str(cfg), music_dir=str(tmp_path / "data"), mode="corrupt")
    assert unlisted.exists()
    assert not track.exists()


def test_corrupt_only_folder_delete_removes_a_listed_file_lidarr_does_not_track(tmp_path, monkeypatch):
    cfg, track = _library(tmp_path)
    (cfg / "corrupt_details.json").write_text("{}")
    _lidarr(monkeypatch, tracks=())
    _webui_env(monkeypatch)
    main.delete_album_folders([str(track.parent)], str(cfg), music_dir=str(tmp_path / "data"), mode="corrupt")
    assert not track.exists()


def test_interactive_delete_stops_when_lidarr_cannot_be_read(tmp_path, monkeypatch):
    cfg, track = _library(tmp_path)
    _lidarr(monkeypatch, up=False)
    answers = iter(["a", "yes"])
    monkeypatch.setattr(builtins, "input", lambda *a: next(answers))
    main._run_delete_mode_locked(str(cfg / "corrupt.txt"), str(cfg / "log.txt"), str(cfg),
                                 str(tmp_path / "data"), URL, KEY, False)
    assert track.exists()
    assert str(track) in (cfg / "corrupt.txt").read_text()


def test_one_failed_artist_request_fails_the_whole_index(monkeypatch):
    def fake(url, api_key, method="GET", data=None, timeout=30):
        if url.endswith("/artist"):
            return [{"id": 1, "path": "/music/A"}, {"id": 2, "path": "/music/B"}]
        return None if url.endswith("artistId=2") else []
    monkeypatch.setattr(main, "_lidarr_request", fake)
    index = main._build_lidarr_index(URL, KEY)
    assert index is None


def test_a_file_with_no_saved_details_is_looked_up_too(tmp_path, monkeypatch):
    _lidarr(monkeypatch, up=False)
    ready = main._lidarr_ids_before_delete({}, ["/data/b.flac"], URL, KEY, str(tmp_path))
    assert ready is False


def test_a_file_with_no_saved_details_gets_its_lidarr_id(tmp_path, monkeypatch):
    _lidarr(monkeypatch)
    details = {}
    ready = main._lidarr_ids_before_delete(details, ["/data/Artist/Album/01.flac"], URL, KEY, str(tmp_path))
    assert ready is True
    assert details["/data/Artist/Album/01.flac"]["trackfileId"] == 5


def test_only_the_files_being_deleted_are_looked_up(tmp_path, monkeypatch):
    calls = _lidarr(monkeypatch, up=False)
    details = {"/data/a.flac": {"reason": "damaged", "trackfileId": 5},
               "/data/b.flac": {"reason": "damaged"}}
    ready = main._lidarr_ids_before_delete(details, ["/data/a.flac"], URL, KEY, str(tmp_path))
    assert ready is True
    assert calls == []
