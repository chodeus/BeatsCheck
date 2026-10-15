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


def _lidarr(monkeypatch, tracks=(SIBLING,), fail_second=None, twin=False, root="/music"):
    """Stub Lidarr; request 2 to *fail_second* fails, *twin* adds a same-named artist in a 2nd root. Returns deleted IDs."""
    deleted, seen = [], []

    def fake(url, api_key, method="GET", data=None, timeout=30):
        path = url.split("/api/v1/", 1)[1]
        seen.append(path)
        if path == fail_second and seen.count(path) == 2:
            return None
        if path == "artist":
            twins = [{"id": 3, "path": "/music2/Artist"}] if twin else []
            return [{"id": 1, "path": f"{root}/Artist"}, {"id": 2, "path": f"{root}/Other"}] + twins
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


def _folder(tmp_path, under=""):
    """A flagged file Lidarr doesn't track beside a healthy one, in data/<under>/Artist/Album; returns (config dir, folder)."""
    folder = tmp_path / "data" / under / "Artist" / "Album"
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


def _managed_copy(tmp_path):
    """The album's tracked copy at data/Artist/Album."""
    copy = tmp_path / "data" / "Artist" / "Album" / "02.flac"
    copy.parent.mkdir(parents=True)
    copy.write_bytes(b"x")
    return copy


def test_a_track_lidarr_has_at_the_same_path_elsewhere_is_not_matched(tmp_path, monkeypatch):
    cfg, folder = _folder(tmp_path, under="Incoming")
    copy = _managed_copy(tmp_path)
    data = str(tmp_path / "data")
    deleted = _lidarr(monkeypatch, tracks=(str(copy),), root=data)
    _webui_delete(tmp_path, monkeypatch, cfg, folder)
    assert deleted == []
    assert copy.exists()


def test_a_copy_higher_in_the_library_blocks_a_match_across_mount_paths(tmp_path, monkeypatch):
    cfg, folder = _folder(tmp_path, under="Incoming")
    copy = _managed_copy(tmp_path)
    deleted = _lidarr(monkeypatch)
    _webui_delete(tmp_path, monkeypatch, cfg, folder)
    assert deleted == []
    assert copy.exists()


def test_a_track_lidarr_has_at_this_very_path_is_matched(tmp_path, monkeypatch):
    cfg, folder = _folder(tmp_path)
    data = str(tmp_path / "data")
    deleted = _lidarr(monkeypatch, tracks=(str(folder / "02.flac"),), root=data)
    _webui_delete(tmp_path, monkeypatch, cfg, folder)
    assert deleted == [9]


def _interactive_folder_delete(cfg, folder, music_dir):
    """Run the interactive Lidarr whole-folder delete on *folder*; returns how many folders it reports deleted."""
    with open(cfg / "log.txt", "a", encoding="utf-8") as log:
        result = main._handle_folder_delete_lidarr(
            str(folder), [str(folder / "01.flac")], log, str(music_dir), {}, URL, KEY, False)
    return result[0]


def test_interactive_folder_delete_does_not_count_a_folder_left_with_a_subfolder(tmp_path, monkeypatch):
    cfg, folder = _folder(tmp_path)
    (folder / "Scans").mkdir()
    (folder / "Scans" / "cover.jpg").write_bytes(b"x")
    _lidarr(monkeypatch, tracks=())
    folders_deleted = _interactive_folder_delete(cfg, folder, tmp_path / "data")
    assert not (folder / "02.flac").exists()
    assert folder.exists()
    assert folders_deleted == 0


def test_interactive_folder_delete_does_not_count_a_folder_whose_file_failed(tmp_path, monkeypatch):
    cfg, folder = _folder(tmp_path)
    _lidarr(monkeypatch, tracks=())
    real_remove = os.remove

    def remove(path):
        if path.endswith("01.flac"):
            raise OSError("busy")
        real_remove(path)
    monkeypatch.setattr(main.os, "remove", remove)
    # As the music root the folder is never removed, so only the failed file decides the count.
    folders_deleted = _interactive_folder_delete(cfg, folder, folder)
    assert not (folder / "02.flac").exists()
    assert folders_deleted == 0


@pytest.mark.parametrize("failing", ["artist", "trackfile?artistId=1"])
def test_folder_delete_keeps_the_folder_when_the_artist_lookup_fails(tmp_path, monkeypatch, failing):
    cfg, folder = _folder(tmp_path)
    deleted = _lidarr(monkeypatch, fail_second=failing)
    result = _webui_delete(tmp_path, monkeypatch, cfg, folder)
    assert deleted == []
    assert (folder / "01.flac").exists() and (folder / "02.flac").exists()
    assert result["errors"]
