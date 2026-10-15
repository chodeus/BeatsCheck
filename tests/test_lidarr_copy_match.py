"""A flagged file matches a Lidarr track file only when it is that file, so a delete can't remove Lidarr's good copy."""

import json
import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "app"))

import main  # noqa: E402

URL = "http://lidarr.invalid"
KEY = "not-a-key"
OTHER_MOUNT = "/lidarr-root.invalid"


def _lidarr(monkeypatch, root, tracks=("Album/01.flac",), up=True):
    """Stub Lidarr with one artist at <root>/Artist holding *tracks*, IDs from 5; returns (requests, bulk-deleted IDs)."""
    calls, deleted = [], []

    def fake(url, api_key, method="GET", data=None, timeout=30):
        path = url.split("/api/v1/", 1)[1]
        calls.append(f"{method} {path.split('?', 1)[0]}")
        if not up:
            return None
        if path == "artist":
            return [{"id": 1, "path": f"{root}/Artist"}]
        if path.startswith("trackfile?"):
            return [{"id": 5 + i, "path": f"{root}/Artist/{t}", "albumId": 7} for i, t in enumerate(tracks)]
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
    return calls, deleted


def _file(tmp_path, rel):
    path = tmp_path / "data" / rel
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b"x")
    return str(path)


def _root(tmp_path, mount):
    return str(tmp_path / "data") if mount == "same" else OTHER_MOUNT


def _look_up(when, fp, tmp_path):
    """Match *fp* the way a scan or the lookup before a delete does; returns its details."""
    details = {fp: {"reason": "damaged"}}
    if when == "scan":
        main._resolve_single_lidarr_id(fp, details, main._build_lidarr_index(URL, KEY))
    else:
        main._lidarr_ids_before_delete(details, [fp], URL, KEY, str(tmp_path))
    return details[fp]


@pytest.mark.parametrize("when", ["scan", "delete"])
@pytest.mark.parametrize("flagged, tracked, mount", [
    ("Incoming/Artist/Album/01.flac", "Album/01.flac", "same"),
    ("Incoming/Artist/Album/01.flac", "Album/01.flac", "other"),
    ("Artist/Album (Deluxe)/01.flac", "Album/01.flac", "other"),
    ("Artist/Album (Deluxe)/CD1/07.flac", "Album/CD1/07.flac", "other"),
    ("Other/Album/01.flac", "Album/01.flac", "other"),
])
def test_a_file_that_is_not_lidarrs_copy_gets_no_lidarr_id(tmp_path, monkeypatch, when, flagged, tracked, mount):
    _file(tmp_path, f"Artist/{tracked}")
    fp = _file(tmp_path, flagged)
    _lidarr(monkeypatch, _root(tmp_path, mount), tracks=(tracked,))
    detail = _look_up(when, fp, tmp_path)
    assert "trackfileId" not in detail


@pytest.mark.parametrize("when", ["scan", "delete"])
@pytest.mark.parametrize("mount", ["same", "other"])
def test_lidarrs_own_file_still_gets_its_id(tmp_path, monkeypatch, when, mount):
    fp = _file(tmp_path, "Artist/Album/01.flac")
    _lidarr(monkeypatch, _root(tmp_path, mount))
    detail = _look_up(when, fp, tmp_path)
    assert detail["trackfileId"] == 5


def test_a_windows_lidarr_path_still_matches(tmp_path):
    fp = _file(tmp_path, "Artist/Album/01.flac")
    same = main._is_lidarr_copy(fp, "D:\\Music\\Artist\\Album\\01.flac", "D:\\Music\\Artist")
    assert same is True


def _stale_library(tmp_path):
    """A second copy flagged with the ID of Lidarr's good copy, as an older scan saved it; returns (config, flagged, good)."""
    good = [_file(tmp_path, f"Artist/Album/{n}.flac") for n in ("01", "02")]
    flagged = _file(tmp_path, "Incoming/Artist/Album/01.flac")
    _file(tmp_path, "Incoming/Artist/Album/02.flac")
    cfg = tmp_path / "config"
    cfg.mkdir()
    (cfg / "corrupt.txt").write_text(f"{flagged}\n")
    (cfg / "corrupt_details.json").write_text(json.dumps({flagged: {
        "reason": "damaged", "trackfileId": 5, "albumId": 7, "artistId": 1,
        "lidarrPath": f"{OTHER_MOUNT}/Artist/Album/01.flac"}}))
    (cfg / "corrupt_tracking.json").write_text(json.dumps({flagged: "2020-01-01T00:00:00"}))
    return cfg, flagged, good


def _webui_delete(tmp_path, monkeypatch, cfg, flagged):
    monkeypatch.setenv("LIDARR_URL", URL)
    monkeypatch.delenv("LIDARR_BLOCKLIST", raising=False)
    monkeypatch.setattr(main, "_load_lidarr_api_key", lambda: KEY)
    main.delete_corrupt_files([flagged], str(cfg), music_dir=str(tmp_path / "data"))


def _webui_folder_delete(tmp_path, monkeypatch, cfg, flagged):
    monkeypatch.setenv("LIDARR_URL", URL)
    monkeypatch.delenv("LIDARR_BLOCKLIST", raising=False)
    monkeypatch.setattr(main, "_load_lidarr_api_key", lambda: KEY)
    main.delete_album_folders([os.path.dirname(flagged)], str(cfg), music_dir=str(tmp_path / "data"), mode="whole")


def _auto_delete(tmp_path, monkeypatch, cfg, flagged):
    monkeypatch.setattr(main, "_reverify_before_delete", lambda to_delete, *a: to_delete)
    main.run_auto_delete(str(cfg), str(cfg / "log.txt"), 1, lidarr_url=URL, lidarr_api_key=KEY)


@pytest.mark.parametrize("delete", [_webui_delete, _webui_folder_delete, _auto_delete])
def test_a_wrong_match_saved_by_an_older_scan_deletes_the_flagged_file_not_lidarrs(tmp_path, monkeypatch, delete):
    cfg, flagged, good = _stale_library(tmp_path)
    _, deleted = _lidarr(monkeypatch, OTHER_MOUNT, tracks=("Album/01.flac", "Album/02.flac"))
    delete(tmp_path, monkeypatch, cfg, flagged)
    assert deleted == []
    assert not os.path.exists(flagged)
    assert all(os.path.exists(g) for g in good)


@pytest.mark.parametrize("saved, root", [
    ({"artistId": 1}, OTHER_MOUNT),
    ({"artistId": 2, "lidarrPath": f"{OTHER_MOUNT}/Artist/Album/01.flac"}, OTHER_MOUNT),
    ({"artistId": 1, "lidarrPath": f"{OTHER_MOUNT}/Artist/Album/01.flac"}, f"{OTHER_MOUNT}/moved"),
])
def test_a_saved_match_that_cannot_be_checked_is_looked_up_again(tmp_path, monkeypatch, saved, root):
    fp = _file(tmp_path, "Artist/Album/01.flac")
    _lidarr(monkeypatch, root)
    details = {fp: dict(saved, reason="damaged", trackfileId=99, albumId=7)}
    ready = main._lidarr_ids_before_delete(details, [fp], URL, KEY, str(tmp_path))
    assert ready is True
    assert details[fp]["trackfileId"] == 5


def test_a_saved_match_is_not_trusted_when_lidarr_cannot_be_read(tmp_path, monkeypatch):
    fp = _file(tmp_path, "Artist/Album/01.flac")
    _lidarr(monkeypatch, OTHER_MOUNT, up=False)
    details = {fp: {"reason": "damaged", "trackfileId": 5, "albumId": 7, "artistId": 1,
                    "lidarrPath": f"{OTHER_MOUNT}/Artist/Album/01.flac"}}
    ready = main._lidarr_ids_before_delete(details, [fp], URL, KEY, str(tmp_path))
    assert ready is False


def test_a_folder_delete_re_checks_a_saved_match_of_a_file_not_in_the_list(tmp_path, monkeypatch):
    cfg, flagged, good = _stale_library(tmp_path)
    unlisted = os.path.join(os.path.dirname(flagged), "02.flac")
    (cfg / "corrupt_details.json").write_text(json.dumps({flagged: {"reason": "damaged"}, unlisted: {
        "reason": "damaged", "trackfileId": 6, "albumId": 7, "artistId": 1,
        "lidarrPath": f"{OTHER_MOUNT}/Artist/Album/02.flac"}}))
    _, deleted = _lidarr(monkeypatch, OTHER_MOUNT, tracks=("Album/01.flac", "Album/02.flac"))
    _webui_folder_delete(tmp_path, monkeypatch, cfg, flagged)
    assert deleted == []
    assert all(os.path.exists(g) for g in good)


def test_a_folder_lookup_stops_when_saved_matches_cannot_be_checked(monkeypatch):
    monkeypatch.setattr(main, "_drop_wrong_lidarr_matches", lambda *a: None)
    file_to_tfid = main._build_file_to_tfid(["/data/a.flac"], set(), {"/data/a.flac": {"trackfileId": 5}}, URL, KEY)
    assert file_to_tfid is None
