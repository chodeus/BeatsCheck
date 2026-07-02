"""Smoke tests for BeatsCheck.

Focused on the security-critical guards: delete-path validation
(mount-root / containment / symlink), the whole-folder delete gate, the
fail-closed auth loader, and password hashing. Runs with plain pytest and
no external services. Import-safe because main.py guards main() behind
``if __name__ == '__main__'``.
"""

import json
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "app"))

import main  # noqa: E402
import webui  # noqa: E402


# --- delete-path validation ------------------------------------------------

def test_validate_delete_rejects_mount_root(tmp_path):
    music = tmp_path / "music"
    music.mkdir()
    validated, errors = main._validate_delete_folders(
        [str(music)], os.path.realpath(str(music)))
    assert validated == []
    assert errors and errors[0]["error"] == "cannot delete mount root"


def test_validate_delete_rejects_outside_music(tmp_path):
    music = tmp_path / "music"
    music.mkdir()
    other = tmp_path / "other"
    other.mkdir()
    validated, errors = main._validate_delete_folders(
        [str(other)], os.path.realpath(str(music)))
    assert validated == []
    assert errors[0]["error"] == "outside music directory"


def test_validate_delete_rejects_symlink(tmp_path):
    music = tmp_path / "music"
    music.mkdir()
    real = music / "album"
    real.mkdir()
    link = music / "link"
    link.symlink_to(real)
    validated, errors = main._validate_delete_folders(
        [str(link)], os.path.realpath(str(music)))
    assert validated == []
    assert errors[0]["error"] == "symlink rejected"


def test_validate_delete_accepts_real_subfolder(tmp_path):
    music = tmp_path / "music"
    music.mkdir()
    album = music / "album"
    album.mkdir()
    validated, errors = main._validate_delete_folders(
        [str(album)], os.path.realpath(str(music)))
    assert errors == []
    assert len(validated) == 1


# --- whole-folder delete gate ----------------------------------------------

def test_delete_whole_refuses_folder_without_corrupt(tmp_path, monkeypatch):
    monkeypatch.delenv("LIDARR_URL", raising=False)
    config = tmp_path / "config"
    config.mkdir()
    music = tmp_path / "music"
    music.mkdir()
    album = music / "album"
    album.mkdir()
    (album / "track.flac").write_bytes(b"x")
    (config / "corrupt.txt").write_text("")  # nothing flagged
    result = main.delete_album_folders(
        [str(album)], str(config), music_dir=str(music), mode="whole")
    assert result["count"] == 0
    assert album.exists()  # folder must NOT be deleted
    assert result["errors"]
    assert "no flagged corrupt" in result["errors"][0]["error"]


def test_delete_whole_allows_flagged_folder(tmp_path, monkeypatch):
    monkeypatch.delenv("LIDARR_URL", raising=False)
    config = tmp_path / "config"
    config.mkdir()
    music = tmp_path / "music"
    music.mkdir()
    album = music / "album"
    album.mkdir()
    track = album / "track.flac"
    track.write_bytes(b"x")
    (config / "corrupt.txt").write_text(str(track) + "\n")
    result = main.delete_album_folders(
        [str(album)], str(config), music_dir=str(music), mode="whole")
    assert result["count"] >= 1
    assert not album.exists()  # flagged folder removed


# --- fail-closed auth loader -----------------------------------------------

def test_load_auth_absent_returns_none(tmp_path):
    assert webui._load_auth(str(tmp_path)) is None


def test_load_auth_valid_returns_dict(tmp_path):
    (tmp_path / "webui_auth.json").write_text(json.dumps({
        "username": "admin",
        "password_hash": webui._hash_password("hunter2000"),
    }))
    data = webui._load_auth(str(tmp_path))
    assert data and data["username"] == "admin"


def test_load_auth_corrupt_fails_closed(tmp_path):
    (tmp_path / "webui_auth.json").write_text("{ not valid json")
    assert webui._load_auth(str(tmp_path)) is webui._AUTH_UNREADABLE


def test_load_auth_missing_fields_fails_closed(tmp_path):
    (tmp_path / "webui_auth.json").write_text(json.dumps({"username": "a"}))
    assert webui._load_auth(str(tmp_path)) is webui._AUTH_UNREADABLE


# --- password + path helpers -----------------------------------------------

def test_password_hash_roundtrip():
    h = webui._hash_password("hunter2000")
    assert webui._verify_password("hunter2000", h)
    assert not webui._verify_password("wrong", h)


def test_is_subpath_separator_aware():
    assert webui._is_subpath("/data/x", "/data")
    assert webui._is_subpath("/data", "/data")
    assert not webui._is_subpath("/data2", "/data")
