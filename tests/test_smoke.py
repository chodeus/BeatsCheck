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


# --- ffmpeg benign metadata-noise stripping --------------------------------

def test_strip_benign_metadata_noise_id3_only_is_empty():
    # Old ffmpeg (4.x-6.x) stderr for a bad-comment-tag file with clean audio.
    stderr = "Incorrect BOM value\nError reading comment frame, skipped"
    assert main._strip_benign_metadata_noise(stderr) == ""


def test_strip_benign_metadata_noise_keeps_real_decoder_error():
    stderr = "[mp3 @ 0x1] invalid new backstep -1"
    assert "invalid new backstep -1" in main._strip_benign_metadata_noise(stderr)


def test_strip_benign_metadata_noise_keeps_real_error_mixed_with_benign():
    stderr = ("Incorrect BOM value\nHeader missing\n"
              "Error reading comment frame, skipped")
    assert main._strip_benign_metadata_noise(stderr) == "Header missing"


def test_strip_benign_metadata_noise_blank_is_empty():
    assert main._strip_benign_metadata_noise("") == ""
    assert main._strip_benign_metadata_noise("   \n") == ""


def test_is_benign_metadata_noise_flags_id3_not_decode_errors():
    assert main._is_benign_metadata_noise("Incorrect BOM value: 0x1234")
    assert main._is_benign_metadata_noise("Error reading frame TXXX, skipped")
    assert not main._is_benign_metadata_noise("Header missing")
    assert not main._is_benign_metadata_noise(
        "[mp3 @ 0x1] invalid new backstep -1")


# --- attached-picture re-verify (scanner) ----------------------------------

class _FakeProc:
    def __init__(self, returncode=0, stderr=""):
        self.returncode = returncode
        self.stderr = stderr


def test_attached_picture_failure_reverifies_clean(monkeypatch, tmp_path):
    f = tmp_path / "song.mp3"
    f.write_bytes(b"x" * 2048)
    calls = []

    def fake_run(cmd, **kwargs):
        calls.append(cmd)
        if "-err_detect" in cmd:
            return _FakeProc(1, "[mp3 @ 0x1] Error parsing attached picture\n")
        return _FakeProc(0, "Error parsing attached picture\n")

    monkeypatch.setattr(main.subprocess, "run", fake_run)
    _, is_corrupt, reason, _ = main.check_audio_file(str(f))
    assert not is_corrupt
    assert len(calls) == 2
    assert "-err_detect" not in calls[1]


def test_attached_picture_reverify_still_corrupt_on_real_error(
        monkeypatch, tmp_path):
    f = tmp_path / "song.mp3"
    f.write_bytes(b"x" * 2048)

    def fake_run(cmd, **kwargs):
        if "-err_detect" in cmd:
            return _FakeProc(
                1, "Could not read mimetype from an attached picture\n")
        return _FakeProc(0, "Header missing\n")

    monkeypatch.setattr(main.subprocess, "run", fake_run)
    _, is_corrupt, reason, _ = main.check_audio_file(str(f))
    assert is_corrupt
    assert "Header missing" in reason


def test_picture_markers_are_benign_noise():
    stderr = ("Could not read mimetype from an attached picture\n"
              "Error parsing attached picture\n")
    assert main._strip_benign_metadata_noise(stderr) == ""


# --- heal-on-clean-rescan ---------------------------------------------------

def test_finalize_scan_drops_healed_paths(tmp_path):
    log_dir = tmp_path / "cfg"
    log_dir.mkdir()
    good = tmp_path / "good.mp3"
    good.write_bytes(b"x")
    bad = tmp_path / "bad.mp3"
    bad.write_bytes(b"x")
    clp = log_dir / "corrupt.txt"
    clp.write_text(f"{good}\n{bad}\n")
    main._finalize_scan(str(log_dir), str(clp), {}, None,
                        {"corrupted": 0}, healed={str(good)})
    remaining = clp.read_text().splitlines()
    assert str(good) not in remaining
    assert str(bad) in remaining


# --- auto-delete re-verify --------------------------------------------------

def test_auto_delete_heals_reverified_clean_file(monkeypatch, tmp_path):
    healed_file = tmp_path / "h.mp3"
    healed_file.write_bytes(b"x" * 2048)
    (tmp_path / "corrupt.txt").write_text(f"{healed_file}\n")
    (tmp_path / "corrupt_tracking.json").write_text(
        json.dumps({str(healed_file): "2020-01-01T00:00:00"}))
    (tmp_path / "corrupt_details.json").write_text(
        json.dumps({str(healed_file): {"reason": "stale"}}))

    monkeypatch.setattr(main, "check_audio_file",
                        lambda p: (p, False, None, 1))
    main.run_auto_delete(str(tmp_path), str(tmp_path / "log.txt"),
                         delete_after_days=1)

    assert healed_file.exists()
    tracking = json.loads((tmp_path / "corrupt_tracking.json").read_text())
    assert str(healed_file) not in tracking
    assert str(healed_file) not in (tmp_path / "corrupt.txt").read_text()
    details = json.loads((tmp_path / "corrupt_details.json").read_text())
    assert str(healed_file) not in details


def test_auto_delete_still_deletes_reverified_corrupt_file(
        monkeypatch, tmp_path):
    bad_file = tmp_path / "b.mp3"
    bad_file.write_bytes(b"x" * 2048)
    (tmp_path / "corrupt.txt").write_text(f"{bad_file}\n")
    (tmp_path / "corrupt_tracking.json").write_text(
        json.dumps({str(bad_file): "2020-01-01T00:00:00"}))

    monkeypatch.setattr(main, "check_audio_file",
                        lambda p: (p, True, "Header missing", 1))
    main.run_auto_delete(str(tmp_path), str(tmp_path / "log.txt"),
                         delete_after_days=1)

    assert not bad_file.exists()


def test_auto_delete_skips_file_when_reverify_crashes(monkeypatch, tmp_path):
    f = tmp_path / "u.mp3"
    f.write_bytes(b"x" * 2048)
    (tmp_path / "corrupt.txt").write_text(f"{f}\n")
    (tmp_path / "corrupt_tracking.json").write_text(
        json.dumps({str(f): "2020-01-01T00:00:00"}))

    def boom(path):
        raise RuntimeError("ffmpeg missing")

    monkeypatch.setattr(main, "check_audio_file", boom)
    main.run_auto_delete(str(tmp_path), str(tmp_path / "log.txt"),
                         delete_after_days=1)

    # Unable to verify -> file must survive and stay tracked for next run
    assert f.exists()
    assert str(f) in (tmp_path / "corrupt.txt").read_text()
