"""Delete mode refuses a music dir it can't read and write."""

import os
import stat
import subprocess
import sys

import pytest

APP = os.path.join(os.path.dirname(__file__), "..", "app")
sys.path.insert(0, APP)

import main  # noqa: E402

needs_non_root = pytest.mark.skipif(
    os.geteuid() == 0, reason="root ignores directory permissions")


@pytest.fixture
def read_only_dir(tmp_path):
    music = tmp_path / "music"
    music.mkdir()
    music.chmod(stat.S_IRUSR | stat.S_IXUSR)
    yield music
    music.chmod(stat.S_IRWXU)


def _run_delete_mode(tmp_path, music_dir, monkeypatch):
    ran = []
    monkeypatch.setattr(main, "_run_delete_mode_locked", lambda *a: ran.append(1))
    main.run_delete_mode(str(tmp_path / "corrupt.txt"), str(tmp_path / "log"),
                         str(tmp_path), input_folder=music_dir)
    return ran


def test_a_missing_music_dir_stops_delete_mode(tmp_path, monkeypatch):
    with pytest.raises(SystemExit) as exc:
        _run_delete_mode(tmp_path, str(tmp_path / "missing"), monkeypatch)
    assert exc.value.code == 1


@needs_non_root
def test_a_read_only_music_dir_stops_delete_mode(tmp_path, read_only_dir,
                                                 monkeypatch):
    with pytest.raises(SystemExit) as exc:
        _run_delete_mode(tmp_path, str(read_only_dir), monkeypatch)
    assert exc.value.code == 1


def test_a_writable_music_dir_runs_delete_mode(tmp_path, monkeypatch):
    ran = _run_delete_mode(tmp_path, str(tmp_path), monkeypatch)
    assert ran == [1]


@needs_non_root
def test_delete_mode_checks_the_music_dir_from_the_config(
        tmp_path, read_only_dir):
    """MUSIC_DIR is unset, so only beatscheck.conf names the read-only dir."""
    (tmp_path / "beatscheck.conf").write_text(f'music_dir = "{read_only_dir}"\n')
    env = {k: v for k, v in os.environ.items() if k != "MUSIC_DIR"}
    env.update(CONFIG_DIR=str(tmp_path), MODE="delete", PYTHONUNBUFFERED="1")

    result = subprocess.run([sys.executable, os.path.join(APP, "main.py")],
                            env=env, capture_output=True, text=True,
                            timeout=60, stdin=subprocess.DEVNULL)

    assert result.returncode == 1
    assert f"Music directory ({read_only_dir}) must be readable and writable" in (
        result.stdout + result.stderr)
