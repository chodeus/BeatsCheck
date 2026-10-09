"""scripts/rescan.sh: the trigger it writes, and how it writes it."""

import os
import subprocess

import pytest

SCRIPT = os.path.join(os.path.dirname(__file__), "..", "scripts", "rescan.sh")


def _rescan(config_dir, *args):
    env = dict(os.environ, CONFIG_DIR=str(config_dir))
    return subprocess.run(["sh", SCRIPT, *args], env=env, capture_output=True,
                          text=True, timeout=30)


@pytest.mark.parametrize("args, trigger", [
    ([], ""),
    (["report"], "report"),
    (["--mode", "move"], "move"),
    (["--fresh"], "fresh:"),
    (["--fresh", "report"], "fresh:report"),
    (["--mode", "move", "--fresh"], "fresh:move"),
])
def test_the_trigger_matches_the_webui_format(tmp_path, args, trigger):
    result = _rescan(tmp_path, *args)

    assert result.returncode == 0
    assert (tmp_path / ".rescan").read_text() == trigger


@pytest.mark.parametrize("args", [
    ["--mode", "delete"], ["--mode", "--fresh"], ["--mode"], ["delete"],
])
def test_an_unsupported_mode_writes_nothing(tmp_path, args):
    result = _rescan(tmp_path, *args)

    assert result.returncode == 1
    assert not (tmp_path / ".rescan").exists()


def test_a_plain_rescan_replaces_an_earlier_mode(tmp_path):
    (tmp_path / ".rescan").write_text("move")

    result = _rescan(tmp_path)

    assert result.returncode == 0
    assert (tmp_path / ".rescan").read_text() == ""


def test_fresh_leaves_the_resume_cache_to_the_scanner(tmp_path):
    (tmp_path / "processed.txt").write_text("/data/a.flac\n")

    result = _rescan(tmp_path, "--fresh", "report")

    assert result.returncode == 0
    assert (tmp_path / "processed.txt").exists()


def test_a_symlink_at_the_trigger_is_replaced_not_followed(tmp_path):
    target = tmp_path / "elsewhere"
    target.write_text("keep me")
    (tmp_path / ".rescan").symlink_to(target)

    result = _rescan(tmp_path, "report")

    assert result.returncode == 0
    assert target.read_text() == "keep me"
    assert not (tmp_path / ".rescan").is_symlink()
    assert (tmp_path / ".rescan").read_text() == "report"


def test_no_temp_file_is_left_behind(tmp_path):
    result = _rescan(tmp_path, "move")

    assert result.returncode == 0
    assert sorted(p.name for p in tmp_path.iterdir()) == [".rescan"]


def test_an_unwritable_config_dir_fails(tmp_path):
    missing = tmp_path / "missing"

    result = _rescan(missing, "report")

    assert result.returncode == 1
    assert "could not write" in result.stdout
