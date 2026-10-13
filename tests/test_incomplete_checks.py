"""A check that cannot judge a file is a warning and a retry, never a corrupt or clean verdict."""

import json
import logging
import os
import subprocess
import sys
import time

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "app"))

import main  # noqa: E402

PROGRESS = "out_time_us=10000000\nprogress=end\n"


def _proc(returncode=0, stderr=""):
    return subprocess.CompletedProcess([], returncode, PROGRESS, stderr)


def _ffmpeg_gives(monkeypatch, *results):
    """Stub the decode: each run takes the next result, raising it if it is an exception."""
    queue = list(results)

    def fake_run(cmd, **kwargs):
        result = queue.pop(0)
        if isinstance(result, BaseException):
            raise result
        return result
    monkeypatch.setattr(main.subprocess, "run", fake_run)


@pytest.fixture
def track(tmp_path):
    path = tmp_path / "a.flac"
    path.write_bytes(b"x" * 4096)
    return str(path)


def test_a_file_that_cannot_be_read_is_incomplete(tmp_path):
    with pytest.raises(main.CheckIncomplete, match="could not read the file"):
        main.check_audio_file(str(tmp_path / "gone.flac"))


def test_a_decode_timeout_is_incomplete(track, monkeypatch):
    _ffmpeg_gives(monkeypatch, subprocess.TimeoutExpired("ffmpeg", 600))
    with pytest.raises(main.CheckIncomplete, match="timed out"):
        main.check_audio_file(track)


@pytest.mark.parametrize("returncode,stderr", [
    (-9, ""),                                       # killed, e.g. out of memory
    (-6, "free(): invalid pointer\n"),              # crashed after printing
    (255, ""),                                      # stopped by a signal ffmpeg caught
    (1, "[in#0 @ 0x1] Error opening input: Permission denied\n"
        "Error opening input file /music/a.flac.\n"
        "Error opening input files: Permission denied\n"),
    (1, "[in#0/flac @ 0x1] Error during demuxing: Input/output error\n"),
])
def test_an_ffmpeg_run_that_cannot_judge_the_file_is_incomplete(track, monkeypatch, returncode, stderr):
    _ffmpeg_gives(monkeypatch, _proc(returncode, stderr))
    with pytest.raises(main.CheckIncomplete):
        main.check_audio_file(track)


def test_a_real_decode_error_is_still_corrupt(track, monkeypatch):
    _ffmpeg_gives(monkeypatch, _proc(1, "[flac @ 0x1] invalid residual\n"))
    _, corrupt, reason, _ = main.check_audio_file(track)
    assert corrupt
    assert reason == "invalid residual"


def test_a_truncated_matroska_file_is_still_corrupt(track, monkeypatch):
    _ffmpeg_gives(monkeypatch, _proc(1, (
        "[matroska,webm @ 0x1] File ended prematurely at pos. 7340032 (0x700000)\n"
        "[in#0/matroska,webm @ 0x1] Error during demuxing: Input/output error\n")))
    _, corrupt, reason, _ = main.check_audio_file(track)
    assert corrupt
    assert reason.startswith("File ended prematurely")


def test_a_cover_art_recheck_that_times_out_is_incomplete(track, monkeypatch):
    _ffmpeg_gives(monkeypatch, _proc(1, "Error parsing attached picture\n"),
                  subprocess.TimeoutExpired("ffmpeg", 600))
    with pytest.raises(main.CheckIncomplete, match="timed out"):
        main.check_audio_file(track)


@pytest.mark.parametrize("error", [OSError(5, "Input/output error"),
                                   subprocess.TimeoutExpired("ffmpeg", 60)])
def test_a_length_check_that_cannot_finish_is_incomplete(track, monkeypatch, error):
    monkeypatch.delenv("LENGTH_CHECK", raising=False)
    _ffmpeg_gives(monkeypatch, _proc())

    def fail(path, decoded):
        raise error
    monkeypatch.setattr(main.length_check, "cut_short_reason", fail)
    with pytest.raises(main.CheckIncomplete):
        main.check_audio_file(track)


def test_a_scan_warns_and_leaves_the_file_for_the_next_scan(tmp_path, monkeypatch, caplog):
    music, cfg = tmp_path / "music", tmp_path / "cfg"
    music.mkdir()
    cfg.mkdir()
    for name in ("busy.flac", "good.flac"):
        path = music / name
        path.write_bytes(b"x" * 4096)
        os.utime(path, (time.time() - 3600,) * 2)

    def fake_check(path):
        if path.endswith("busy.flac"):
            raise main.CheckIncomplete("the decode timed out after 10 minutes")
        return (path, False, None, 4096)
    monkeypatch.setattr(main, "check_audio_file", fake_check)
    log_file = cfg / "beats_check.log"
    with caplog.at_level(logging.INFO):
        main._run_scan_inner(str(music), str(tmp_path / "out"), str(log_file), str(cfg),
                             "report", 1, str(cfg / "corrupt.txt"), 0)

    processed = (cfg / "processed.txt").read_text().splitlines()
    assert processed == [str(music / "good.flac")]
    corrupt = (cfg / "corrupt.txt").read_text() if (cfg / "corrupt.txt").exists() else ""
    assert "busy.flac" not in corrupt
    log_text = log_file.read_text()
    assert f"WARN: {music / 'busy.flac'} - the decode timed out after 10 minutes" in log_text
    levels = [r.levelno for r in caplog.records if "busy.flac" in r.getMessage()]
    assert levels == [logging.WARNING]


def test_auto_delete_keeps_a_file_it_cannot_re_verify(tmp_path, monkeypatch, caplog):
    f = tmp_path / "u.mp3"
    f.write_bytes(b"x" * 2048)
    (tmp_path / "corrupt.txt").write_text(f"{f}\n")
    (tmp_path / "corrupt_tracking.json").write_text(
        json.dumps({str(f): "2020-01-01T00:00:00"}))

    def busy(path):
        raise main.CheckIncomplete("the decode timed out after 10 minutes")
    monkeypatch.setattr(main, "check_audio_file", busy)
    with caplog.at_level(logging.INFO):
        main.run_auto_delete(str(tmp_path), str(tmp_path / "log.txt"), delete_after_days=1)

    assert f.exists()
    assert str(f) in (tmp_path / "corrupt.txt").read_text()
    warned = [r for r in caplog.records if "Could not re-verify" in r.getMessage()]
    assert [r.levelno for r in warned] == [logging.WARNING]
    assert not any(r.levelno >= logging.ERROR for r in caplog.records)
