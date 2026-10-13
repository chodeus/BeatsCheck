"""corrupt.txt is rewritten deduplicated and in path order, whichever step rewrites it."""

import json
import os
import random
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "app"))

import main  # noqa: E402
import webui  # noqa: E402

ARTISTS = ("abba", "Beck", "nonpoint", "Zz Top")  # path order, ignoring case


def _library(tmp_path):
    """Real files for four artists: (path order, the same paths shuffled)."""
    ordered = []
    for artist in ARTISTS:
        for n in range(6):
            path = tmp_path / "music" / artist / f"{n:02d}.flac"
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(b"x")
            ordered.append(str(path))
    shuffled = ordered[:]
    random.Random(7).shuffle(shuffled)
    return ordered, shuffled


def _lines(path):
    return path.read_text().splitlines()


def test_the_list_is_sorted_ignoring_case_and_deduplicated(tmp_path):
    path = tmp_path / "corrupt.txt"
    main.write_corrupt_list(str(path), [
        "/data/b/x.flac", "/data/A/y.flac", "/data/a/z.flac", "/data/b/x.flac"])
    assert _lines(path) == ["/data/A/y.flac", "/data/a/z.flac", "/data/b/x.flac"]


def test_an_empty_list_leaves_an_empty_file(tmp_path):
    path = tmp_path / "corrupt.txt"
    path.write_text("/data/a.flac\n")
    main.write_corrupt_list(str(path), [])
    assert path.read_text() == ""


def test_a_finished_scan_leaves_the_list_in_path_order(tmp_path):
    ordered, shuffled = _library(tmp_path)
    cfg = tmp_path / "cfg"
    cfg.mkdir()
    clp = cfg / "corrupt.txt"
    clp.write_text("".join(p + "\n" for p in shuffled + shuffled[:3]))
    main._finalize_scan(str(cfg), str(clp), {}, None, {"corrupted": len(ordered)})
    assert _lines(clp) == ordered


def test_ignoring_a_file_keeps_the_rest_in_path_order(tmp_path):
    ordered, shuffled = _library(tmp_path)
    clp = tmp_path / "corrupt.txt"
    clp.write_text("".join(p + "\n" for p in shuffled))
    webui._ignore_corrupt_files(str(tmp_path), [ordered[0]])
    assert _lines(clp) == ordered[1:]


def test_auto_delete_rewrites_the_list_in_path_order(tmp_path, monkeypatch):
    ordered, shuffled = _library(tmp_path)
    clp = tmp_path / "corrupt.txt"
    clp.write_text("".join(p + "\n" for p in shuffled))
    now = time.strftime('%Y-%m-%dT%H:%M:%S')
    tracking = {p: now for p in shuffled}
    tracking[ordered[0]] = "2020-01-01T00:00:00"
    (tmp_path / "corrupt_tracking.json").write_text(json.dumps(tracking))
    monkeypatch.setattr(main, "check_audio_file", lambda p: (p, False, None, 1))
    main.run_auto_delete(str(tmp_path), str(tmp_path / "log.txt"), delete_after_days=1)
    assert _lines(clp) == ordered[1:]


def test_a_mass_delete_with_nothing_left_clears_the_list(tmp_path):
    clp = tmp_path / "corrupt.txt"
    clp.write_text("/data/gone.flac\n")
    main.run_mass_delete(["/data/gone.flac"], str(tmp_path / "log.txt"), str(tmp_path))
    assert clp.read_text() == ""
