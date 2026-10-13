"""Plain-English reason text, and the ffmpeg error cleaner it reads."""

import io
import json
import logging
import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "app"))

import main  # noqa: E402
import reason_text  # noqa: E402
import webui  # noqa: E402

INVALID = "Invalid data found when processing input"
SUBMIT = f"Error submitting packet to decoder: {INVALID}"
PROCESS = f"Error processing packet in decoder: {INVALID}"
OPEN_INVALID = (f"Error opening input: {INVALID} | Error opening input file "
                f"/music/a.x. | Error opening input files: {INVALID}")
OPEN_EOF = ("Error opening input: End of file | Error opening input file "
            "/music/a.x. | Error opening input files: End of file")

# Reasons as ffmpeg n9.0.2 produced them on deliberately damaged files.
CASES = [
    (f"Header missing | {SUBMIT} | {PROCESS}", "MP3 or MP2"),
    ("invalid new backstep -1", "MP3 or MP2"),
    (f"Failed to find two consecutive MPEG audio frames. | {OPEN_INVALID}",
     "No audio was found"),
    (f"invalid residual | decode_frame() failed | Decoding error: {INVALID} | {PROCESS}",
     "FLAC audio"),
    (f"invalid rice order: 14 blocksize 1024 | decode_frame() failed | {PROCESS}",
     "FLAC audio"),
    (f"qlevel -9 not supported, maybe buggy stream | decode_frame() failed | {PROCESS}",
     "FLAC audio"),
    ("invalid sync code | invalid frame header | decode_frame() failed | "
     "Could not open encoder before EOF | Nothing was written into output file, "
     "because at least one of its streams received no packets.",
     "No playable audio"),
    (f"moov atom not found | {OPEN_INVALID}", "missing its index"),
    (f"stream 0, contradictionary STSC and STCO | error reading header | {OPEN_INVALID}",
     "header (the part"),
    (f"CRC mismatch! | Header processing failed: {INVALID} | {OPEN_INVALID}",
     "header (the part"),
    ("CRC mismatch!", "checksum inside the file"),
    (f"channel element 0.0 is not allocated | {SUBMIT} | {PROCESS}",
     "audio data is damaged"),
    (f"Number of bands (73) exceeds limit (47). | {SUBMIT}", "audio data is damaged"),
    (f"invalid element channel count | {SUBMIT}", "audio data is damaged"),
    (f"Scalefactor (-4) out of range. | {SUBMIT}", "audio data is damaged"),
    (f"Invalid block header. | Error during demuxing: {INVALID}", "audio data is damaged"),
    ("packet_obj_size 229975794 invalid", "audio data is damaged"),
    ("corrupt input packet in stream 0", "ends too early"),
    (f"Invalid PCM packet, data has size 1 but at least a size of 4 was expected | "
     f"Decoding error: {INVALID}", "ends too early"),
    (f"Input packet size too small (581 < 1114) | {SUBMIT}", "ends too early"),
    (f"Error during demuxing: {INVALID}", "ends too early"),
    ("Cannot determine format of input 0:0 after EOF", "No audio was found"),
    (OPEN_INVALID, "No audio was found"),
    (OPEN_EOF, "ended before any audio"),
    ("File too small (512 bytes)", "almost empty"),
    ("File not accessible: [Errno 13] Permission denied: '/music/a.flac'",
     "could not read the file"),
    ("Error opening input: Permission denied | Error opening input file /music/a.flac. | "
     "Error opening input files: Permission denied", "could not read the file"),
    ("Error opening input: Input/output error | Error opening input file /music/a.flac. | "
     "Error opening input files: Input/output error", "could not read the file"),
    ("Error during demuxing: Input/output error", "could not read the file"),
    ("File ended prematurely at pos. 7340032 (0x700000) | "
     "Error during demuxing: Input/output error", "ends too early"),
    ("Truncating packet of size 4096 to 1201", "ends too early"),
    ("Stream map '0:a' matches no streams. | To ignore this, add a trailing '?' to the map. | "
     "Error opening output file -. | Error opening output files: Invalid argument",
     "No audio was found"),
    ("Non-zero exit code", "stopped without an error message"),
    ("Decode timed out (>10 minutes)", "more than 10 minutes"),
    ("Cut short: decodes 2:01 of the 4:05 its header declares", "cut short"),
    (f"Error opening input: {INVALID} | Error opening input file "
     f"/music/Header missing - CRC mismatch.mp3. | Error opening input files: {INVALID}",
     "No audio was found"),
]


@pytest.mark.parametrize("reason,phrase", CASES)
def test_each_recorded_reason_gets_its_plain_text(reason, phrase):
    assert phrase in reason_text.describe(reason)


def test_a_reason_saved_before_the_cleaner_fix_still_matches():
    legacy = (f"Header missing {SUBMIT} {PROCESS} Task finished with error code: "
              f"-1094995529 ({INVALID}) Terminating thread with return code "
              f"-1094995529 ({INVALID})")
    assert "MP3 or MP2" in reason_text.describe(legacy)


def test_an_unknown_reason_gets_the_fallback():
    assert reason_text.describe("Some new decoder message") == reason_text._FALLBACK


@pytest.mark.parametrize("reason", ["", None])
def test_no_recorded_reason_gets_no_text(reason):
    assert reason_text.describe(reason) == ""


def test_the_cleaner_strips_a_prefix_that_holds_commas():
    stderr = ("[in#0/mov,mp4,m4a,3gp,3g2,mj2 @ 0xf4c393ff4170] corrupt input packet in stream 0\n"
              "[in#0/mov,mp4,m4a,3gp,3g2,mj2 @ 0xf4c393ff4170] Task finished with error "
              f"code: -1094995529 ({INVALID})\n")
    assert main._clean_ffmpeg_errors(stderr) == "corrupt input packet in stream 0"


def test_the_cleaner_keeps_one_copy_of_each_line():
    stderr = ("[flac @ 0x1] invalid sync code\n[flac @ 0x1] invalid frame header\n"
              "[flac @ 0x2] invalid sync code\n    Last message repeated 1 times\n")
    assert main._clean_ffmpeg_errors(stderr) == "invalid sync code | invalid frame header"


def test_the_cleaner_keeps_boilerplate_when_it_is_all_ffmpeg_said():
    stderr = (f"Task finished with error code: -1094995529 ({INVALID})\n"
              f"Terminating thread with return code -1094995529 ({INVALID})\n")
    assert main._clean_ffmpeg_errors(stderr) == (
        f"Task finished with error code: -1094995529 ({INVALID}) | "
        f"Terminating thread with return code -1094995529 ({INVALID})")


@pytest.mark.parametrize("stderr", ["", "  \n", None])
def test_the_cleaner_reports_an_empty_stderr(stderr):
    assert main._clean_ffmpeg_errors(stderr) == "Non-zero exit code"


def test_the_log_shows_the_explanation_at_info(tmp_path, caplog):
    track = str(tmp_path / "a.mp3")
    with caplog.at_level(logging.INFO, logger="beatscheck"):
        main._handle_corrupt_file(track, "Header missing", "report", str(tmp_path),
                                  str(tmp_path), io.StringIO(), io.StringIO(), set(), {})
    info = [r.getMessage() for r in caplog.records if r.levelno == logging.INFO]
    assert any(reason_text.describe("Header missing") in m for m in info)


def test_the_delete_review_prints_the_explanation_then_the_reason(tmp_path, capsys):
    track = tmp_path / "a.flac"
    track.write_bytes(b"x")
    main._display_folder_files([str(track)], {str(track): {"reason": "CRC mismatch!"}})
    lines = capsys.readouterr().out.splitlines()
    assert lines[1].strip() == "-> " + reason_text.describe("CRC mismatch!")
    assert lines[2].strip() == "CRC mismatch!"


def test_the_corrupt_list_carries_the_explanation(tmp_path, monkeypatch):
    monkeypatch.setattr(webui, "_corrupt_cache", {"key": None, "value": None})
    track = str(tmp_path / "a.mp3")
    (tmp_path / "corrupt.txt").write_text(track + "\n", encoding="utf-8")
    (tmp_path / "corrupt_details.json").write_text(
        json.dumps({track: {"reason": "Header missing"}}), encoding="utf-8")
    entry = webui._read_corrupt_list(str(tmp_path))[0]
    assert entry["reason"] == "Header missing"
    assert entry["explanation"] == reason_text.describe("Header missing")
