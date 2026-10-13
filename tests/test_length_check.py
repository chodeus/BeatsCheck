"""Cut-short checks: MP3 header length, Ogg end-of-stream page, WMA recorded sizes."""

import os
import struct
import subprocess
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "app"))

import length_check  # noqa: E402
import main  # noqa: E402


# --- decoded length --------------------------------------------------------

def test_decoded_seconds_reads_the_last_progress_block():
    progress = ("out_time_us=1000000\nprogress=continue\n"
                "out_time_us=20000000\nprogress=end\n")
    assert length_check.decoded_seconds(progress) == 20.0


@pytest.mark.parametrize("progress", ["out_time_us=N/A\nprogress=end\n", "", None])
def test_decoded_seconds_is_none_without_a_reading(progress):
    assert length_check.decoded_seconds(progress) is None


# --- MP3 / MP2 -------------------------------------------------------------

# MPEG-1 Layer III, 128 kbit/s, 44.1 kHz, no padding: 417 bytes per frame.
FRAME = b"\xff\xfb\x90\x00" + bytes(413)
ID3V2 = b"ID3\x03\x00\x00\x00\x00\x00\x0a" + bytes(10)


def _mp3(tmp_path, cut=200, tail=b"", ext=".mp3"):
    """An ID3v2 tag plus 20 frames; cut=200 stops the file inside the last frame."""
    data = ID3V2 + FRAME * 20 + tail
    path = tmp_path / ("a" + ext)
    path.write_bytes(data[:len(data) - cut])
    return str(path)


@pytest.mark.parametrize("head,length", [
    (b"\xff\xfb\x90\x00", 417), (b"\xff\xfb\x92\x00", 418),   # MPEG-1 L3 128k 44.1 kHz
    (b"\xff\xf3\x80\x00", 208),                                # MPEG-2 L3 64k 22.05 kHz
    (b"\xff\xfd\xa4\x00", 576),                                # MPEG-1 L2 192k 48 kHz
    (b"\xff\xff\xc0\x00", 416),                                # MPEG-1 L1 384k 44.1 kHz
    (b"\xff\xf7\x90\x00", 312), (b"\xff\xf7\x20\x00", 104),  # MPEG-2 L1 144k/48k 22.05 kHz
    (b"\xff\xe3\x80\x00", 417), (b"\xff\xe3\x88\x00", 576),  # MPEG-2.5 L3 64k 11.025/8 kHz
    (b"\xff\xfb\x00\x00", None), (b"\xff\xfb\xf0\x00", None),  # free format, bad bitrate
    (b"\xff\xfb\x9c\x00", None), (b"\xff\xeb\x90\x00", None),  # bad rate, reserved version
])
def test_mpeg_frame_lengths(head, length):
    assert length_check._mpeg_frame_length(head) == length


def _dump(duration, extra=""):
    return ("Input #0, mp3, from '/music/a.mp3':\n  Metadata:\n"
            f"    title           : T\n{extra}"
            f"  Duration: {duration}, start: 0.025057, bitrate: 192 kb/s\n")


@pytest.fixture
def ffmpeg_says(monkeypatch):
    """Stub the header probe; returns the list of commands it was given."""
    calls = []

    def install(stderr):
        def fake_run(cmd, **kwargs):
            calls.append(cmd)
            return subprocess.CompletedProcess(cmd, 1, None, stderr)
        monkeypatch.setattr(length_check.subprocess, "run", fake_run)
        return calls
    return install


@pytest.mark.parametrize("decoded,cut", [
    (58.0, False), (57.9, True),      # 60 s header: the 2 s floor
    (240.5, False), (240.0, True),    # 245 s header: the 2% floor (4.9 s)
])
def test_mp3_is_cut_short_only_past_both_floors(ffmpeg_says, tmp_path, decoded, cut):
    declared = "00:01:00.00" if decoded < 100 else "00:04:05.00"
    ffmpeg_says(_dump(declared))
    reason = length_check.cut_short_reason(_mp3(tmp_path), decoded)
    assert (reason is not None) == cut


def test_mp3_reason_names_both_lengths(ffmpeg_says, tmp_path):
    ffmpeg_says(_dump("00:04:05.00"))
    reason = length_check.cut_short_reason(_mp3(tmp_path, ext=".mp2"), 121.3)
    assert reason == (
        "Cut short: decodes 2:01 of the 4:05 its header declares")


@pytest.mark.parametrize("cut,tail", [
    (0, b""),                       # ends on a frame boundary
    (0, b"TAG" + bytes(125)),       # ID3v1 after the last frame
    (100, b"TAG" + bytes(125)),     # stops inside a trailing tag, not a frame
])
def test_a_whole_mp3_with_an_overstated_header_is_not_flagged(ffmpeg_says, tmp_path, cut, tail):
    ffmpeg_says(_dump("00:04:05.00"))
    reason = length_check.cut_short_reason(_mp3(tmp_path, cut=cut, tail=tail), 10.0)
    assert reason is None


def test_a_whole_mp3_is_never_probed(ffmpeg_says, tmp_path):
    calls = ffmpeg_says(_dump("00:04:05.00"))
    reason = length_check.cut_short_reason(_mp3(tmp_path, cut=0), 10.0)
    assert reason is None
    assert calls == []


def test_frame_like_bytes_inside_an_id3v2_tag_are_skipped(ffmpeg_says, tmp_path):
    ffmpeg_says(_dump("00:04:05.00"))
    body = FRAME * 2 + bytes(5)
    size = bytes([0, 0, len(body) >> 7, len(body) & 0x7F])
    data = b"ID3\x03\x00\x00" + size + body + FRAME * 20
    path = tmp_path / "a.mp3"
    path.write_bytes(data[:-200])
    reason = length_check.cut_short_reason(str(path), 10.0)
    assert reason is not None


def test_a_second_tag_after_an_id3v2_footer_is_skipped(ffmpeg_says, tmp_path):
    ffmpeg_says(_dump("00:04:05.00"))
    footed = b"ID3\x04\x00\x10\x00\x00\x00\x0a" + bytes(10) + b"3DI\x04\x00\x10\x00\x00\x00\x0a"
    body = FRAME * 2 + bytes(5)
    second = b"ID3\x03\x00\x00" + bytes([0, 0, len(body) >> 7, len(body) & 0x7F]) + body
    path = tmp_path / "a.mp3"
    path.write_bytes((footed + second + FRAME * 20)[:-200])
    reason = length_check.cut_short_reason(str(path), 10.0)
    assert reason is not None


def test_a_stray_sync_before_the_audio_is_not_taken_as_the_first_frame(ffmpeg_says, tmp_path):
    ffmpeg_says(_dump("00:04:05.00"))
    data = ID3V2 + FRAME[:4] + bytes(10) + FRAME * 20
    path = tmp_path / "a.mp3"
    path.write_bytes(data[:-200])
    reason = length_check.cut_short_reason(str(path), 10.0)
    assert reason is not None


def test_a_free_format_mp3_gets_no_verdict(ffmpeg_says, tmp_path):
    ffmpeg_says(_dump("00:04:05.00"))
    path = tmp_path / "a.mp3"
    path.write_bytes((b"\xff\xfb\x00\x00" + bytes(413)) * 20)
    reason = length_check.cut_short_reason(str(path), 10.0)
    assert reason is None


def test_an_estimated_mp3_length_is_never_trusted(ffmpeg_says, tmp_path):
    ffmpeg_says("[mp3 @ 0x1] Estimating duration from bitrate, this may be "
                "inaccurate\n" + _dump("00:04:05.00"))
    reason = length_check.cut_short_reason(_mp3(tmp_path), 10.0)
    assert reason is None


def test_a_tag_cannot_fake_the_header_length(ffmpeg_says, tmp_path):
    ffmpeg_says(_dump("00:01:00.00",
                      extra="    comment         : Duration: 09:00:00.00, start\n"))
    reason = length_check.cut_short_reason(_mp3(tmp_path), 59.0)
    assert reason is None


def test_an_mp3_without_a_length_is_skipped(ffmpeg_says, tmp_path):
    ffmpeg_says(_dump("N/A"))
    reason = length_check.cut_short_reason(_mp3(tmp_path), 10.0)
    assert reason is None


def test_no_decoded_length_skips_the_probe(ffmpeg_says):
    calls = ffmpeg_says(_dump("00:04:05.00"))
    reason = length_check.cut_short_reason("/music/a.mp3", None)
    assert reason is None
    assert calls == []


def test_a_probe_timeout_is_raised_not_read_as_clean(monkeypatch, tmp_path):
    def fake_run(cmd, **kwargs):
        raise subprocess.TimeoutExpired(cmd, kwargs.get("timeout"))
    monkeypatch.setattr(length_check.subprocess, "run", fake_run)
    path = _mp3(tmp_path)
    with pytest.raises(subprocess.TimeoutExpired):
        length_check.cut_short_reason(path, 10.0)


@pytest.mark.parametrize("returncode,stderr", [
    (-9, ""),                                                    # killed
    (255, "Exiting normally, received signal 15.\n"),            # stopped
    (1, "[in#0 @ 0x1] Error opening input: Input/output error\n"
        "Error opening input file /music/a.mp3.\n"),            # read failed
])
def test_a_failed_probe_is_raised_not_read_as_clean(monkeypatch, tmp_path, returncode, stderr):
    def fake_run(cmd, **kwargs):
        return subprocess.CompletedProcess(cmd, returncode, None, stderr)
    monkeypatch.setattr(length_check.subprocess, "run", fake_run)
    path = _mp3(tmp_path)
    with pytest.raises(OSError):
        length_check.cut_short_reason(path, 10.0)


def test_other_formats_are_not_probed(ffmpeg_says):
    calls = ffmpeg_says(_dump("00:04:05.00"))
    reason = length_check.cut_short_reason("/music/a.flac", 1.0)
    assert reason is None
    assert calls == []


# --- Ogg / Opus ------------------------------------------------------------

def _ogg_page(flags, body, seq):
    """One Ogg page with a correct CRC; body must be under 255 bytes."""
    head = bytearray(b"OggS" + bytes([0, flags]) + bytes(8)
                     + struct.pack("<II", 1, seq) + bytes(4)
                     + bytes([1, len(body)]))
    page = head + body
    struct.pack_into("<I", page, 22, length_check._ogg_crc(bytes(page)))
    return bytes(page)


def _ogg_file(tmp_path, data, ext=".ogg"):
    path = tmp_path / ("a" + ext)
    path.write_bytes(data)
    return str(path)


def test_the_ogg_crc_matches_the_published_check_value():
    # CRC-32/CKSUM's check value 0x765E7680 with its final xor undone
    assert length_check._ogg_crc(b"123456789") == 0x89A1897F


def test_an_ogg_ending_on_its_eos_page_is_whole(tmp_path):
    data = _ogg_page(2, b"x" * 200, 0) + _ogg_page(0, b"y" * 200, 1) + _ogg_page(4, b"z" * 50, 2)
    reason = length_check.cut_short_reason(_ogg_file(tmp_path, data), 1.0)
    assert reason is None


def test_an_ogg_cut_mid_page_is_cut_short(tmp_path):
    data = _ogg_page(2, b"x" * 200, 0) + _ogg_page(0, b"y" * 200, 1)
    data += _ogg_page(4, b"z" * 200, 2)[:120]
    path = _ogg_file(tmp_path, data, ".opus")
    reason = length_check.cut_short_reason(path, 1.0)
    assert reason == (
        "Cut short: the Ogg stream stops partway through a page, before its end-of-stream page")


def test_an_ogg_without_eos_that_ends_on_a_page_boundary_is_not_flagged(tmp_path):
    data = _ogg_page(2, b"x" * 200, 0) + _ogg_page(0, b"y" * 200, 1)
    reason = length_check.cut_short_reason(_ogg_file(tmp_path, data), 1.0)
    assert reason is None


def test_junk_after_the_eos_page_is_not_cut_short(tmp_path):
    data = _ogg_page(2, b"x" * 200, 0) + _ogg_page(4, b"z" * 50, 1) + os.urandom(3000)
    reason = length_check.cut_short_reason(_ogg_file(tmp_path, data), 1.0)
    assert reason is None


def test_a_partial_page_after_the_eos_page_is_not_cut_short(tmp_path):
    data = _ogg_page(2, b"x" * 200, 0) + _ogg_page(4, b"z" * 50, 1)
    data += _ogg_page(0, b"y" * 200, 2)[:120]
    reason = length_check.cut_short_reason(_ogg_file(tmp_path, data), 1.0)
    assert reason is None


def test_a_page_with_an_unknown_version_is_not_trusted(tmp_path):
    odd = bytearray(_ogg_page(0, b"y" * 200, 2))
    odd[4] = 1
    odd[22:26] = bytes(4)
    struct.pack_into("<I", odd, 22, length_check._ogg_crc(bytes(odd)))
    data = _ogg_page(2, b"x" * 200, 0) + _ogg_page(4, b"z" * 50, 1) + bytes(odd)
    data += _ogg_page(0, b"w" * 200, 3)[:120]
    reason = length_check.cut_short_reason(_ogg_file(tmp_path, data), 1.0)
    assert reason is None


def test_a_page_with_a_bad_crc_is_not_taken_as_the_last_page(tmp_path):
    last = bytearray(_ogg_page(4, b"z" * 50, 2))
    last[-1] ^= 0xFF
    data = _ogg_page(2, b"x" * 200, 0) + _ogg_page(0, b"y" * 200, 1) + bytes(last)
    reason = length_check.cut_short_reason(_ogg_file(tmp_path, data), 1.0)
    assert reason is not None


def test_a_tail_with_no_ogg_page_gets_no_verdict(tmp_path):
    reason = length_check.cut_short_reason(_ogg_file(tmp_path, os.urandom(4000)), 1.0)
    assert reason is None


# --- WMA (ASF) -------------------------------------------------------------

def _asf(data_bytes=1000, flags=0, with_properties=True, packets=(100, 100),
         seconds=100.0, declared_extra=0, tail=b"", recorded_size=None):
    """ASF header + data object of 100-byte packets; File Properties records the whole size."""
    header_size = 30 + (104 if with_properties else 0)
    total = header_size + 50 + data_bytes + len(tail)
    props = (length_check._ASF_FILE_PROPERTIES + struct.pack("<Q", 104) + bytes(16)
             + struct.pack("<QQQQQQ", total if recorded_size is None else recorded_size,
                           0, 0, round((seconds + 3.1) * 1e7), 0, 3100)
             + struct.pack("<IIII", flags, *packets, 0))
    objs = props if with_properties else b""
    header = (length_check._ASF_HEADER + struct.pack("<QI", header_size, int(with_properties))
              + bytes(2) + objs)
    data = (length_check._ASF_DATA + struct.pack("<Q", 50 + data_bytes + declared_extra)
            + bytes(26) + b"\x82" * data_bytes)
    return header + data + tail


def _wma(tmp_path, blob):
    path = tmp_path / "a.wma"
    path.write_bytes(blob)
    return str(path)


def test_a_whole_wma_is_not_cut_short(tmp_path):
    reason = length_check.cut_short_reason(_wma(tmp_path, _asf()), 1.0)
    assert reason is None


def test_a_wma_cut_inside_a_packet_is_cut_short(tmp_path):
    reason = length_check.cut_short_reason(_wma(tmp_path, _asf()[:-150]), 1.0)
    assert reason == "Cut short: holds about 1:25 of the 1:40 its header declares"


@pytest.mark.parametrize("seconds,cut,flagged", [
    (60.0, 33, False), (60.0, 34, True),      # the 2 s floor (33.3 bytes)
    (245.0, 19, False), (245.0, 21, True),    # the 2% floor (20 bytes)
])
def test_a_wma_is_cut_short_only_past_both_floors(tmp_path, seconds, cut, flagged):
    blob = _asf(seconds=seconds)[:-cut]
    reason = length_check.cut_short_reason(_wma(tmp_path, blob), 1.0)
    assert (reason is not None) == flagged


def test_a_complete_wma_that_overstates_its_data_size_is_not_flagged(tmp_path):
    blob = _asf(declared_extra=100, tail=bytes(56))
    reason = length_check.cut_short_reason(_wma(tmp_path, blob), 1.0)
    assert reason is None


def test_a_whole_wma_with_a_part_packet_of_data_is_not_flagged(tmp_path):
    reason = length_check.cut_short_reason(_wma(tmp_path, _asf(data_bytes=1050)), 1.0)
    assert reason is None


def test_a_wma_short_by_whole_packets_is_not_flagged(tmp_path):
    reason = length_check.cut_short_reason(_wma(tmp_path, _asf()[:-100]), 1.0)
    assert reason is None


def test_a_wma_cut_inside_its_data_header_holds_nothing(tmp_path):
    blob = _asf()[:30 + 104 + 30]
    reason = length_check.cut_short_reason(_wma(tmp_path, blob), 1.0)
    assert reason == "Cut short: holds about 0:00 of the 1:40 its header declares"


def test_a_wma_declaring_no_packet_data_gets_no_verdict(tmp_path):
    blob = _asf(data_bytes=0)[:30 + 104 + 40]
    reason = length_check.cut_short_reason(_wma(tmp_path, blob), 1.0)
    assert reason is None


def test_a_wma_with_variable_packets_is_skipped(tmp_path):
    blob = _asf(packets=(100, 200))[:-150]
    reason = length_check.cut_short_reason(_wma(tmp_path, blob), 1.0)
    assert reason is None


def test_junk_after_the_wma_data_is_not_cut_short(tmp_path):
    reason = length_check.cut_short_reason(_wma(tmp_path, _asf() + os.urandom(500)), 1.0)
    assert reason is None


def test_a_broadcast_wma_is_skipped(tmp_path):
    blob = _asf(flags=length_check._ASF_BROADCAST)[:-150]
    reason = length_check.cut_short_reason(_wma(tmp_path, blob), 1.0)
    assert reason is None


def test_a_wma_without_file_properties_is_skipped(tmp_path):
    blob = _asf(with_properties=False)[:-150]
    reason = length_check.cut_short_reason(_wma(tmp_path, blob), 1.0)
    assert reason is None


def test_a_file_that_is_not_asf_is_skipped(tmp_path):
    reason = length_check.cut_short_reason(_wma(tmp_path, os.urandom(2000)), 1.0)
    assert reason is None


# --- check_audio_file wiring -----------------------------------------------

def _decoded(stdout="out_time_us=10000000\nprogress=end\n", returncode=0, stderr=""):
    return subprocess.CompletedProcess([], returncode, stdout, stderr)


def test_the_decode_reports_progress_on_stdout(monkeypatch):
    calls = []

    def fake_run(cmd, **kwargs):
        calls.append(cmd)
        return _decoded()
    monkeypatch.setattr(main.subprocess, "run", fake_run)
    main._ffmpeg_decode("/music/a.mp3")
    cmd = calls[0]
    assert cmd[cmd.index("-progress") + 1] == "pipe:1"
    assert cmd.index("-progress") < cmd.index("-i")


@pytest.fixture
def track(tmp_path):
    path = tmp_path / "a.mp3"
    path.write_bytes(b"\xff" * 4096)
    return str(path)


def test_a_cut_short_file_is_corrupt(track, monkeypatch):
    monkeypatch.delenv("LENGTH_CHECK", raising=False)
    monkeypatch.setattr(main, "_ffmpeg_decode", lambda p, explode=True: _decoded())
    seen = []
    monkeypatch.setattr(main.length_check, "cut_short_reason",
                        lambda p, d: seen.append(d) or "Cut short: x")
    _, corrupt, reason, _ = main.check_audio_file(track)
    assert corrupt and reason == "Cut short: x"
    assert seen == [10.0]


def test_length_check_off_skips_the_check(track, monkeypatch):
    monkeypatch.setenv("LENGTH_CHECK", "false")
    monkeypatch.setattr(main, "_ffmpeg_decode", lambda p, explode=True: _decoded())
    monkeypatch.setattr(main.length_check, "cut_short_reason",
                        lambda p, d: pytest.fail("length check ran while off"))
    _, corrupt, reason, _ = main.check_audio_file(track)
    assert not corrupt and reason is None


def test_the_cover_art_recheck_also_runs_the_length_check(track, monkeypatch):
    monkeypatch.delenv("LENGTH_CHECK", raising=False)
    picture = _decoded(returncode=1, stderr="Error parsing attached picture\n")

    def fake_decode(path, explode=True):
        return picture if explode else _decoded()
    monkeypatch.setattr(main, "_ffmpeg_decode", fake_decode)
    monkeypatch.setattr(main.length_check, "cut_short_reason", lambda p, d: "Cut short: x")
    _, corrupt, reason, _ = main.check_audio_file(track)
    assert corrupt and reason == "Cut short: x"
