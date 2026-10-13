"""Cut-short checks: the file stops inside a frame, page or packet, before the end its header or stream declares."""

import os
import re
import struct
import subprocess

_MIN_SHORTFALL_S = 2.0
_MIN_SHORTFALL_FRACTION = 0.02
_PROBE_TIMEOUT_S = 60
_OGG_TAIL = 131072  # two maximum-size Ogg pages
_OGG_EOS = 0x04
_ASF_HEADER = bytes.fromhex("3026b2758e66cf11a6d900aa0062ce6c")
_ASF_DATA = bytes.fromhex("3626b2758e66cf11a6d900aa0062ce6c")
_ASF_FILE_PROPERTIES = bytes.fromhex("a1dcab8c47a9cf118ee400c00c205365")
_ASF_BROADCAST = 0x01
_ASF_DATA_HEADER = 50
# Anchored: tag values and the file name print before this line in ffmpeg's dump.
_DURATION_RE = re.compile(r"^  Duration: (\d+):(\d\d):(\d\d(?:\.\d+)?),", re.M)
_OUT_TIME_RE = re.compile(r"^out_time_us=(\d+)$", re.M)
_MPEG_SYNC_SEARCH = 65536
# kbit/s by (is MPEG-1, layer); index 0 is free format and 15 is invalid.
_MPEG_BITRATES = {
    (True, 1): (0, 32, 64, 96, 128, 160, 192, 224, 256, 288, 320, 352, 384, 416, 448),
    (True, 2): (0, 32, 48, 56, 64, 80, 96, 112, 128, 160, 192, 224, 256, 320, 384),
    (True, 3): (0, 32, 40, 48, 56, 64, 80, 96, 112, 128, 160, 192, 224, 256, 320),
    (False, 1): (0, 32, 48, 56, 64, 80, 96, 112, 128, 144, 160, 176, 192, 224, 256),
    (False, 2): (0, 8, 16, 24, 32, 40, 48, 56, 64, 80, 96, 112, 128, 144, 160),
    (False, 3): (0, 8, 16, 24, 32, 40, 48, 56, 64, 80, 96, 112, 128, 144, 160),
}
_MPEG_RATES = {3: (44100, 48000, 32000), 2: (22050, 24000, 16000), 0: (11025, 12000, 8000)}


def _crc_table():
    table = []
    for n in range(256):
        r = n << 24
        for _ in range(8):
            r = (r << 1) ^ 0x04C11DB7 if r & 0x80000000 else r << 1
        table.append(r & 0xFFFFFFFF)
    return tuple(table)


_CRC_TABLE = _crc_table()


def decoded_seconds(progress):
    """Decoded length from ffmpeg -progress output, or None when it reported none."""
    values = _OUT_TIME_RE.findall(progress or "")
    return int(values[-1]) / 1e6 if values else None


def cut_short_reason(path, decoded):
    """Why a cleanly decoded file was cut short, or None."""
    ext = os.path.splitext(path)[1].lower()
    if ext in (".mp3", ".mp2"):
        return None if decoded is None else _mpeg_reason(path, decoded)
    if ext in (".ogg", ".opus"):
        return _ogg_reason(path)
    if ext == ".wma":
        return _asf_reason(path)
    return None


def _counts_as_cut(declared, present):
    """True when present falls short of declared by more than max(2 s, 2%)."""
    return declared - present > max(_MIN_SHORTFALL_S, declared * _MIN_SHORTFALL_FRACTION)


def _mpeg_reason(path, decoded):
    # Local walk first: only a file that ends mid-frame needs the ffmpeg probe.
    if _mpeg_ends_mid_frame(path) is not True:
        return None
    declared = _mpeg_header_seconds(path)
    if declared is None or not _counts_as_cut(declared, decoded):
        return None
    return (f"Cut short: decodes {_clock(decoded)} of the {_clock(declared)} "
            "its header declares")


def _mpeg_ends_mid_frame(path):
    """True when the last MPEG frame runs past EOF; False on a clean end; None when unsure."""
    with open(path, "rb") as f:
        size = os.fstat(f.fileno()).st_size
        pos = _first_mpeg_frame(f, _id3v2_end(f))
        if pos is None:
            return None
        while True:
            f.seek(pos)
            length = _mpeg_frame_length(f.read(4))
            if length is None:
                return None
            if pos + length >= size:
                return pos + length > size
            pos += length


def _id3v2_end(f):
    """Offset just past any ID3v2 tags at the start of the file."""
    pos = 0
    while True:
        f.seek(pos)
        head = f.read(10)
        if len(head) < 10 or head[:3] != b"ID3" or any(b & 0x80 for b in head[6:10]):
            return pos
        size = (head[6] << 21) | (head[7] << 14) | (head[8] << 7) | head[9]
        pos += 10 + size + (10 if head[5] & 0x10 else 0)


def _first_mpeg_frame(f, start):
    """Offset of the first frame header that a second valid header follows, or None."""
    f.seek(start)
    buf = f.read(_MPEG_SYNC_SEARCH)
    for i in range(len(buf) - 3):
        length = _mpeg_frame_length(buf[i:i + 4])
        if length is None:
            continue
        f.seek(start + i + length)
        if _mpeg_frame_length(f.read(4)) is not None:
            return start + i
    return None


def _mpeg_frame_length(head):
    """Byte length of the frame whose 4-byte header is head, or None when it isn't one."""
    if len(head) < 4 or head[0] != 0xFF or head[1] & 0xE0 != 0xE0:
        return None
    version, layer_bits = (head[1] >> 3) & 3, (head[1] >> 1) & 3
    rate_idx, sample_idx, pad = head[2] >> 4, (head[2] >> 2) & 3, (head[2] >> 1) & 1
    if version == 1 or layer_bits == 0 or rate_idx in (0, 15) or sample_idx == 3:
        return None
    layer = 4 - layer_bits
    bitrate = _MPEG_BITRATES[(version == 3, layer)][rate_idx] * 1000
    sample_rate = _MPEG_RATES[version][sample_idx]
    if layer == 1:
        return (12 * bitrate // sample_rate + pad) * 4
    per_frame = 72 if layer == 3 and version != 3 else 144
    return per_frame * bitrate // sample_rate + pad


def _mpeg_header_seconds(path):
    """Header length as ffmpeg reports it, or None when ffmpeg only estimates it."""
    result = subprocess.run(
        ["ffmpeg", "-hide_banner", "-nostdin", "-i", path],
        stdout=subprocess.DEVNULL, stderr=subprocess.PIPE,
        encoding="utf-8", errors="replace", timeout=_PROBE_TIMEOUT_S)
    # The decode just opened this file, so a failed probe is transient, never clean.
    if result.returncode not in (0, 1) or "Error opening input" in result.stderr:
        raise OSError(f"ffmpeg could not probe {path} (exit {result.returncode})")
    if "Estimating duration from bitrate" in result.stderr:
        return None
    match = _DURATION_RE.search(result.stderr)
    if not match:
        return None
    hours, minutes, seconds = match.groups()
    return int(hours) * 3600 + int(minutes) * 60 + float(seconds)


def _clock(seconds):
    minutes, secs = divmod(int(round(seconds)), 60)
    hours, minutes = divmod(minutes, 60)
    return f"{hours}:{minutes:02d}:{secs:02d}" if hours else f"{minutes}:{secs:02d}"


def _ogg_reason(path):
    last = _last_ogg_page(path)
    if last is None:
        return None
    flags, rest = last
    # A stream that never set EOS ends on a page boundary; a cut copy ends inside a page.
    if flags & _OGG_EOS or not rest.startswith(b"OggS"):
        return None
    return "Cut short: the Ogg stream stops partway through a page, before its end-of-stream page"


def _last_ogg_page(path):
    """(flags, bytes after it) of the last complete, CRC-valid Ogg page in the tail."""
    with open(path, "rb") as f:
        f.seek(0, os.SEEK_END)
        f.seek(max(0, f.tell() - _OGG_TAIL))
        tail = f.read()
    end = len(tail)
    while True:
        start = tail.rfind(b"OggS", 0, end)
        if start < 0:
            return None
        page_end = _ogg_page_end(tail, start)
        if page_end is not None:
            return tail[start + 5], tail[page_end:]
        end = start


def _ogg_page_end(buf, start):
    """End offset of the page at buf[start] when it is complete and its CRC matches, else None."""
    if len(buf) < start + 27 or buf[start + 4] != 0:
        return None
    seg_end = start + 27 + buf[start + 26]
    if len(buf) < seg_end:
        return None
    page_end = seg_end + sum(buf[start + 27:seg_end])
    if len(buf) < page_end:
        return None
    page = bytearray(buf[start:page_end])
    stored = struct.unpack_from("<I", page, 22)[0]
    page[22:26] = bytes(4)
    return page_end if _ogg_crc(page) == stored else None


def _ogg_crc(data):
    """Ogg page CRC-32: poly 0x04C11DB7, not reflected, zero init and final xor."""
    crc = 0
    for byte in data:
        crc = ((crc << 8) & 0xFFFFFFFF) ^ _CRC_TABLE[(crc >> 24) ^ byte]
    return crc


def _asf_reason(path):
    with open(path, "rb") as f:
        head = f.read(30)
        if len(head) < 30 or head[:16] != _ASF_HEADER:
            return None
        header_size, count = struct.unpack_from("<QI", head, 16)
        props = _asf_file_properties(f, header_size, count)
        f.seek(header_size)
        data = f.read(24)
        file_size = os.fstat(f.fileno()).st_size
    if props is None or len(data) < 24 or data[:16] != _ASF_DATA:
        return None
    recorded_size, play, preroll, flags, min_packet, max_packet = props
    # Broadcast files leave the sizes unset; variable packets give no boundary to test.
    if flags & _ASF_BROADCAST or min_packet != max_packet or min_packet == 0:
        return None
    packet_bytes = struct.unpack_from("<Q", data, 16)[0] - _ASF_DATA_HEADER
    missing = header_size + _ASF_DATA_HEADER + packet_bytes - file_size
    # The recorded file size clears a complete file whose data size is overstated.
    if recorded_size <= file_size or packet_bytes <= 0:
        return None
    if (file_size - header_size - _ASF_DATA_HEADER) % min_packet == 0:
        return None
    declared = play / 1e7 - preroll / 1000
    present = max(0.0, declared * (1 - missing / packet_bytes))
    if not _counts_as_cut(declared, present):
        return None
    return (f"Cut short: holds about {_clock(present)} of the {_clock(declared)} "
            "its header declares")


def _asf_file_properties(f, header_size, count):
    """(file size, play duration, preroll, flags, min packet, max packet) from File Properties, or None."""
    pos = 30
    for _ in range(count):
        f.seek(pos)
        obj = f.read(24)
        if len(obj) < 24:
            return None
        if obj[:16] == _ASF_FILE_PROPERTIES:
            f.seek(pos + 40)
            raw = f.read(60)
            return struct.unpack("<Q16xQ8xQIII", raw) if len(raw) == 60 else None
        size = struct.unpack_from("<Q", obj, 16)[0]
        if size < 24 or pos + size >= header_size:
            return None
        pos += size
    return None
