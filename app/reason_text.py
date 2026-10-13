"""Plain-English text for the reasons BeatsCheck records against a corrupt file."""

# First match wins. Markers are ffmpeg n9.0.2 text; the generic open-failure
# groups go last because ffmpeg prints them after the specific cause.
_NO_AUDIO = ("No audio was found in this file. It may not be a music file at "
             "all (for example, a saved error page with a music file name), "
             "or its start is damaged.")
_ENDS_EARLY = ("The file ends too early or breaks partway through. Most often "
               "the download or copy stopped before it finished.")
_REASON_TEXT = (
    (("File too small",),
     "The file is almost empty, so the download or copy most likely failed."),
    # Matroska reports a truncated file as an I/O error after this line.
    (("File ended prematurely",), _ENDS_EARLY),
    # Read failures, timeouts and stopped runs are now warnings; older corrupt lists hold them.
    (("File not accessible", "Permission denied", "Input/output error",
      "No such file or directory"),
     "BeatsCheck could not read the file (a permissions or disk problem). "
     "The audio may be fine: fix access, then run a fresh scan "
     "(rescan --fresh)."),
    (("Decode timed out",),
     "The check ran for more than 10 minutes and was stopped. "
     "The file may be fine: run a fresh scan (rescan --fresh) to check it "
     "again."),
    (("Non-zero exit code",),
     "ffmpeg stopped without an error message. It may have crashed or run "
     "out of memory. The file may be fine: run a fresh scan "
     "(rescan --fresh) to check it again."),
    (("Cut short:",),
     "The file is cut short: the end of the track is missing. "
     "The download or copy most likely stopped before it finished."),
    (("moov atom not found",),
     "The M4A file is missing its index (the part that tells players where "
     "the audio is). The download or copy most likely stopped before the "
     "end. It will not play."),
    (("Nothing was written into output file",),
     "No playable audio was found. The start of the file is damaged."),
    (("Failed to find two consecutive MPEG audio frames",
      "matches no streams"), _NO_AUDIO),
    (("error reading header", "contradictionary STSC and STCO",
      "Header processing failed"),
     "The header (the part at the start that describes the audio) is "
     "damaged, so the audio cannot be read."),
    (("CRC mismatch",),
     "A checksum inside the file does not match the audio. Part of the file "
     "changed after it was made, on disk or in transfer."),
    (("Header missing", "invalid new backstep"),
     "Part of the MP3 or MP2 audio is not valid: some bytes in the middle "
     "or at the end are damaged or junk. Players usually skip the bad part, "
     "often with a click or a jump."),
    (("invalid sync code", "invalid frame header", "invalid residual",
      "invalid coding type", "invalid rice order", "decode_frame() failed"),
     "Part of the FLAC audio is damaged. Common causes: the download stopped "
     "early, junk data was added to the end of the file, or the file was "
     "damaged on disk."),
    (("Number of bands", "is not allocated", "Input buffer exhausted",
      "spectral RLE", "bits left", "Too few bits", "Invalid block header",
      "Block size", "packet_obj_size", "invalid element channel count",
      "Scalefactor ("),
     "Part of the audio data is damaged and cannot be decoded. Players may "
     "skip, click or stop at that point."),
    (("corrupt input packet", "Invalid PCM packet",
      "Input packet size too small", "Truncating packet of size",
      "Error during demuxing"), _ENDS_EARLY),
    (("Cannot determine format of input",
      "Error opening input: Invalid data"), _NO_AUDIO),
    (("Error opening input: End of file",),
     "The file ended before any audio was found. It is cut short, or it is "
     "not a music file."),
)
_FALLBACK = ("ffmpeg could not decode the whole file, so part of the audio "
             "is most likely damaged.")
# ffmpeg names the file after this; matching past it would read the file name.
_PATH_LINE = "Error opening input file "


def describe(reason):
    """Plain-English explanation of a recorded reason; '' when none was recorded."""
    if not reason:
        return ""
    head = reason.split(_PATH_LINE, 1)[0]
    for markers, text in _REASON_TEXT:
        if any(m in head for m in markers):
            return text
    return _FALLBACK
