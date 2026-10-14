# How it works

## The decode test

Each audio file is decoded end to end with:

```
ffmpeg -v error -err_detect explode -xerror -nostdin -progress pipe:1 -i <file> -map 0:a -f null -
```

`-err_detect explode` and `-xerror` make ffmpeg stop at the first decode error, and `-map 0:a` ensures only audio streams are tested. This catches audio stream corruption such as `Decoding error: Invalid data` and `Decode error rate exceeds maximum`. Files that fail are flagged as corrupt. A file BeatsCheck cannot read, a decode that times out, and an ffmpeg run that is killed or exits without an error message are logged as warnings instead, and checked again on the next scan. A clean `corrupt.txt` list is written for scripting or interactive deletion.

The log, the WebUI and the delete review explain each corrupt file in plain English. ffmpeg's own error text is kept in the WebUI, the delete review and `beats_check.log`.

## Cut-short files

Some files decode without an error even though their end is missing. With `length_check` on (the default), BeatsCheck also flags a file as cut short, but only when the file itself shows the cut: it stops partway through an audio frame, page or packet, which is what an interrupted download or copy leaves behind. A wrong length in the header does not flag a file on its own. A file is cut short when:

- an MP3 decodes more than 2 seconds and more than 2% shorter than the length in its Xing/Info/VBRI header, and stops partway through its last frame
- an Ogg or Opus file stops partway through a page, and the last complete page has no end-of-stream flag
- a WMA file is smaller than the file size its header records, stops partway through a packet, and is missing more than 2 seconds and more than 2% of the length its header records

Some damage still plays through without an error: an MP2, or an MP3 with no usable length header, that was cut short; damage inside a WAV or AIFF file (they carry no checksums); and flipped bits in an MP3. Files already listed in `processed.txt` get the length check on the next full rescan (`rescan --fresh`).

## Safety features

- **Setup mode by default** — container starts idle, nothing happens until you choose a mode
- **Read-only music mount** — kernel-enforced via Docker `:ro` flag (change to `rw` only to delete or move files: `delete` and `move` modes, auto-delete and WebUI deletes)
- **Strict decode checks** — `-err_detect explode` and `-xerror` stop ffmpeg at the first decode error. A run that exits cleanly but still prints an error (other than harmless tag warnings) is flagged too, so a skipped bad frame isn't missed
- **Symlink boundary check** — won't traverse symlinks that point outside the music directory
- **Graceful shutdown** — responds to SIGTERM/SIGINT, finishes in-progress files then exits cleanly
- **Resume support** — tracks already-checked files in `processed.txt` across runs (essential for multi-hour scans)
- **CPU throttled** — `nice(10)` + configurable `--cpus` to keep its CPU use down (disk reads aren't throttled)
- **10 min per-file timeout** — prevents hangs on severely corrupt files; a file that times out is logged as a warning and checked again next scan
- **Docker HEALTHCHECK** — verifies process is running and heartbeat is fresh
- **Hardlink aware** — logs link count when corrupt files have multiple hard links
- **Atomic JSON writes** — output files are written to a temp file and renamed into place, so readers never see a half-written file
- **Auto-delete safety threshold** — aborts if too many files flagged (prevents catastrophic deletion)

## Performance (3 TB library)

| Resource | Impact |
|----------|--------|
| **CPU** | 10-20% total with 6 workers (audio decode is light) |
| **RAM** | Under 500MB |
| **Disk I/O** | The bottleneck — expect 6-15 hours depending on array speed |
| **Other services** | Low CPU priority (`nice` 10), plus a `--cpus` cap if you set one; disk reads still compete with anything else on the same disks |
