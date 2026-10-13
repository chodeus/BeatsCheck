# Scanning and deleting

## 1. Scan

Start a scan in report mode with `rescan report` from the container console (or set `mode = report`). BeatsCheck scans your library, writes its results and waits for the next interval:

```
BeatsCheck v1.0.0 starting
2026-01-15 10:00:00 | INFO     | BeatsCheck v1.0.0
2026-01-15 10:00:00 | INFO     |   Mode:    report
2026-01-15 10:00:00 | INFO     |   Workers: 6
2026-01-15 10:00:00 | INFO     |   Library: 98432 files (2.8 TB)
2026-01-15 10:00:00 | INFO     |   To scan: 98432 files (0 already processed)
2026-01-15 10:05:00 | INFO     | [5%] 5000/98432 checked, 2 corrupt, ETA 3h42m
```

With `run_interval` set (e.g. `168` for weekly), the container sleeps for the interval after a scan, then scans again. Later runs check only files not already in `processed.txt` (resume support), so a file replaced under the same name is re-checked only by a full rescan (`rescan --fresh`). With `run_interval = 0` (the default), it scans once and stays idle until the next `rescan`.

## 2. Review the results

Check the logs on the host:

```bash
# Clean list of corrupt file paths
cat /path/to/config/corrupt.txt

# Full log with error details
cat /path/to/config/beats_check.log
```

The WebUI's **Corrupt Files** page shows the same list, with a plain-English explanation for each file.

## 3. Delete corrupt files

**Important:** the music mount must be `rw` (not `ro`) for BeatsCheck to delete anything. With Lidarr set up, deletes go through Lidarr; see [Lidarr integration](lidarr.md).

Three options — pick what suits your workflow.

### Auto-delete after X days

Set `delete_after = 7` (or `DELETE_AFTER=7`). Corrupt files are deleted automatically 7 days after they were first detected, which gives you time to review `corrupt.txt` before anything is removed. `max_auto_delete` aborts the run if too many files would go.

### Interactive delete

From the container console or via `docker exec`:

```bash
# Inside container console
delete

# Or from the host
docker exec -it beatscheck delete
```

You'll see a menu:

```
Found 5 corrupt files across 3 folders (142.5 MB)

  [a] delete ALL corrupt files now
  [i] interactive (decide per folder)
  [q] quit

  Choice: i

  [1/3] /data/Artist Name/Album Name/
           track01.flac (45.2 MB)
             -> Part of the FLAC audio is damaged. Common causes: the download stopped early, junk data was added to the end of the file, or the file was damaged on disk.
                invalid residual | decode_frame() failed | Decoding error: Invalid data found when processing input | Error processing packet in decoder: Invalid data found when processing input
           track05.flac (512 B)
             -> The file is almost empty, so the download or copy most likely failed.
                File too small (512 bytes)
           (2 corrupt / 12 total files in folder)
           Action? [y/f/n/a/q]
```

Interactive options:
- `y` — delete entire folder (nuke the album, re-download later)
- `f` — delete just the corrupt files, keep the rest
- `n` — skip this folder
- `a` — delete all remaining folders without asking
- `q` — quit

After deletion, `corrupt.txt` is updated to remove handled entries.

### From the WebUI

On **Corrupt Files**, select files or albums and click **Delete**. Each file is checked against `corrupt.txt` and the music directory. For an album, at least one flagged file must be in that album's folder before the whole folder is removed. Files Lidarr tracks are deleted through Lidarr; BeatsCheck removes the rest itself, so mount the music folder `rw`. Don't feed `corrupt.txt` to `rm` on the host: it lists paths as the container sees them (`/data/...`), not host paths.

## 4. Rescan

Trigger a rescan or change modes without restarting the container:

```bash
# Rescan with current mode
rescan

# Change mode and scan (works from setup mode too)
rescan report
rescan move

# Full rescan (the resume cache is cleared when the scan starts)
rescan --fresh report

# From the host
docker exec beatscheck rescan report
```
