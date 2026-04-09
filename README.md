# BeatsCheck

Audio file integrity checker that performs a full decode test using ffmpeg. Catches audio stream corruption like `Decoding error: Invalid data` and `Decode error rate exceeds maximum`.

**Default mode is report-only. No files are moved or deleted until you explicitly choose to.**

## Supported Formats

FLAC, MP3, M4A, OGG, Opus, WAV, WMA, AAC, AIFF, APE, WavPack, ALAC, M4B, M4P, MP2, MPC, DSF, DFF

## How It Works

Each audio file is decoded end-to-end with `ffmpeg -v error -xerror -nostdin -i <file> -map 0:a -f null -`. The `-xerror` flag makes ffmpeg exit immediately on any decode error and `-map 0:a` ensures only audio streams are tested. Files that fail are flagged as corrupt. A clean `corrupt.txt` list is written for scripting or interactive deletion.

## Modes

| Mode | What it does | Music mount | Safe? |
|------|-------------|-------------|-------|
| `report` | Scan and log only, writes `corrupt.txt` | `ro` | Yes (default) |
| `delete` | Interactive — prompts per album folder to delete | **`rw`** | You choose |
| `move` | Auto-move corrupt files to quarantine folder | **`rw`** | Destructive |

## Recommended Workflow

### 1. Scan (report mode)

Start the container normally. It scans your library, writes results, and waits for next interval:

```
BeatsCheck v2.0.0 starting
2025-01-15 10:00:00 | INFO     | BeatsCheck v2.0.0
2025-01-15 10:00:00 | INFO     |   Mode:    report
2025-01-15 10:00:00 | INFO     |   Workers: 6
2025-01-15 10:00:00 | INFO     |   Library: 98432 files (2.8 TB)
2025-01-15 10:00:00 | INFO     |   To scan: 98432 files (0 already processed)
2025-01-15 10:05:00 | INFO     | [5%] 5000/98432 checked, 2 corrupt, ETA 3h42m
```

### 2. Review results

Check the logs on the host:

```bash
# Clean list of corrupt file paths
cat /path/to/config/corrupt.txt

# Full log with error details
cat /path/to/config/beats_check.log
```

### 3. Delete corrupt files

Exec into the running container:

```bash
docker exec -it beatscheck /app/delete.sh
```

You'll see a menu:

```
Found 5 corrupt files across 3 folders (142.5 MB)

  [a] delete ALL corrupt files now
  [i] interactive (decide per folder)
  [q] quit

  Choice: i

  [1/3] /music/Artist Name/Album Name/
           track01.flac (45.2 MB)
             -> [flac @ 0x...] invalid residual | decode frame failed
           track05.flac (512 B)
             -> File too small (512 bytes)
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

## Safety Features

- **Report mode by default** — nothing is moved or deleted until you opt in
- **Read-only music mount** — kernel-enforced via Docker `:ro` flag (change to `rw` only for `delete`/`move` modes)
- **`-xerror` flag** — fail-fast on decode errors, no false positives from partial decodes
- **Symlink boundary check** — won't traverse symlinks that point outside the music directory
- **Graceful shutdown** — responds to SIGTERM/SIGINT, finishes in-progress files then exits cleanly
- **Resume support** — tracks already-checked files in `processed.txt` across runs (essential for multi-hour scans)
- **CPU throttled** — `nice(10)` + configurable `--cpus` to avoid impacting other services
- **10 min per-file timeout** — prevents hangs on severely corrupt files
- **Docker HEALTHCHECK** — verifies process is running and heartbeat is fresh
- **Hardlink aware** — logs link count when corrupt files have multiple hard links
- **Atomic JSON writes** — crash-safe output files (no corruption on power loss)
- **Auto-delete safety threshold** — aborts if too many files flagged (prevents catastrophic deletion)

## Configuration

| Env Var | Default | Description |
|---------|---------|-------------|
| `MUSIC_DIR` | `/music` | Path to music library inside container |
| `OUTPUT_DIR` | `/corrupted` | Quarantine destination (move mode only) |
| `CONFIG_DIR` | `/config` | Persistent directory for logs, corrupt.txt, and tracking data |
| `MODE` | `report` | `report`, `delete`, or `move` |
| `WORKERS` | `4` | Parallel ffmpeg decode workers. 2 = conservative, 4 = balanced, 8+ = fast |
| `RUN_INTERVAL` | `0` | Hours between scans. `0` = run once and exit. `168` = weekly. `24` = daily |
| `DELETE_AFTER` | `0` | Days before corrupt files are auto-deleted. `0` = never (manual only). `7` = 7 day review window |
| `MAX_AUTO_DELETE` | `50` | Safety threshold — abort auto-delete if more than this many files would be removed. `0` = no limit |
| `MIN_FILE_AGE` | `30` | Skip files modified within this many minutes. Prevents flagging active downloads |
| `MAX_LOG_MB` | `50` | Rotate log and do fresh full scan when log exceeds this size. `0` = never rotate |
| `LOG_LEVEL` | `INFO` | Logging verbosity: `DEBUG`, `INFO`, `WARNING`, `ERROR` |
| `PUID` | `99` | User ID for file ownership |
| `PGID` | `100` | Group ID for file ownership |
| `TZ` | `UTC` | Timezone for log timestamps. Auto-detected if `/etc/localtime` is bind-mounted |
| `UMASK` | `002` | File creation mask |

## Docker Usage

### Docker Compose (Recommended)

A complete `docker-compose.yml` is included in the repository:

```bash
# Copy and edit the compose file
cp docker-compose.yml /path/to/your/docker-compose.yml
# Edit paths and settings, then:
docker compose up -d
```

### Docker Run

```bash
# Scan (report mode, run once)
docker run --rm \
  -v /path/to/music:/music:ro \
  -v /path/to/config:/config \
  -e MODE=report \
  -e WORKERS=6 \
  --cpus=2 \
  ghcr.io/chodeus/beatscheck:latest

# Daemon mode (weekly scans, stays running)
docker run -d --restart unless-stopped \
  --name beatscheck \
  -v /path/to/music:/music:ro \
  -v /path/to/config:/config \
  -e MODE=report \
  -e RUN_INTERVAL=168 \
  -e WORKERS=6 \
  ghcr.io/chodeus/beatscheck:latest

# Interactive delete (into running container)
docker exec -it beatscheck /app/delete.sh
```

## Installation on Unraid

### Pull from GHCR

```bash
docker pull ghcr.io/chodeus/beatscheck:latest
```

### Install the Template

```bash
wget -O /boot/config/plugins/dockerMan/templates-user/my-BeatsCheck.xml \
  https://raw.githubusercontent.com/chodeus/BeatsCheck/main/beats-check.xml
```

### Add the Container

1. Go to **Docker** tab in Unraid web UI
2. Click **Add Container**
3. Select **BeatsCheck** from the template dropdown
4. Verify paths match your setup:
   - **Music Library**: `/mnt/user/data/media/music` (or your music directory)
   - **Config**: `/mnt/user/appdata/beatscheck`
5. Leave **Mode** as `report` and **Workers** as `4`
6. Click **Apply**

### How the Container Runs

**With `RUN_INTERVAL` set (e.g., 168 for weekly):**
The container stays running permanently. It scans your library, sleeps for the interval, then scans again. Only new/changed files are checked on subsequent runs (resume support). The container shows as **running** in the Docker tab — this is the recommended setup.

**With `RUN_INTERVAL=0` (default):**
The container scans once and exits. You'd need to manually start it or use User Scripts to schedule it.

### Deleting Corrupt Files

Three options — pick what suits your workflow:

**Option 1: Auto-delete after X days (fully automated)**

Set `DELETE_AFTER=7` in the container config. Corrupt files are automatically deleted 7 days after being first detected. This gives you time to review `corrupt.txt` before anything is removed.

**Option 2: Interactive delete (on demand)**

While the container is running (daemon mode), open the Unraid **terminal** and type:

```bash
docker exec -it BeatsCheck /app/delete.sh
```

**Option 3: Manual delete from corrupt.txt**

```bash
cat /mnt/user/appdata/beatscheck/corrupt.txt
while IFS= read -r f; do rm -v "$f"; done < /mnt/user/appdata/beatscheck/corrupt.txt
```

### Unraid Notifications (Optional)

Use a **User Scripts** wrapper to get notified after scans:

```bash
#!/bin/bash
LOG_DIR="/mnt/user/appdata/beatscheck"

if [ -f "$LOG_DIR/summary.json" ]; then
    CHECKED=$(jq -r '.files_checked' "$LOG_DIR/summary.json")
    CORRUPT=$(jq -r '.corrupted' "$LOG_DIR/summary.json")
    DURATION=$(jq -r '.duration' "$LOG_DIR/summary.json")
    SIZE=$(jq -r '.library_size_human' "$LOG_DIR/summary.json")

    if [ "$CORRUPT" -gt 0 ]; then
        /usr/local/emhttp/webGui/scripts/notify \
            -i warning -s "BeatsCheck" \
            -d "Scan complete: $CHECKED files ($SIZE), $CORRUPT corrupt found ($DURATION)"
    else
        /usr/local/emhttp/webGui/scripts/notify \
            -i normal -s "BeatsCheck" \
            -d "Scan complete: $CHECKED files ($SIZE), no corruption ($DURATION)"
    fi
fi
```

## Standalone Usage (No Docker)

Requires Python 3.9+ and ffmpeg installed.

```bash
MODE=report WORKERS=6 python3 beats_check.py /path/to/music /path/to/quarantine /path/to/config/beats_check.log
```

The third argument is the log file path. All state files (`processed.txt`, `corrupt.txt`, etc.) are written to the same directory as the log file. Unix-only (requires `fcntl`).

## Output Files (in /config)

| File | Contents |
|------|----------|
| `beats_check.log` | Full scan log — errors, moves, deletes, and scan summaries |
| `beats_check.log.1` `.2` `.3` | Previous logs after rotation (last 3 kept) |
| `processed.txt` | Resume cache — one checked file path per line. Rotated alongside the log |
| `corrupt.txt` | One corrupt file path per line (deduplicated) — for scripting or delete mode |
| `corrupt_details.json` | Path-to-error-reason mapping — shown during interactive delete |
| `corrupt_tracking.json` | Path-to-first-seen timestamps — used by `DELETE_AFTER` auto-delete |
| `summary.json` | Machine-readable scan results for notification scripts |
| `.scanning` | Lock file (exists only during active scans, uses `flock`) |
| `.heartbeat` | Timestamp updated per-file during scans — used by Docker healthcheck |

### Log Rotation

When the log exceeds 50 MB (`MAX_LOG_MB=50`), it's rotated to `beats_check.log.1` (keeping up to 3 old copies). The resume cache (`processed.txt`) is rotated alongside it, so the next scan does a fresh full re-check. Set `MAX_LOG_MB=0` to disable rotation.

For a 100K file library, expect ~10 MB per full scan. With daemon mode, subsequent scans only log corrupt files so growth is slow.

## Performance (3TB Library)

| Resource | Impact |
|----------|--------|
| **CPU** | 10-20% total with 6 workers (audio decode is light) |
| **RAM** | Under 500MB |
| **Disk I/O** | The bottleneck — expect 6-15 hours depending on array speed |
| **Other services** | Unaffected (low priority + CPU cap) |

## Updating

```bash
docker pull ghcr.io/chodeus/beatscheck:latest
```

Then restart the container.

## Security

See [SECURITY.md](.github/SECURITY.md) for the security policy, vulnerability reporting, and container security measures.

## License

[MIT](LICENSE)
