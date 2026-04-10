# BeatsCheck

> **This project is in active development.** Features may change between releases. Please report issues on [GitHub](https://github.com/chodeus/BeatsCheck/issues).

Audio file integrity checker that performs a full decode test using ffmpeg. Catches audio stream corruption like `Decoding error: Invalid data` and `Decode error rate exceeds maximum`.

**Default mode is report-only. No files are moved or deleted until you explicitly choose to.**

## Supported Formats

FLAC, MP3, M4A, OGG, Opus, WAV, WMA, AAC, AIFF, APE, WavPack, ALAC, M4B, M4P, MP2, MPC, DSF, DFF

## How It Works

Each audio file is decoded end-to-end with `ffmpeg -v error -xerror -nostdin -i <file> -map 0:a -f null -`. The `-xerror` flag makes ffmpeg exit immediately on any decode error and `-map 0:a` ensures only audio streams are tested. Files that fail are flagged as corrupt. A clean `corrupt.txt` list is written for scripting or interactive deletion.

## Modes

| Mode | What it does | Music mount | Safe? |
|------|-------------|-------------|-------|
| `setup` | Container starts idle, no scanning (default) | `ro` | Yes |
| `report` | Scan and log only, writes `corrupt.txt` | `ro` | Yes |
| `delete` | Interactive — prompts per album folder to delete | **`rw`** | You choose |
| `move` | Auto-move corrupt files to quarantine folder | **`rw`** | Destructive |

You can change modes without restarting — see [Rescan](#4-rescan) below.

## Recommended Workflow

### 1. Scan (report mode)

Start the container normally. It scans your library, writes results, and waits for next interval:

```
BeatsCheck v1.0.0 starting
2026-01-15 10:00:00 | INFO     | BeatsCheck v1.0.0
2026-01-15 10:00:00 | INFO     |   Mode:    report
2026-01-15 10:00:00 | INFO     |   Workers: 6
2026-01-15 10:00:00 | INFO     |   Library: 98432 files (2.8 TB)
2026-01-15 10:00:00 | INFO     |   To scan: 98432 files (0 already processed)
2026-01-15 10:05:00 | INFO     | [5%] 5000/98432 checked, 2 corrupt, ETA 3h42m
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

**Important:** The music mount must be `rw` (not `ro`) for delete mode to work.

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

  [1/3] /music/Artist Name/Album Name/
           track01.flac (45.2 MB)
             -> invalid residual | decode_frame() failed | Decoding error: Invalid data found
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

### 4. Rescan

Trigger a rescan or change modes without restarting the container:

```bash
# Rescan with current mode
rescan

# Change mode and scan (works from setup mode too)
rescan report
rescan move

# Full rescan (clear resume cache)
rescan --fresh report

# From the host
docker exec beatscheck rescan report
```

## Safety Features

- **Setup mode by default** — container starts idle, nothing happens until you choose a mode
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
| `MODE` | `setup` | `setup` (idle), `report`, `delete`, or `move`. Can be changed at runtime via `rescan` |
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
| `LIDARR_URL` | *(empty)* | Lidarr instance URL (e.g. `http://lidarr:8686`). Enables Lidarr API integration |
| `LIDARR_API_KEY` | *(empty)* | Lidarr API key (Settings → General in Lidarr). Also reads from `/run/secrets/lidarr_api_key` |
| `LIDARR_SEARCH` | `false` | Queue album search after auto-delete so Lidarr re-downloads. Processes 5 albums/hour during idle |
| `LIDARR_BLOCKLIST` | `false` | Blocklist the release in Lidarr before deleting, preventing re-download of the same corrupt copy |

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

# Interactive delete (requires rw music mount)
docker exec -it beatscheck delete

# Trigger a rescan
docker exec beatscheck rescan
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
5. Configure your settings (paths, workers, Lidarr if needed)
6. Click **Apply** — the container starts idle in setup mode
7. Open the container console and type `rescan report` to start scanning

### How the Container Runs

The container defaults to **setup mode** — it starts idle and waits for you to trigger a scan. This lets you configure everything before any scanning begins.

**To start scanning:** run `rescan report` from the container console or set `MODE=report` and restart.

**With `RUN_INTERVAL` set (e.g., 168 for weekly):**
After the first scan, the container sleeps for the interval, then scans again. Only new/changed files are checked on subsequent runs (resume support).

**With `RUN_INTERVAL=0` (default):**
The container scans once and stays idle. Use `rescan` to trigger another scan without restarting.

### Deleting Corrupt Files

Three options — pick what suits your workflow:

**Option 1: Auto-delete after X days (fully automated)**

Set `DELETE_AFTER=7` in the container config. Corrupt files are automatically deleted 7 days after being first detected. This gives you time to review `corrupt.txt` before anything is removed.

**Option 2: Interactive delete (on demand)**

Change the music mount to `rw`, then from the Unraid **terminal**:

```bash
docker exec -it BeatsCheck delete
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
| `search_queue.json` | Pending Lidarr album search queue — drained during idle (5/hour) |
| `.scanning` | Lock file (exists only during active scans, uses `flock`) |
| `.heartbeat` | Timestamp updated during scans and idle — used by Docker healthcheck |

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

## Lidarr Integration

When `LIDARR_URL` and `LIDARR_API_KEY` are set, BeatsCheck uses the Lidarr API to delete corrupt files instead of deleting them directly. Track file records are removed via the API while albums stay monitored — so Lidarr marks them as missing and can re-download.

**Auto-delete flow (`DELETE_AFTER`):**

1. Scan finds corrupt files → added to `corrupt.txt` with first-seen timestamp
2. After the threshold (e.g. 7 days), auto-delete runs
3. BeatsCheck maps each corrupt file to a Lidarr track file via the API
4. If `LIDARR_BLOCKLIST=true`, marks the most recent grab for each affected album as failed — Lidarr auto-creates a blocklist entry so the same release is not re-downloaded
5. Deletes track files via Lidarr bulk delete (albums stay monitored, show as missing)
6. Files not tracked by Lidarr are deleted directly, then a Lidarr artist refresh is triggered

**Search queue (`LIDARR_SEARCH=true`):**

After auto-delete removes corrupt files, affected album IDs are written to a persistent search queue (`search_queue.json`). During idle time the container drains this queue — one album at a time, waiting for each Lidarr search to complete before starting the next. Rate limited to 5 albums/hour to avoid flooding indexers/trackers. The queue survives container restarts.

**Interactive delete** also offers to queue searches. After deleting files, if Lidarr is configured you'll be prompted:

```
Queue Lidarr search for 3 deleted albums? [y/n]
```

**Security:**
- API key is sent only via HTTP header, never in URLs or logs
- Lidarr URL is masked in all log output
- Supports Docker secrets (`/run/secrets/lidarr_api_key`)
- HTTP redirects are blocked to prevent credential leaking
- All API calls have explicit timeouts

```yaml
environment:
  - DELETE_AFTER=7
  - LIDARR_URL=http://lidarr:8686
  - LIDARR_API_KEY=your-api-key-here
  - LIDARR_SEARCH=true
  - LIDARR_BLOCKLIST=true
```

## Updating

```bash
docker pull ghcr.io/chodeus/beatscheck:latest
```

Then restart the container.

## Security

See [SECURITY.md](.github/SECURITY.md) for the security policy, vulnerability reporting, and container security measures.

## License

[MIT](LICENSE)
