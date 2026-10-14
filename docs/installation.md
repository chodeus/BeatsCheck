# Installation

## Docker Compose (recommended)

A complete [`compose.yaml`](../compose.yaml) is included in the repository:

```bash
# Copy the compose file into its own folder
mkdir -p /path/to/beatscheck
cp compose.yaml /path/to/beatscheck/
cd /path/to/beatscheck
# Edit paths and settings in compose.yaml, then:
docker compose up -d
```

## Docker run

```bash
# Scan (report mode, run once)
docker run --rm \
  -v /path/to/music:/data:ro \
  -v /path/to/config:/config \
  -e MODE=report \
  -e WORKERS=6 \
  --cpus=2 \
  ghcr.io/chodeus/beatscheck:latest

# Daemon mode (weekly scans, stays running)
docker run -d --restart unless-stopped \
  --name beatscheck \
  -v /path/to/music:/data:ro \
  -v /path/to/config:/config \
  -e MODE=report \
  -e RUN_INTERVAL=168 \
  -e WORKERS=6 \
  ghcr.io/chodeus/beatscheck:latest

# Interactive delete needs the music mounted read-write: recreate the
# daemon above with -v /path/to/music:/data (no :ro), then run
docker exec -it beatscheck delete

# Trigger a rescan
docker exec beatscheck rescan
```

## Unraid

### Install the template

```bash
docker pull ghcr.io/chodeus/beatscheck:latest

wget -O /boot/config/plugins/dockerMan/templates-user/my-BeatsCheck.xml \
  https://raw.githubusercontent.com/chodeus/BeatsCheck/main/beats-check.xml
```

### Add the container

1. Go to **Docker** tab in Unraid web UI
2. Click **Add Container**
3. Select **BeatsCheck** from the template dropdown
4. Verify paths match your setup:
   - **Data**: `/mnt/user/data` (or your music directory)
   - **Config**: `/mnt/user/appdata/beatscheck`
5. Configure your settings (paths, workers, Lidarr if needed)
6. Click **Apply** — the container starts idle in setup mode
7. Open the container console and type `rescan report` to start scanning

On Unraid the container is named `BeatsCheck`, so host commands use that name: `docker exec -it BeatsCheck delete`.

### Notifications (optional)

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

## Without Docker

Requires Python 3.9+ and ffmpeg installed. The Docker image bundles a fully static FFmpeg from [chodeus/ffmpeg-static](https://github.com/chodeus/ffmpeg-static) (kept current via Renovate); outside Docker you provide your own ffmpeg.

```bash
MODE=report WORKERS=6 python3 app/main.py /path/to/music /path/to/quarantine /path/to/config/beats_check.log
```

The third argument is the log file path. All state files (`processed.txt`, `corrupt.txt`, etc.) are written to the same directory as the log file. Unix-only (requires `fcntl`).

## Updating

Pull the new image, then recreate the container. A plain restart keeps running the old image.

```bash
# Docker Compose, from the folder that holds your compose.yaml
docker compose pull
docker compose up -d

# docker run: pull, remove the container, then start it again with the same options
docker pull ghcr.io/chodeus/beatscheck:latest
docker rm -f beatscheck
```

On Unraid, click **Check for Updates** on the Docker tab, then **apply update** on the BeatsCheck container.
