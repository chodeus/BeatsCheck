# Configuration

## Config file

On first run, BeatsCheck creates `/config/beatscheck.conf` with all options and their defaults. Edit this file (or use the WebUI's Configuration page) to configure scanning, scheduling, and Lidarr integration. Credentials stored here stay out of `docker inspect` and process listings.

| Setting | Default | Description |
|---------|---------|-------------|
| `output_dir` | `/data/corrupted` | Quarantine destination for move mode. Must match a mounted volume |
| `mode` | `setup` | `setup` (idle), `report`, `delete`, or `move`. `rescan report` or `rescan move` switches to that mode until the saved `mode` changes or the container restarts. `delete` works only at startup: saved while the container runs, it (or an unknown mode) is ignored with a warning, and the WebUI won't save it; use `docker exec -it beatscheck delete` |
| `workers` | `4` | Parallel ffmpeg decode workers. 2 = conservative, 4 = balanced, 8+ = fast |
| `run_interval` | `0` | Hours between scans. `0` = scan once, then stay idle until `rescan`. `168` = weekly. `24` = daily |
| `delete_after` | `0` | Days before corrupt files are auto-deleted. `0` = never (manual only). `7` = 7 day review window |
| `max_auto_delete` | `50` | Safety threshold — abort auto-delete if more than this many files would be removed. `0` = no limit |
| `min_file_age` | `30` | Skip files modified within this many minutes. Prevents flagging active downloads |
| `length_check` | `true` | Flag MP3, Ogg, Opus and WMA files whose audio stops partway through (an interrupted download or copy). See [Cut-short files](how-it-works.md#cut-short-files) |
| `log_level` | `INFO` | Logging verbosity: `DEBUG`, `INFO`, `WARNING`, `ERROR` |
| `max_log_mb` | `50` | Rotate log and do fresh full scan when log exceeds this size. `0` = never rotate |
| `lidarr_url` | *(empty)* | Lidarr instance URL (e.g. `http://lidarr:8686`). Enables [Lidarr integration](lidarr.md) |
| `lidarr_api_key` | *(empty)* | Lidarr API key (Settings → General in Lidarr). Also reads from `/run/secrets/lidarr_api_key` |
| `lidarr_search` | `false` | Queue search for unmonitored albums after auto-delete. Monitored albums are auto-searched by Lidarr. 5 albums/hour during idle |
| `lidarr_blocklist` | `false` | Blocklist the release in Lidarr before deleting, preventing re-download of the same corrupt copy |
| `webui` | `false` | Enable the built-in [web interface](webui.md). Requires a published port |
| `webui_port` | `8484` | Port for the web interface |

Environment variables (uppercase, e.g. `MODE`, `WORKERS`) override the config file if set, including values saved in the WebUI. A WebUI save that a variable overrides says so, and the saved value applies once you remove the variable and recreate the container.

## Container environment variables

These Docker-level settings are configured as environment variables:

| Env Var | Default | Description |
|---------|---------|-------------|
| `PUID` | `99` | User ID for file ownership |
| `PGID` | `100` | Group ID for file ownership |
| `TZ` | `UTC` | Timezone for log timestamps. Auto-detected if `/etc/localtime` is bind-mounted |
| `UMASK` | `002` | File creation mask |
| `WEBUI_TRUSTED_PROXIES` | *(empty)* | Your reverse proxy's IP, e.g. `172.18.0.10` (give the proxy a fixed IP); several IPs or CIDRs are comma-separated. The login lockout honors `X-Forwarded-For` only from these addresses, to tell clients apart, so any host listed can pick its own lockout key: don't list a subnet that other containers share. The proxy must write the client's address into the header itself (nginx: `proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;`); a proxy that passes the client's own header through lets clients dodge the lockout. Empty = every client behind the proxy shares one lockout. |

`MUSIC_DIR`, `OUTPUT_DIR` and `CONFIG_DIR` default to `/data`, `/data/corrupted` and `/config`. `music_dir` and `output_dir` can also be set in `beatscheck.conf`. Each path must be inside a mounted volume.

## Output files (in /config)

| File | Contents |
|------|----------|
| `beats_check.log` | Full scan log — errors, moves, deletes, and scan summaries |
| `beats_check.log.1` `.2` `.3` | Previous logs after rotation (last 3 kept) |
| `processed.txt` | Resume cache — one checked file path per line. Rotated alongside the log |
| `corrupt.txt` | One corrupt file path per line, deduplicated and sorted by path (a scan in progress appends new finds at the end until it finishes) — for scripting or delete mode |
| `corrupt_details.json` | Path-to-error mapping with Lidarr trackfile/album IDs (when configured) |
| `corrupt_tracking.json` | Path-to-first-seen timestamps — used by `DELETE_AFTER` auto-delete |
| `summary.json` | Machine-readable scan results for notification scripts |
| `search_queue.json` | Pending Lidarr album search queue — drained during idle (5/hour) |
| `webui_auth.json` | WebUI login credentials (username + PBKDF2-hashed password), or `{"login": "off"}` when the login is turned off |
| `.scanning` | Lock file: a scan, a delete or an ignore holds a `flock` on it while it runs; the file itself stays |
| `.heartbeat` | Timestamp updated during scans and idle — used by Docker healthcheck |

## Log rotation

When the log exceeds 50 MB (`MAX_LOG_MB=50`), it's rotated to `beats_check.log.1` (keeping up to 3 old copies). The resume cache (`processed.txt`) is rotated alongside it, so the next scan does a fresh full re-check. Set `MAX_LOG_MB=0` to disable rotation.

A scan logs a progress line every 100 files plus a few lines per corrupt file, so the log grows slowly.
