# Lidarr integration

When `LIDARR_URL` and `LIDARR_API_KEY` are set, BeatsCheck deletes the files it has matched to Lidarr track files through the Lidarr API. Track file records are removed via the API while albums stay monitored — Lidarr automatically re-downloads monitored albums after deletion. Files with no Lidarr match are deleted directly (see [Error handling](#error-handling)).

```conf
## /config/beatscheck.conf — API key stays off the command line
lidarr_url = "http://lidarr:8686"
lidarr_api_key = "your-api-key-here"
lidarr_search = true
lidarr_blocklist = true
```

## How it works

1. **Scan** — corrupt files are detected and matched to Lidarr trackfile IDs via path suffix matching. IDs are stored in `corrupt_details.json` so delete operations don't need to re-resolve them.
2. **Blocklist** (`LIDARR_BLOCKLIST=true`) — before deletion, the most recent grab for each affected album is marked as failed. Lidarr auto-creates a blocklist entry so the same corrupt release is not re-downloaded. **If blocklist fails, deletion is aborted.**
3. **Delete** — track files are removed via Lidarr's bulk delete API. Albums stay monitored and show as "missing". **If the API call fails, deletion is aborted** (no silent fallback to filesystem delete).
4. **Re-download** — monitored albums are automatically re-searched by Lidarr after the trackfiles are deleted. No manual intervention needed.

## Unmonitored albums

If a deleted album is unmonitored in Lidarr, it won't be auto-searched. In interactive delete, you'll be prompted:

```
2 deleted albums are unmonitored in Lidarr. Queue search? [y/n]
```

With `LIDARR_SEARCH=true` and auto-delete, unmonitored album IDs are written to a persistent search queue (`search_queue.json`). The container drains this queue during idle — one album at a time, rate limited to 5/hour to avoid flooding indexers.

## Path handling

BeatsCheck runs in a container where music is mounted at `/data`, while Lidarr sees the same files at the same or different mount path. When both containers mount the same host path to `/data`, the paths match exactly. At scan time, corrupt files are matched to Lidarr trackfiles by comparing path components from the right (suffix matching). This works regardless of how the music directory is mounted in each container.

## Error handling

All Lidarr API operations are fail-safe:
- If blocklist fails → delete is aborted, error shown
- If bulk delete API fails → delete is aborted, error shown
- If Lidarr is unreachable when a scan ends → the files it flagged get no Lidarr ID yet, and each later scan tries again; a file that still has no ID when it is deleted is deleted directly
- Files not tracked by Lidarr (cover art, .nfo, etc.) are deleted directly via `os.remove`

## Security

- API key is sent only via HTTP header, never in URLs or logs
- Lidarr URL is masked in all log output
- Config file support — store the API key in `/config/beatscheck.conf` to keep it out of `docker inspect` and process listings
- Also supports Docker secrets (`/run/secrets/lidarr_api_key`)
- HTTP redirects are blocked to prevent credential leaking
- All API calls have explicit timeouts
