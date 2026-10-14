# Lidarr integration

When `LIDARR_URL` and `LIDARR_API_KEY` are set, BeatsCheck deletes the files it has matched to Lidarr track files through the Lidarr API. Track file records are removed via the API while albums stay monitored, so Lidarr searches for a replacement. Files with no Lidarr match are deleted directly (see [Error handling](#error-handling)).

```conf
## /config/beatscheck.conf — API key stays off the command line
lidarr_url = "http://lidarr:8686"
lidarr_api_key = "your-api-key-here"
lidarr_search = true
lidarr_blocklist = true
```

## How it works

1. **Scan** — corrupt files are detected and matched to Lidarr trackfile IDs via path suffix matching. IDs are stored in `corrupt_details.json`. Before most deletes, BeatsCheck reads the album's current track files from Lidarr and matches them again, falling back to the stored ID.
2. **Blocklist** (`LIDARR_BLOCKLIST=true`) — before deletion, the most recent grab for each affected album is marked as failed. Lidarr auto-creates a blocklist entry so the same corrupt release is not re-downloaded. **If marking the grab as failed fails, that album is skipped.** An album with no grab history (for example, one imported by hand) is deleted without a blocklist entry. So is an album whose history lookup fails, because BeatsCheck cannot tell that apart from no history.
3. **Delete** — track files are removed via Lidarr's bulk delete API. Albums stay monitored and show as "missing". If the bulk call fails, an album with more than one track file has each one retried on its own. **If nothing is deleted, the album is skipped** (no fallback to filesystem delete). If only some retries succeed, the log counts all of the album's files as deleted, but files still on disk stay in `corrupt.txt`.
4. **Re-download** — Lidarr searches for each monitored album after its track files are deleted, and grabs a replacement if it finds one.

## Unmonitored albums

If a deleted album is unmonitored in Lidarr, it won't be auto-searched. Interactive delete prints `album N unmonitored — will not re-download`; search for it in Lidarr yourself.

With `LIDARR_SEARCH=true` and auto-delete, unmonitored album IDs are written to a persistent search queue (`search_queue.json`). The container drains this queue during idle — one album at a time, rate limited to 5/hour to avoid flooding indexers.

## Path handling

BeatsCheck runs in a container where music is mounted at `/data`, while Lidarr sees the same files at the same or different mount path. When both containers mount the same host path to `/data`, the paths match exactly. At scan time, corrupt files are matched to Lidarr trackfiles by comparing path components from the right (suffix matching). This works regardless of how the music directory is mounted in each container.

## Error handling

When a Lidarr API call fails:
- Marking the grab as failed fails → that album is skipped, error shown
- The grab history lookup fails → the album is deleted without a blocklist entry
- Bulk delete fails → each track file is retried on its own (albums with more than one); if none succeed, the album is skipped and listed for manual deletion
- If Lidarr is unreachable when a scan ends → the files it flagged get no Lidarr ID yet, and each later scan that checks at least one file tries again; a file that still has no ID when it is deleted is deleted directly
- Files not tracked by Lidarr (cover art, .nfo, etc.) are deleted directly, not through Lidarr

## Security

- API key is sent only via HTTP header, never in URLs or logs
- Lidarr URL is masked in all log output
- Config file support — store the API key in `/config/beatscheck.conf` to keep it out of `docker inspect` and process listings
- Also supports Docker secrets (`/run/secrets/lidarr_api_key`)
- HTTP redirects are blocked to prevent credential leaking
- All API calls have explicit timeouts
