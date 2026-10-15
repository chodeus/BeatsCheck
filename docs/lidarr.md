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

1. **Scan** — corrupt files are detected and matched to Lidarr trackfile IDs (see [Path handling](#path-handling)). IDs are stored in `corrupt_details.json`. Each delete checks the stored matches of the files it deletes again, and looks up any that no longer pass. Before most deletes, BeatsCheck also reads the album's current track files from Lidarr and matches each file again by its name and folder. It keeps the stored ID when that read fails or returns no track files, and for a file that no current track file matches.
2. **Blocklist** (`LIDARR_BLOCKLIST=true`) — before deletion, the most recent grab for each affected album is marked as failed. Lidarr auto-creates a blocklist entry so the same corrupt release is not re-downloaded. **If the history lookup or marking the grab as failed fails, that album is skipped.** An album with no grab history (for example, one imported by hand) is deleted without a blocklist entry.
3. **Delete** — track files are removed via Lidarr's bulk delete API. Albums stay monitored and show as "missing". If the bulk call fails, an album with more than one track file has each one retried on its own. **If nothing is deleted, the album is skipped** (no fallback to filesystem delete). If only some retries succeed, the log counts all of the album's files as deleted, but files still on disk stay in `corrupt.txt`.
4. **Re-download** — Lidarr searches for each monitored album after its track files are deleted, and grabs a replacement if it finds one.

## Unmonitored albums

If a deleted album is unmonitored in Lidarr, it won't be auto-searched. Interactive delete prints `album N unmonitored — will not re-download`; search for it in Lidarr yourself.

With `LIDARR_SEARCH=true` and auto-delete, unmonitored album IDs are written to a persistent search queue (`search_queue.json`). The container drains this queue during idle — one album at a time, rate limited to 5/hour to avoid flooding indexers.

## Path handling

BeatsCheck runs in a container where music is mounted at `/data`, while Lidarr sees the same files at the same or different mount path. A corrupt file matches a Lidarr track file only when its whole path from the artist folder down is the same as Lidarr's, so the mount paths above the artist folders can differ. A file isn't matched when Lidarr's track file is a different file on this disk, or when the same path under the artist folder also exists higher in the library (a second copy of the album). A file that shares only its name, or its album folder name, with a Lidarr track file (another edition, or another artist's album with the same title) isn't matched either. If two or more track files tie for the best match, the file stays unmatched.

Files with no match are deleted directly, and Lidarr isn't asked to replace them. So the music folder BeatsCheck scans must hold the artist folders, as Lidarr's root folder does. If it points inside an artist folder, no file matches.

## Error handling

When a Lidarr API call fails:
- Marking the grab as failed fails → that album is skipped, error shown
- The grab history lookup fails → that album is skipped, error shown
- Bulk delete fails → each track file is retried on its own (albums with more than one); if none succeed, the album is skipped and listed for manual deletion
- If Lidarr is unreachable when a scan ends → the files it flagged get no Lidarr ID yet. Each later scan that checks at least one file tries again, and so does every delete. The same lookup re-checks stored matches against Lidarr's artist list. If that lookup fails at delete time, nothing is deleted: auto-delete tries again next run, and the WebUI and interactive delete show an error. A file Lidarr still doesn't match after a good lookup is deleted directly
- A folder delete reads each album's track files to match the rest of the folder. If Lidarr doesn't track the flagged file, it reads the track files of the artist whose folder is in the path instead, and matches each file the same way a scan does (see [Path handling](#path-handling)). If either read fails → nothing in that folder is deleted, error shown
- Files not tracked by Lidarr (cover art, .nfo, etc.) are deleted directly, not through Lidarr

## Security

- API key is sent only via HTTP header, never in URLs or logs
- Lidarr URL is masked in all log output
- Config file support — store the API key in `/config/beatscheck.conf` to keep it out of `docker inspect` and process listings
- Also supports Docker secrets (`/run/secrets/lidarr_api_key`)
- HTTP redirects are blocked to prevent credential leaking
- All API calls have explicit timeouts
