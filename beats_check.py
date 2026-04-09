import fcntl
import json
import logging
import os
import re
import subprocess
import sys
import shutil
import signal
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed

__version__ = "2.0.0"

AUDIO_EXTENSIONS = {
    '.flac', '.mp3', '.m4a', '.ogg', '.opus', '.wav',
    '.wma', '.aac', '.aiff', '.aif', '.ape', '.wv',
    '.alac', '.m4b', '.m4p', '.mp2', '.mpc', '.dsf', '.dff',
}

shutdown_requested = False
logger = logging.getLogger("beatscheck")


def handle_shutdown(signum, frame):
    global shutdown_requested
    shutdown_requested = True
    logger.info("Shutdown requested, finishing in-progress files...")


def format_size(bytes_val):
    if bytes_val >= 1024 ** 4:
        return f"{bytes_val / 1024 ** 4:.1f} TB"
    if bytes_val >= 1024 ** 3:
        return f"{bytes_val / 1024 ** 3:.1f} GB"
    if bytes_val >= 1024 ** 2:
        return f"{bytes_val / 1024 ** 2:.1f} MB"
    if bytes_val >= 1024:
        return f"{bytes_val / 1024:.1f} KB"
    return f"{bytes_val} B"


def _clean_ffmpeg_errors(stderr):
    """Strip ffmpeg memory addresses and internal codec references from errors."""
    if not stderr or not stderr.strip():
        return "Non-zero exit code"
    # Strip [codec @ 0x...] prefixes and [aist#N:N/codec @ 0x...] wrappers
    cleaned = re.sub(r'\[[\w:#/]+ @ 0x[0-9a-f]+\]\s*', '', stderr)
    # Collapse duplicate whitespace and pipe separators
    cleaned = re.sub(r'\s*\|\s*', ' | ', cleaned)
    cleaned = re.sub(r'\s+', ' ', cleaned).strip()
    # Deduplicate repeated error messages
    parts = [p.strip() for p in cleaned.split(' | ') if p.strip()]
    seen = set()
    unique = []
    for p in parts:
        if p not in seen:
            seen.add(p)
            unique.append(p)
    return ' | '.join(unique) if unique else "Non-zero exit code"


def check_audio_file(file_path):
    """Decode-test a single audio file. Pure function — no shared state.
    Returns (file_path, is_corrupt, reason)."""
    try:
        file_size = os.path.getsize(file_path)
        if file_size < 1024:
            return (file_path, True, f"File too small ({file_size} bytes)")
    except OSError as e:
        return (file_path, True, f"File not accessible: {e}")

    try:
        result = subprocess.run(
            ["ffmpeg", "-v", "error", "-xerror", "-nostdin",
             "-i", file_path, "-map", "0:a", "-f", "null", "-"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            timeout=600,
        )
    except subprocess.TimeoutExpired:
        return (file_path, True, "Decode timed out (>10 minutes)")

    if result.returncode != 0:
        reason = _clean_ffmpeg_errors(result.stderr)
        return (file_path, True, reason)

    return (file_path, False, None)


def collect_audio_files(input_folder, min_age_minutes=30):
    """Walk the input folder and return all audio file paths.
    Does not follow symlinks to prevent traversal outside the music dir.
    Skips files modified within min_age_minutes to avoid flagging files
    being actively written by download clients."""
    real_root = os.path.realpath(input_folder)
    age_threshold = time.time() - (min_age_minutes * 60)
    files = []
    skipped_young = 0
    for root, _, filenames in os.walk(input_folder, followlinks=False):
        for f in sorted(filenames):
            file_path = os.path.join(root, f)
            real_path = os.path.realpath(file_path)
            if not real_path.startswith(real_root + os.sep) and real_path != real_root:
                continue
            if os.path.splitext(f)[1].lower() not in AUDIO_EXTENSIONS:
                continue
            try:
                if os.path.getmtime(file_path) > age_threshold:
                    skipped_young += 1
                    continue
            except OSError:
                continue
            files.append(file_path)
    if skipped_young > 0:
        logger.debug(
            "Skipped %d files modified within last %d minutes",
            skipped_young, min_age_minutes
        )
    return files


def format_eta(seconds):
    if seconds < 0:
        return "unknown"
    h, remainder = divmod(int(seconds), 3600)
    m, s = divmod(remainder, 60)
    if h > 0:
        return f"{h}h{m:02d}m"
    if m > 0:
        return f"{m}m{s:02d}s"
    return f"{s}s"


def get_already_processed_files(log_dir):
    """Load the set of already-processed file paths from processed.txt."""
    processed_path = os.path.join(log_dir, "processed.txt")
    if not os.path.exists(processed_path):
        return set()
    processed = set()
    with open(processed_path, 'r', encoding='utf-8') as f:
        for line in f:
            path = line.rstrip("\n")
            if path:
                processed.add(path)
    return processed


def write_json_atomic(path, data):
    """Write JSON data atomically using a temp file + rename."""
    tmp_path = path + ".tmp"
    with open(tmp_path, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2)
    os.rename(tmp_path, path)


def _idle_wait(log_dir, timeout_seconds):
    """Sleep until timeout, shutdown, or .rescan trigger. Returns True if rescan triggered."""
    rescan_path = os.path.join(log_dir, ".rescan")
    heartbeat_path = os.path.join(log_dir, ".heartbeat")
    deadline = time.time() + timeout_seconds if timeout_seconds else None
    while not shutdown_requested:
        _write_heartbeat(heartbeat_path)
        if os.path.exists(rescan_path):
            try:
                os.remove(rescan_path)
            except OSError:
                pass
            logger.info("Rescan requested.")
            return True
        if deadline and time.time() >= deadline:
            return False
        time.sleep(10)
    return False


def _write_heartbeat(heartbeat_path):
    """Write current timestamp to heartbeat file for healthcheck."""
    try:
        with open(heartbeat_path, 'w') as f:
            f.write(str(int(time.time())))
    except OSError:
        pass


def _rotate_file(path, keep=3):
    """Rotate path -> path.1 -> path.2 -> ... keeping last N copies."""
    oldest = f"{path}.{keep}"
    if os.path.exists(oldest):
        os.remove(oldest)
    for i in range(keep - 1, 0, -1):
        src = f"{path}.{i}"
        dst = f"{path}.{i + 1}"
        if os.path.exists(src):
            shutil.move(src, dst)
    if os.path.exists(path):
        shutil.move(path, f"{path}.1")


# ---------------------------------------------------------------------------
# Lidarr API integration
# ---------------------------------------------------------------------------

class _NoRedirectHandler(urllib.request.HTTPRedirectHandler):
    """Block redirects to prevent API key leaking to a redirected host."""
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        raise urllib.error.HTTPError(
            newurl, code, "Redirect blocked for security", headers, fp)


def _lidarr_request(url, api_key, method="GET", data=None, timeout=30):
    """Make an authenticated request to the Lidarr API."""
    headers = {
        "X-Api-Key": api_key,
        "Content-Type": "application/json",
    }
    body = json.dumps(data).encode("utf-8") if data else None
    req = urllib.request.Request(url, data=body, headers=headers, method=method)
    opener = urllib.request.build_opener(_NoRedirectHandler)
    # Extract just the API path for logging (never log the full URL or host)
    api_path = url.split("/api/", 1)[-1] if "/api/" in url else "?"
    try:
        with opener.open(req, timeout=timeout) as resp:
            raw = resp.read()
            return json.loads(raw) if raw else {}
    except urllib.error.HTTPError as e:
        logger.error("Lidarr API %s /api/%s -> HTTP %d",
                     method, api_path, e.code)
        return None
    except (urllib.error.URLError, OSError) as e:
        logger.error("Lidarr API connection failed (%s /api/%s)",
                     method, api_path)
        logger.debug("Lidarr connection error detail: %s", e)
        return None


def _load_lidarr_api_key():
    """Load Lidarr API key from env var or Docker secret. Never log it."""
    key = os.environ.get("LIDARR_API_KEY", "")
    if not key:
        secret_path = "/run/secrets/lidarr_api_key"
        if os.path.exists(secret_path):
            with open(secret_path, 'r') as f:
                key = f.read().strip()
    return key


def _lidarr_get_artists(base_url, api_key):
    """Fetch all artists. Returns list of {id, path} dicts."""
    result = _lidarr_request(f"{base_url}/api/v1/artist", api_key)
    if result is None:
        return []
    return [{"id": a["id"], "path": a["path"]} for a in result]


def _lidarr_get_trackfiles(base_url, api_key, artist_id):
    """Fetch all track files for an artist."""
    url = f"{base_url}/api/v1/trackfile?artistId={artist_id}"
    result = _lidarr_request(url, api_key)
    if result is None:
        return []
    return [{"id": tf["id"], "path": tf["path"],
             "albumId": tf.get("albumId", 0)} for tf in result]


def _lidarr_get_album_trackfiles(base_url, api_key, album_id):
    """Fetch all track files for an album."""
    url = f"{base_url}/api/v1/trackfile?albumId={album_id}"
    result = _lidarr_request(url, api_key)
    if result is None:
        return []
    return [{"id": tf["id"], "path": tf["path"],
             "albumId": tf.get("albumId", 0)} for tf in result]


def _lidarr_delete_album(base_url, api_key, album_id):
    """Delete an album via Lidarr API (removes files + DB records)."""
    url = f"{base_url}/api/v1/album/{album_id}?deleteFiles=true"
    return _lidarr_request(url, api_key, method="DELETE")


def _lidarr_delete_trackfiles_bulk(base_url, api_key, track_file_ids):
    """Bulk delete track files via Lidarr API."""
    url = f"{base_url}/api/v1/trackfile/bulk"
    data = {"trackFileIds": track_file_ids}
    return _lidarr_request(url, api_key, method="DELETE", data=data)


def _lidarr_refresh_artists(base_url, api_key, artist_ids):
    """Trigger a rescan for the given artists."""
    url = f"{base_url}/api/v1/command"
    data = {"name": "RefreshArtist", "artistIds": list(artist_ids)}
    return _lidarr_request(url, api_key, method="POST", data=data)


def _lidarr_delete_corrupt(base_url, api_key, corrupt_paths, log_file):
    """Delete corrupt files via Lidarr API. Falls back to os.remove for
    files not tracked by Lidarr. Returns count of files deleted."""
    artists = _lidarr_get_artists(base_url, api_key)
    if not artists:
        logger.warning("Lidarr: could not fetch artists, "
                       "falling back to direct deletion")
        return None

    # Map corrupt file paths to artist IDs by matching path prefixes
    path_to_artist = {}
    for corrupt_path in corrupt_paths:
        for artist in artists:
            artist_path = artist["path"].rstrip("/")
            if corrupt_path.startswith(artist_path + "/"):
                path_to_artist[corrupt_path] = artist["id"]
                break

    lidarr_paths = set(path_to_artist.keys())
    non_lidarr_paths = [p for p in corrupt_paths if p not in lidarr_paths]

    # Fetch track files for each affected artist and map paths to IDs
    affected_artist_ids = set(path_to_artist.values())
    path_to_trackfile = {}
    trackfile_to_album = {}
    for artist_id in affected_artist_ids:
        trackfiles = _lidarr_get_trackfiles(base_url, api_key, artist_id)
        for tf in trackfiles:
            path_to_trackfile[tf["path"]] = tf["id"]
            trackfile_to_album[tf["id"]] = tf["albumId"]

    # Group corrupt files by album to decide album-level vs track-level delete
    album_corrupt_ids = {}
    unmatched_lidarr = []
    for corrupt_path in lidarr_paths:
        tf_id = path_to_trackfile.get(corrupt_path)
        if tf_id is None:
            unmatched_lidarr.append(corrupt_path)
            continue
        album_id = trackfile_to_album.get(tf_id, 0)
        if album_id not in album_corrupt_ids:
            album_corrupt_ids[album_id] = []
        album_corrupt_ids[album_id].append(tf_id)

    deleted = 0
    albums_deleted = []
    tracks_to_delete = []

    with open(log_file, 'a', encoding='utf-8') as log:
        # For each album, check if ALL tracks are corrupt → album delete
        for album_id, corrupt_tf_ids in album_corrupt_ids.items():
            if album_id == 0:
                tracks_to_delete.extend(corrupt_tf_ids)
                continue
            all_album_tfs = _lidarr_get_album_trackfiles(
                base_url, api_key, album_id)
            all_album_tf_ids = {tf["id"] for tf in all_album_tfs}
            corrupt_set = set(corrupt_tf_ids)

            if corrupt_set >= all_album_tf_ids and len(all_album_tf_ids) > 0:
                result = _lidarr_delete_album(base_url, api_key, album_id)
                if result is not None:
                    deleted += len(corrupt_tf_ids)
                    albums_deleted.append(album_id)
                    logger.info("  Lidarr: deleted album %d "
                                "(%d corrupt files)", album_id,
                                len(corrupt_tf_ids))
                    log.write(f"LIDARR ALBUM DELETE: album {album_id} "
                              f"({len(corrupt_tf_ids)} files)\n")
                else:
                    tracks_to_delete.extend(corrupt_tf_ids)
            else:
                tracks_to_delete.extend(corrupt_tf_ids)

        # Bulk delete individual corrupt tracks
        if tracks_to_delete:
            result = _lidarr_delete_trackfiles_bulk(
                base_url, api_key, tracks_to_delete)
            if result is not None:
                deleted += len(tracks_to_delete)
                logger.info("  Lidarr: deleted %d individual track files",
                            len(tracks_to_delete))
                log.write(f"LIDARR TRACK DELETE: "
                          f"{len(tracks_to_delete)} files\n")
            else:
                logger.error("  Lidarr: bulk track delete failed, "
                             "falling back to direct deletion")
                for tf_path in lidarr_paths:
                    if path_to_trackfile.get(tf_path) in tracks_to_delete:
                        non_lidarr_paths.append(tf_path)

        # Direct delete for files not in Lidarr
        for path in non_lidarr_paths + unmatched_lidarr:
            try:
                os.remove(path)
                deleted += 1
                log.write(f"DIRECT DELETE (not in Lidarr): {path}\n")
                logger.info("  Deleted (not in Lidarr): %s", path)
            except OSError as e:
                log.write(f"ERROR deleting {path}: {e}\n")
                logger.error("  ERROR: %s - %s", path, e)

        # Refresh affected artists so Lidarr detects changes
        if affected_artist_ids:
            _lidarr_refresh_artists(
                base_url, api_key, list(affected_artist_ids))
            logger.info("  Lidarr: triggered refresh for %d artists",
                        len(affected_artist_ids))

    return deleted


def _display_folder_files(corrupt_files, corrupt_details):
    """Display corrupt files in a folder with reasons and sizes."""
    for cf in corrupt_files:
        name = os.path.basename(cf)
        reason = corrupt_details.get(cf, "")
        try:
            size_str = format_size(os.path.getsize(cf))
            print(f"           {name} ({size_str})")
            if reason:
                if len(reason) > 200:
                    cut = reason[:200].rsplit(' ', 1)[0]
                    reason = cut + "..."
                print(f"             -> {reason}")
        except OSError:
            print(f"           {name} (already deleted)")


def _handle_folder_action(choice, folder, existing, log):
    """Execute a delete action on a folder. Returns (folders_del, files_del, skipped)."""
    if choice == 'y' and os.path.isdir(folder):
        try:
            shutil.rmtree(folder)
            log.write(f"DELETED FOLDER: {folder} ({len(existing)} corrupt files)\n")
            print("           -> Folder deleted\n")
            return (1, len(existing), 0)
        except OSError as e:
            print(f"           ERROR: {e}\n")
            log.write(f"ERROR deleting folder {folder}: {e}\n")
            return (0, 0, 0)
    elif choice == 'f':
        deleted = 0
        for cf in existing:
            try:
                os.remove(cf)
                deleted += 1
                log.write(f"DELETED FILE: {cf}\n")
            except OSError as e:
                print(f"           ERROR deleting {os.path.basename(cf)}: {e}")
                log.write(f"ERROR deleting {cf}: {e}\n")
        print(f"           -> {len(existing)} corrupt files deleted\n")
        return (0, deleted, 0)
    else:
        log.write(f"SKIPPED: {folder}\n")
        print()
        return (0, 0, 1)


def _load_corrupt_file_list(corrupt_list_path, log_dir):
    """Load and deduplicate corrupt files, group by folder."""
    with open(corrupt_list_path, 'r', encoding='utf-8') as f:
        all_paths = [line.strip() for line in f if line.strip()]

    details_path = os.path.join(log_dir, "corrupt_details.json")
    corrupt_details = {}
    if os.path.exists(details_path):
        with open(details_path, 'r', encoding='utf-8') as f:
            corrupt_details = json.load(f)

    seen = set()
    files = []
    for fp in all_paths:
        if fp not in seen:
            seen.add(fp)
            files.append(fp)

    folders = {}
    for fp in files:
        folder = os.path.dirname(fp)
        if folder not in folders:
            folders[folder] = []
        folders[folder].append(fp)

    return files, folders, corrupt_details


def run_delete_mode(corrupt_list_path, log_file, log_dir):
    """Interactive delete mode. Groups corrupt files by album folder and prompts."""
    lock_path = os.path.join(log_dir, ".scanning")
    if os.path.exists(lock_path):
        logger.info("A scan is currently running. Waiting for it to finish...")
        _wait_for_scan_lock(log_dir)
        if shutdown_requested:
            return
        logger.info("Scan finished. Starting delete mode.")

    if not os.path.exists(corrupt_list_path):
        logger.error("No corrupt file list found at %s", corrupt_list_path)
        logger.info("Run a scan first with MODE=report")
        sys.exit(1)

    files, folders, corrupt_details = _load_corrupt_file_list(
        corrupt_list_path, log_dir)

    if not files:
        logger.info("corrupt.txt is empty — no corrupt files found.")
        return

    total_files = len(files)
    total_folders = len(folders)
    total_corrupt_size = 0
    for f in files:
        try:
            total_corrupt_size += os.path.getsize(f)
        except OSError:
            pass

    print(f"Found {total_files} corrupt files across "
          f"{total_folders} folders ({format_size(total_corrupt_size)})\n")

    try:
        action = input(
            "  [a] delete ALL corrupt files now\n"
            "  [i] interactive (decide per folder)\n"
            "  [q] quit\n\n"
            "  Choice: "
        ).strip().lower()
    except EOFError:
        print("\nNo input available. Run with: "
              "docker exec -it BeatsCheck /app/delete.sh")
        return

    if action == 'q':
        return
    if action == 'a':
        run_mass_delete(files, log_file, log_dir)
        return
    if action != 'i':
        print("Invalid choice.")
        return

    _run_interactive_delete(folders, total_folders, corrupt_details,
                            log_file, files, corrupt_list_path)


def _run_interactive_delete(folders, total_folders, corrupt_details,
                            log_file, files, corrupt_list_path):
    """Interactive per-folder delete loop."""
    print("\nFor each folder:")
    print("  [y] delete entire folder (album)    [f] delete corrupt files only")
    print("  [n] skip                             [a] delete all remaining folders")
    print("  [q] quit\n")

    folders_deleted = 0
    files_deleted = 0
    skipped_folders = 0
    missing_files = 0
    delete_all = False

    with open(log_file, 'a', encoding='utf-8') as log:
        log.write(f"\n{'='*60}\n")
        log.write(f"Interactive delete started: "
                  f"{time.strftime('%Y-%m-%d %H:%M:%S')}\n")
        log.write(f"{'='*60}\n")

        for i, (folder, corrupt_files) in enumerate(folders.items(), 1):
            existing = [f for f in corrupt_files if os.path.exists(f)]
            gone = len(corrupt_files) - len(existing)
            if gone:
                missing_files += gone

            if not existing and not os.path.isdir(folder):
                print(f"  [{i}/{total_folders}] GONE: {folder}")
                log.write(f"MISSING FOLDER: {folder}\n")
                continue

            print(f"  [{i}/{total_folders}] {folder}/")
            _display_folder_files(corrupt_files, corrupt_details)

            try:
                entries = os.listdir(folder)
                total_in_folder = sum(
                    1 for e in entries
                    if os.path.isfile(os.path.join(folder, e)))
                print(f"           ({len(existing)} corrupt / "
                      f"{total_in_folder} total files in folder)")
            except OSError:
                pass

            if delete_all:
                choice = 'y'
            else:
                try:
                    choice = input(
                        "           Action? [y/f/n/a/q] ").strip().lower()
                except EOFError:
                    print("\nNo input available. Run with: docker run -it ...")
                    break

            if choice == 'q':
                print("\nQuitting.")
                break
            if choice == 'a':
                delete_all = True
                choice = 'y'

            fd, fid, sk = _handle_folder_action(choice, folder, existing, log)
            folders_deleted += fd
            files_deleted += fid
            skipped_folders += sk

        summary = (
            f"\nDelete summary:\n"
            f"  Folders deleted:  {folders_deleted}\n"
            f"  Files deleted:    {files_deleted}\n"
            f"  Folders skipped:  {skipped_folders}\n"
            f"  Already missing:  {missing_files}\n"
        )
        print(summary)
        log.write(summary)

    remaining = [f for f in files if os.path.exists(f)]
    with open(corrupt_list_path, 'w', encoding='utf-8') as f:
        for fp in remaining:
            f.write(fp + "\n")
    if remaining:
        print(f"Updated {corrupt_list_path} ({len(remaining)} files remaining)")
    else:
        print(f"All corrupt files handled. {corrupt_list_path} cleared.")


def run_mass_delete(files, log_file, log_dir):
    """Delete all corrupt files without prompts."""
    corrupt_list_path = os.path.join(log_dir, "corrupt.txt")
    existing = [f for f in files if os.path.exists(f)]

    if not existing:
        print("All files already deleted.")
        with open(corrupt_list_path, 'w') as f:
            pass
        return

    total_size = 0
    for f in existing:
        try:
            total_size += os.path.getsize(f)
        except OSError:
            pass

    try:
        confirm = input(
            f"\nConfirm: delete {len(existing)} files ({format_size(total_size)})? [yes/no] "
        ).strip().lower()
    except EOFError:
        print("\nNo input available.")
        return

    if confirm != "yes":
        print("Cancelled.")
        return

    print()
    deleted = 0
    with open(log_file, 'a', encoding='utf-8') as log:
        log.write(f"\n{'='*60}\n")
        log.write(f"Mass delete: {time.strftime('%Y-%m-%d %H:%M:%S')}\n")
        log.write(f"{'='*60}\n")

        for fp in existing:
            try:
                os.remove(fp)
                deleted += 1
                log.write(f"DELETED: {fp}\n")
                print(f"  Deleted: {fp}")
            except OSError as e:
                log.write(f"ERROR: {fp} - {e}\n")
                print(f"  ERROR: {fp} - {e}")

        log.write(f"Mass delete complete: {deleted}/{len(existing)} deleted\n")

    print(f"\n{deleted}/{len(existing)} files deleted")

    remaining = [f for f in files if os.path.exists(f)]
    with open(corrupt_list_path, 'w', encoding='utf-8') as f:
        for fp in remaining:
            f.write(fp + "\n")
    if not remaining:
        print("corrupt.txt cleared.")


def _acquire_scan_lock(log_dir):
    """Acquire an exclusive file lock for scanning. Returns the lock fd."""
    lock_path = os.path.join(log_dir, ".scanning")
    lf = open(lock_path, 'w')
    fcntl.flock(lf.fileno(), fcntl.LOCK_EX)
    lf.write(str(os.getpid()))
    lf.flush()
    return lf


def _wait_for_scan_lock(log_dir):
    """Block until the scan lock is available, then release immediately."""
    lock_path = os.path.join(log_dir, ".scanning")
    if not os.path.exists(lock_path):
        return
    try:
        with open(lock_path, 'r') as lf:
            fcntl.flock(lf.fileno(), fcntl.LOCK_SH)
    except OSError:
        pass


def run_scan(input_folder, output_folder, log_file, log_dir, mode, workers,
             min_age_minutes=30):
    """Scan mode: decode-test all audio files with parallel workers."""
    corrupt_list_path = os.path.join(log_dir, "corrupt.txt")

    lock_fd = _acquire_scan_lock(log_dir)
    try:
        return _run_scan_inner(input_folder, output_folder, log_file, log_dir,
                               mode, workers, corrupt_list_path,
                               min_age_minutes)
    finally:
        try:
            fcntl.flock(lock_fd.fileno(), fcntl.LOCK_UN)
            lock_fd.close()
            os.remove(os.path.join(log_dir, ".scanning"))
        except OSError:
            pass


def _handle_corrupt_file(file_path, reason, mode, input_folder, output_folder,
                         corrupt_log, log, existing_corrupt, corrupt_details):
    logger.info("CORRUPT: %s", file_path)
    logger.info("         %s", reason)

    try:
        nlinks = os.stat(file_path).st_nlink
        if nlinks > 1:
            logger.info("         hardlinked (%d links) — "
                        "other links share the same data", nlinks)
    except OSError:
        pass

    if file_path not in existing_corrupt:
        corrupt_log.write(file_path + "\n")
        corrupt_log.flush()
        existing_corrupt.add(file_path)
    corrupt_details[file_path] = reason

    if mode == "move":
        rel = os.path.relpath(os.path.dirname(file_path), input_folder)
        dest_dir = os.path.join(output_folder, rel)
        os.makedirs(dest_dir, exist_ok=True)
        dest = os.path.join(dest_dir, os.path.basename(file_path))
        try:
            shutil.move(file_path, dest)
            log.write(f"File moved: {file_path} -> {dest}\n")
            log.flush()
            logger.info("         moved -> %s", dest)
            corrupt_details[dest] = reason
        except (OSError, shutil.Error) as e:
            log.write(f"ERROR: Failed to move {file_path}: {e}\n")
            log.flush()
            logger.error("         ERROR: failed to move: %s", e)


def _log_scan_banner(mode, workers, input_folder, output_folder, log_file,
                     corrupt_list_path, all_files, total_library_size,
                     total, skipped):
    """Log the scan configuration banner."""
    logger.info("BeatsCheck v%s — %s mode, %d workers", __version__, mode, workers)
    logger.info("  Library: %d files (%s), %d to scan (%d already processed)",
                len(all_files), format_size(total_library_size), total, skipped)
    logger.debug("  Music:   %s", input_folder)
    logger.debug("  Log:     %s", log_file)
    logger.debug("  Corrupt: %s", corrupt_list_path)
    if mode == "move":
        logger.debug("  Output:  %s", output_folder)
    if mode == "report":
        logger.info("  (report mode - no files will be moved)")


def _load_existing_corrupt(corrupt_list_path):
    """Load existing corrupt paths to deduplicate appends."""
    existing = set()
    if os.path.exists(corrupt_list_path):
        with open(corrupt_list_path, 'r', encoding='utf-8') as f:
            for line in f:
                path = line.strip()
                if path:
                    existing.add(path)
    return existing


def _total_file_size(files):
    """Sum file sizes, ignoring missing files."""
    total = 0
    for f in files:
        try:
            total += os.path.getsize(f)
        except OSError:
            pass
    return total


def _run_scan_inner(input_folder, output_folder, log_file, log_dir,
                    mode, workers, corrupt_list_path, min_age_minutes):
    """Inner scan logic."""
    already_processed = get_already_processed_files(log_dir)
    processed_path = os.path.join(log_dir, "processed.txt")

    logger.debug("Scanning for audio files...")
    all_files = collect_audio_files(input_folder, min_age_minutes)
    total_library_size = _total_file_size(all_files)

    files_to_check = [f for f in all_files if f not in already_processed]
    skipped = len(all_files) - len(files_to_check)
    total = len(files_to_check)

    _log_scan_banner(mode, workers, input_folder, output_folder, log_file,
                     corrupt_list_path, all_files, total_library_size,
                     total, skipped)

    if total == 0:
        logger.debug("Nothing to do.")
        return 0

    checked = 0
    corrupted = 0
    corrupt_size = 0
    start_time = time.time()

    details_path = os.path.join(log_dir, "corrupt_details.json")
    corrupt_details = {}
    if os.path.exists(details_path):
        with open(details_path, 'r', encoding='utf-8') as f:
            corrupt_details = json.load(f)

    existing_corrupt = _load_existing_corrupt(corrupt_list_path)

    heartbeat_path = os.path.join(log_dir, ".heartbeat")
    with open(log_file, 'a', encoding='utf-8') as log, \
         open(corrupt_list_path, 'a', encoding='utf-8') as corrupt_log, \
         open(processed_path, 'a', encoding='utf-8') as processed_log:

        log.write(f"\n{'='*60}\n")
        log.write(f"Scan started: {time.strftime('%Y-%m-%d %H:%M:%S')}\n")
        log.write(f"Mode: {mode} | Workers: {workers}\n")
        log.write(f"Library: {len(all_files)} files ({format_size(total_library_size)})\n")
        log.write(f"Files to check: {total} (skipped {skipped} already processed)\n")
        log.write(f"{'='*60}\n")
        log.flush()

        batch_size = 1000
        file_iter = iter(files_to_check)
        active_futures = {}

        def _submit_batch():
            for _ in range(batch_size):
                f = next(file_iter, None)
                if f is None:
                    break
                fut = pool.submit(check_audio_file, f)
                active_futures[fut] = f

        with ThreadPoolExecutor(max_workers=workers) as pool:
            _submit_batch()

            while active_futures:
                if shutdown_requested:
                    pool.shutdown(wait=True, cancel_futures=True)
                    break

                done = set()
                for future in as_completed(active_futures, timeout=None):
                    done.add(future)

                    try:
                        file_path, is_corrupt, reason = future.result()
                    except Exception as e:
                        file_path = active_futures[future]
                        logger.error("Unexpected error checking %s: %s",
                                     file_path, e)
                        log.write(f"ERROR: {file_path} - {e}\n")
                        log.flush()
                        checked += 1
                        continue

                    checked += 1

                    processed_log.write(file_path + "\n")
                    if is_corrupt:
                        log.write(f"CORRUPT: {file_path} - {reason}\n")
                    processed_log.flush()
                    log.flush()

                    if is_corrupt:
                        corrupted += 1
                        try:
                            corrupt_size += os.path.getsize(file_path)
                        except OSError:
                            pass
                        _handle_corrupt_file(
                            file_path, reason, mode, input_folder,
                            output_folder, corrupt_log, log,
                            existing_corrupt, corrupt_details)

                    _write_heartbeat(heartbeat_path)

                    if checked % 100 == 0 or checked == total:
                        elapsed = time.time() - start_time
                        rate = checked / elapsed if elapsed > 0 else 0
                        eta = (total - checked) / rate if rate > 0 else 0
                        pct = checked * 100 // total
                        logger.info(
                            "[%d%%] %d/%d checked, %d corrupt, ETA %s",
                            pct, checked, total, corrupted,
                            format_eta(eta))

                    if shutdown_requested:
                        break

                for fut in done:
                    del active_futures[fut]
                if not shutdown_requested:
                    _submit_batch()

        elapsed = time.time() - start_time
        summary = (
            f"\n{'='*60}\n"
            f"Scan finished: {time.strftime('%Y-%m-%d %H:%M:%S')}\n"
            f"Duration: {format_eta(elapsed)}\n"
            f"Library: {len(all_files)} files ({format_size(total_library_size)})\n"
            f"Files checked: {checked}\n"
            f"Corrupted: {corrupted} ({format_size(corrupt_size)})\n"
            f"{'='*60}\n"
        )
        logger.info(summary.strip())
        log.write(summary)
        log.flush()

    output_real = os.path.realpath(output_folder) if output_folder else ""
    corrupt_details = {
        p: r for p, r in corrupt_details.items()
        if os.path.exists(p) or (output_real and
                                 os.path.realpath(p).startswith(output_real))
    }
    write_json_atomic(details_path, corrupt_details)

    summary_path = os.path.join(log_dir, "summary.json")
    summary_data = {
        "version": __version__,
        "finished": time.strftime('%Y-%m-%d %H:%M:%S'),
        "duration": format_eta(elapsed),
        "library_files": len(all_files),
        "library_size": total_library_size,
        "library_size_human": format_size(total_library_size),
        "files_checked": checked,
        "corrupted": corrupted,
        "corrupt_size": corrupt_size,
        "corrupt_size_human": format_size(corrupt_size),
        "mode": mode,
    }
    write_json_atomic(summary_path, summary_data)

    if corrupted > 0:
        logger.info("Corrupt file list: %s", corrupt_list_path)
        logger.info("Review with: cat %s", corrupt_list_path)

    return corrupted


def run_auto_delete(log_dir, log_file, delete_after_days, max_deletes=50,
                    lidarr_url=None, lidarr_api_key=None):
    """Auto-delete corrupt files that have been known for longer than DELETE_AFTER days.
    Aborts if more than max_deletes files would be removed (safety threshold)."""
    tracking_path = os.path.join(log_dir, "corrupt_tracking.json")
    corrupt_list_path = os.path.join(log_dir, "corrupt.txt")

    tracking = {}
    if os.path.exists(tracking_path):
        with open(tracking_path, 'r', encoding='utf-8') as f:
            tracking = json.load(f)

    now = time.strftime('%Y-%m-%dT%H:%M:%S')
    if os.path.exists(corrupt_list_path):
        with open(corrupt_list_path, 'r', encoding='utf-8') as f:
            for line in f:
                path = line.strip()
                if path and path not in tracking:
                    tracking[path] = now

    tracking = {p: t for p, t in tracking.items() if os.path.exists(p)}

    if not tracking:
        write_json_atomic(tracking_path, tracking)
        return

    threshold = time.time() - (delete_after_days * 86400)
    to_delete = []
    to_keep = []
    for path, first_seen in tracking.items():
        try:
            seen_time = time.mktime(time.strptime(first_seen, '%Y-%m-%dT%H:%M:%S'))
        except ValueError:
            to_keep.append(path)
            continue
        if seen_time < threshold:
            to_delete.append(path)
        else:
            to_keep.append(path)

    if not to_delete:
        write_json_atomic(tracking_path, tracking)
        if to_keep:
            logger.info("%d corrupt files still within %d-day review window",
                        len(to_keep), delete_after_days)
        return

    if max_deletes > 0 and len(to_delete) > max_deletes:
        logger.warning(
            "Auto-delete aborted — %d files exceed safety threshold of %d",
            len(to_delete), max_deletes
        )
        logger.warning("This may indicate a filesystem issue. Review corrupt.txt manually.")
        logger.warning("Adjust MAX_AUTO_DELETE to increase the threshold if this is expected.")
        with open(log_file, 'a', encoding='utf-8') as log:
            log.write(
                f"AUTO-DELETE ABORTED: {len(to_delete)} files exceed "
                f"threshold of {max_deletes}\n"
            )
        write_json_atomic(tracking_path, tracking)
        return

    logger.info("Auto-deleting %d corrupt files (older than %d days):",
                len(to_delete), delete_after_days)
    deleted = 0

    if lidarr_url and lidarr_api_key:
        logger.info("  Using Lidarr API for deletion")
        result = _lidarr_delete_corrupt(
            lidarr_url, lidarr_api_key, to_delete, log_file)
        if result is not None:
            deleted = result
            for path in to_delete:
                if not os.path.exists(path):
                    tracking.pop(path, None)
        else:
            logger.warning("  Lidarr integration failed, "
                           "falling back to direct deletion")
            lidarr_url = None

    if not lidarr_url or not lidarr_api_key:
        with open(log_file, 'a', encoding='utf-8') as log:
            log.write(f"\nAuto-delete ({delete_after_days}d threshold): "
                      f"{len(to_delete)} files\n")
            for path in to_delete:
                try:
                    os.remove(path)
                    deleted += 1
                    tracking.pop(path, None)
                    log.write(f"AUTO-DELETED: {path}\n")
                    logger.info("  Deleted: %s", path)
                except OSError as e:
                    log.write(f"ERROR auto-deleting {path}: {e}\n")
                    logger.error("  ERROR: %s - %s", path, e)

    logger.info("  %d/%d files deleted", deleted, len(to_delete))
    if to_keep:
        logger.info("  %d files still within review window", len(to_keep))

    write_json_atomic(tracking_path, tracking)
    with open(corrupt_list_path, 'w', encoding='utf-8') as f:
        for path in tracking:
            f.write(path + "\n")


def setup_logging(log_level, log_file):
    """Configure logging with console and file handlers."""
    level = getattr(logging, log_level.upper(), logging.INFO)

    root = logging.getLogger()
    root.setLevel(level)

    console = logging.StreamHandler(sys.stdout)
    console.setLevel(level)
    console_fmt = logging.Formatter(
        "%(asctime)s | %(levelname)-8s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    console.setFormatter(console_fmt)
    root.addHandler(console)


def _parse_env_int(name, default, label=None):
    """Parse an integer environment variable, exit on error."""
    try:
        val = int(os.environ.get(name, str(default)))
        return val
    except ValueError:
        print(f"Invalid {name} value. Must be an integer"
              f"{' (' + label + ')' if label else ''}.")
        sys.exit(1)


def _parse_env_float(name, default, label=None):
    """Parse a float environment variable, exit on error."""
    try:
        return float(os.environ.get(name, str(default)))
    except ValueError:
        print(f"Invalid {name} value. Must be a number"
              f"{' (' + label + ')' if label else ''}.")
        sys.exit(1)


def _load_config():
    """Load configuration from CLI args or environment variables."""
    if len(sys.argv) == 4:
        input_folder = sys.argv[1].rstrip("/")
        output_folder = sys.argv[2].rstrip("/")
        log_file = sys.argv[3]
        log_dir = os.path.dirname(log_file)
    else:
        input_folder = os.environ.get("MUSIC_DIR", "/music").rstrip("/")
        output_folder = os.environ.get("OUTPUT_DIR", "/corrupted").rstrip("/")
        log_dir = os.environ.get("CONFIG_DIR", "/config").rstrip("/")
        log_file = os.path.join(log_dir, "beats_check.log")

    mode = os.environ.get("MODE", "report").lower()
    if mode not in ("report", "move", "delete"):
        print(f"Invalid MODE '{mode}'. Must be: report, move, delete")
        sys.exit(1)

    workers = _parse_env_int("WORKERS", 4)
    if workers < 1:
        print("Invalid WORKERS value. Must be a positive integer.")
        sys.exit(1)

    return {
        "input_folder": input_folder,
        "output_folder": output_folder,
        "log_dir": log_dir,
        "log_file": log_file,
        "mode": mode,
        "log_level": os.environ.get("LOG_LEVEL", "INFO"),
        "workers": workers,
        "run_interval": _parse_env_float("RUN_INTERVAL", 0, "hours"),
        "delete_after": _parse_env_float("DELETE_AFTER", 0, "days"),
        "max_auto_delete": _parse_env_int("MAX_AUTO_DELETE", 50),
        "min_age_minutes": _parse_env_int("MIN_FILE_AGE", 30, "minutes"),
        "max_log_mb": _parse_env_int("MAX_LOG_MB", 50, "MB"),
        "lidarr_url": os.environ.get("LIDARR_URL", "").rstrip("/"),
        "lidarr_api_key": _load_lidarr_api_key(),
    }


def main():
    signal.signal(signal.SIGTERM, handle_shutdown)
    signal.signal(signal.SIGINT, handle_shutdown)

    try:
        os.nice(10)
    except OSError:
        pass

    cfg = _load_config()
    input_folder = cfg["input_folder"]
    output_folder = cfg["output_folder"]
    log_dir = cfg["log_dir"]
    log_file = cfg["log_file"]
    mode = cfg["mode"]
    workers = cfg["workers"]
    run_interval = cfg["run_interval"]
    delete_after = cfg["delete_after"]
    max_auto_delete = cfg["max_auto_delete"]
    min_age_minutes = cfg["min_age_minutes"]
    max_log_mb = cfg["max_log_mb"]
    lidarr_url = cfg["lidarr_url"]
    lidarr_api_key = cfg["lidarr_api_key"]

    os.makedirs(log_dir, exist_ok=True)

    setup_logging(cfg["log_level"], log_file)

    logger.info("BeatsCheck v%s starting", __version__)
    if lidarr_url and not lidarr_url.startswith(("http://", "https://")):
        logger.error("LIDARR_URL must start with http:// or https://")
        sys.exit(1)

    if lidarr_url and lidarr_api_key:
        logger.info("  Lidarr integration: enabled")
        logger.debug("  Lidarr URL: %s", re.sub(
            r'://.*@', '://****@',
            re.sub(r'(https?://)(.+)', r'\1****', lidarr_url)))
    elif lidarr_url:
        logger.warning("  Lidarr URL set but API key missing — disabled")

    if mode == "delete":
        corrupt_list_path = os.path.join(log_dir, "corrupt.txt")
        run_delete_mode(corrupt_list_path, log_file, log_dir)
        return

    if not os.path.isdir(input_folder):
        logger.error("Music directory not found: %s", input_folder)
        sys.exit(1)

    if mode == "move":
        if not output_folder or output_folder == "/corrupted":
            if not os.path.isdir("/corrupted"):
                logger.error("Move mode requires the Corrupted Output path to be configured.")
                logger.error("Set the OUTPUT_DIR variable or mount a volume to /corrupted.")
                sys.exit(1)
        os.makedirs(output_folder, exist_ok=True)

    while True:
        if max_log_mb > 0 and os.path.exists(log_file):
            try:
                log_size = os.path.getsize(log_file)
                if log_size > max_log_mb * 1024 * 1024:
                    _rotate_file(log_file, keep=3)
                    processed = os.path.join(log_dir, "processed.txt")
                    if os.path.exists(processed):
                        _rotate_file(processed, keep=3)
                    logger.info("Log rotated (%s > %dMB limit). Starting fresh full scan.",
                                format_size(log_size), max_log_mb)
            except OSError:
                pass

        run_scan(input_folder, output_folder, log_file, log_dir, mode, workers,
                 min_age_minutes)

        if delete_after > 0:
            run_auto_delete(
                log_dir, log_file, delete_after, max_auto_delete,
                lidarr_url, lidarr_api_key)

        if shutdown_requested:
            break

        if run_interval <= 0:
            logger.info("Scan complete. Container is idle."
                        " Rescan with: docker exec beatscheck touch /config/.rescan")
            if _idle_wait(log_dir, None):
                continue
            break

        next_run = time.strftime(
            '%Y-%m-%d %H:%M:%S',
            time.localtime(time.time() + run_interval * 3600)
        )
        logger.info(
            "Next scan at %s (%sh interval). Waiting...",
            next_run, run_interval
        )

        if _idle_wait(log_dir, run_interval * 3600):
            logger.info("Rescan triggered.")
        else:
            if shutdown_requested:
                break
            logger.info("Scheduled scan starting.")


if __name__ == "__main__":
    main()
