import concurrent.futures
import fcntl
import json
import logging
import os
import re
import shutil
import signal
import subprocess
import sys
import time
import types
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed

__version__ = "1.0.0"

AUDIO_EXTENSIONS = {
    '.flac', '.mp3', '.m4a', '.ogg', '.opus', '.wav',
    '.wma', '.aac', '.aiff', '.aif', '.ape', '.wv',
    '.alac', '.m4b', '.m4p', '.mp2', '.mpc', '.dsf', '.dff',
}

shutdown_requested = False
scan_cancelled = False
logger = logging.getLogger("beatscheck")


# --- Utilities ---

def handle_shutdown(signum, frame):
    global shutdown_requested
    shutdown_requested = True
    logger.info("Shutdown requested, finishing in-progress files...")


def cancel_scan():
    """Cancel the current scan. Called from WebUI."""
    global scan_cancelled
    scan_cancelled = True
    logger.info("Scan cancel requested.")


def _decode_mountinfo_path(path):
    """Decode octal escapes in /proc/self/mountinfo paths.
    e.g. \\040 -> space, \\011 -> tab."""
    import re as _re
    return _re.sub(r'\\([0-7]{3})',
                   lambda m: chr(int(m.group(1), 8)), path)


def _get_host_mount_path(container_path):
    """Resolve the host bind-mount path for a container mount point
    by reading /proc/self/mountinfo. Returns the host path or None."""
    real = os.path.realpath(container_path)
    try:
        with open("/proc/self/mountinfo", "r") as f:
            best = None
            for line in f:
                parts = line.split()
                if len(parts) < 5:
                    continue
                mount_point = _decode_mountinfo_path(parts[4])
                mount_root = _decode_mountinfo_path(parts[3])
                try:
                    sep = parts.index("-")
                    if sep + 2 < len(parts):
                        mount_source = _decode_mountinfo_path(
                            parts[sep + 2])
                    else:
                        continue
                except (ValueError, IndexError):
                    continue
                if real == mount_point or real.startswith(mount_point + "/"):
                    if best is None or len(mount_point) > len(best[0]):
                        best = (mount_point, mount_source, mount_root)
            if best:
                mount_point, source, root = best
                # For Docker bind mounts, source is the host path
                # For Unraid shfs/fuse, source is the fs name —
                # use mount root which has the subpath
                if source.startswith("/"):
                    rel = real[len(mount_point):].lstrip("/")
                    if root and root != "/":
                        return os.path.join(
                            source, root.lstrip("/"), rel)
                    return os.path.join(source, rel) if rel else source
                elif root and root != "/":
                    # Unraid: root has the path (e.g. /data/media/music)
                    return root
    except OSError:
        pass
    return None


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


# --- File I/O ---

def write_json_atomic(path, data):
    """Write JSON data atomically using a temp file + rename."""
    tmp_path = path + ".tmp"
    with open(tmp_path, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2)
    os.rename(tmp_path, path)


def _load_json(path, default=None):
    """Load a JSON file, returning default if missing or invalid."""
    if default is None:
        default = {}
    if not os.path.exists(path):
        return default
    try:
        with open(path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except (json.JSONDecodeError, ValueError):
        logger.warning("Corrupt JSON file %s — using defaults", path)
        return default


def _load_lines_as_set(path):
    """Load a text file as a set of non-empty stripped lines."""
    if not os.path.exists(path):
        return set()
    with open(path, 'r', encoding='utf-8') as f:
        return {line.strip() for line in f if line.strip()}


def delete_corrupt_files(paths, config_dir, music_dir=None):
    """Delete corrupt files with Lidarr support and full state cleanup.

    Validates paths, routes Lidarr-tracked files through the Lidarr API,
    and updates corrupt.txt, corrupt_details.json, and corrupt_tracking.json.

    Args:
        paths: List of file paths to delete.
        config_dir: Directory containing state files.
        music_dir: If set, rejects paths outside this directory.

    Returns:
        dict with 'deleted' (list), 'errors' (list of dicts), 'count' (int).
    """
    corrupt_path = os.path.join(config_dir, "corrupt.txt")
    details_path = os.path.join(config_dir, "corrupt_details.json")
    tracking_path = os.path.join(config_dir, "corrupt_tracking.json")
    log_file = os.path.join(config_dir, "beats_check.log")

    # Load allowlist from corrupt.txt
    allowed = _load_lines_as_set(corrupt_path)

    # Resolve music_dir realpath once for containment checks
    music_real = os.path.realpath(music_dir) if music_dir else None

    # Validate all paths first
    validated = []
    errors = []
    for fp in paths:
        if fp not in allowed:
            errors.append({"path": fp, "error": "not in corrupt list"})
            continue
        if os.path.islink(fp):
            errors.append({"path": fp, "error": "symlink rejected"})
            continue
        real = os.path.realpath(fp)
        if music_real and not real.startswith(music_real + os.sep):
            errors.append({"path": fp, "error": "outside music directory"})
            continue
        if not os.path.isfile(real):
            errors.append({"path": fp, "error": "not found"})
            continue
        validated.append(fp)

    if not validated:
        return {"deleted": [], "errors": errors, "count": 0}

    # Load Lidarr config and corrupt details
    lidarr_url = os.environ.get("LIDARR_URL", "").rstrip("/")
    lidarr_key = _load_lidarr_api_key()
    lidarr_blocklist = _parse_env_bool("LIDARR_BLOCKLIST", False)
    details = _load_json(details_path)

    # Split into Lidarr-tracked and non-Lidarr paths
    lidarr_paths = []
    direct_paths = []
    for fp in validated:
        detail = details.get(fp, {})
        if (lidarr_url and lidarr_key
                and isinstance(detail, dict)
                and "trackfileId" in detail):
            lidarr_paths.append(fp)
        else:
            direct_paths.append(fp)

    deleted = []

    # Delete Lidarr-tracked files via API (search=False for prompt return)
    if lidarr_paths:
        count, _ = _lidarr_delete_corrupt(
            lidarr_url, lidarr_key, lidarr_paths, log_file,
            log_dir=config_dir, blocklist=lidarr_blocklist,
            search=False)
        if count > 0:
            # Check which files were actually removed
            for fp in lidarr_paths:
                if not os.path.exists(fp):
                    deleted.append(fp)
                else:
                    errors.append({"path": fp,
                                   "error": "Lidarr delete failed"})
        else:
            # API reported failure but may have partially succeeded
            for fp in lidarr_paths:
                if not os.path.exists(fp):
                    deleted.append(fp)
                else:
                    errors.append(
                        {"path": fp,
                         "error": "Lidarr API delete failed"})

    # Direct delete for non-Lidarr files
    for fp in direct_paths:
        try:
            os.remove(fp)
            deleted.append(fp)
        except OSError as e:
            errors.append({"path": fp, "error": str(e)})

    # Update all state files
    if deleted:
        deleted_set = set(deleted)
        remaining = sorted(allowed - deleted_set)
        # Rewrite corrupt.txt atomically
        tmp = corrupt_path + ".tmp"
        try:
            with open(tmp, 'w', encoding='utf-8') as f:
                for p in remaining:
                    f.write(p + '\n')
            os.rename(tmp, corrupt_path)
        except OSError:
            pass

        # Remove deleted entries from corrupt_details.json
        for fp in deleted:
            details.pop(fp, None)
        write_json_atomic(details_path, details)

        # Remove deleted entries from corrupt_tracking.json
        tracking = _load_json(tracking_path)
        for fp in deleted:
            tracking.pop(fp, None)
        write_json_atomic(tracking_path, tracking)

    return {"deleted": deleted, "errors": errors, "count": len(deleted)}


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


def _total_file_size(files):
    """Sum file sizes, ignoring missing files."""
    total = 0
    for f in files:
        try:
            total += os.path.getsize(f)
        except OSError:
            pass
    return total


# --- Heartbeat & idle ---

def _write_heartbeat(heartbeat_path):
    """Write current timestamp to heartbeat file for healthcheck."""
    try:
        with open(heartbeat_path, 'w') as f:
            f.write(str(int(time.time())))
    except OSError:
        pass


def _read_rescan_trigger(log_dir):
    """Check for .rescan file. Returns mode override string or empty string."""
    rescan_path = os.path.join(log_dir, ".rescan")
    if not os.path.exists(rescan_path):
        return None
    try:
        with open(rescan_path, 'r') as f:
            content = f.read().strip()
        os.remove(rescan_path)
    except OSError:
        content = ""
    return content


def _idle_wait(log_dir, timeout_seconds, lidarr_url=None, lidarr_api_key=None):
    """Sleep until timeout, shutdown, or .rescan trigger.
    Drains the Lidarr search queue during idle (max 5/hour).
    Returns mode override string if rescan triggered, False otherwise."""
    heartbeat_path = os.path.join(log_dir, ".heartbeat")
    deadline = time.time() + timeout_seconds if timeout_seconds is not None else None
    last_search_time = 0
    while not shutdown_requested:
        _write_heartbeat(heartbeat_path)
        trigger = _read_rescan_trigger(log_dir)
        if trigger is not None:
            logger.info("Rescan requested.")
            return trigger if trigger else True
        if deadline and time.time() >= deadline:
            return False
        # Drain search queue: 1 album every 720s (5/hour)
        if (lidarr_url and lidarr_api_key
                and time.time() - last_search_time >= 720):
            if _search_queue_drain_one(log_dir, lidarr_url, lidarr_api_key):
                last_search_time = time.time()
        time.sleep(30)
    return False


# --- Scan lock ---

def _acquire_scan_lock(log_dir):
    """Acquire an exclusive file lock for scanning. Returns the lock fd."""
    lock_path = os.path.join(log_dir, ".scanning")
    lf = open(lock_path, 'w')
    try:
        fcntl.flock(lf.fileno(), fcntl.LOCK_EX)
    except OSError:
        lf.close()
        raise
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


# --- Scanning ---

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


def _handle_corrupt_file(file_path, reason, mode, input_folder, output_folder,
                         corrupt_log, log, existing_corrupt, corrupt_details,
                         log_dir=None):
    logger.info("CORRUPT: %s", file_path)
    logger.debug("         %s", reason)

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
    corrupt_details[file_path] = {"reason": reason}

    # Write details incrementally so WebUI shows reasons in real time
    if log_dir:
        details_path = os.path.join(log_dir, "corrupt_details.json")
        write_json_atomic(details_path, corrupt_details)

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
            corrupt_details[dest] = {"reason": reason}
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
    host_path = _get_host_mount_path(input_folder)
    if host_path:
        logger.debug("  Music:   %s (host: %s)", input_folder, host_path)
    else:
        logger.debug("  Music:   %s", input_folder)
    logger.debug("  Log:     %s", log_file)
    logger.debug("  Corrupt: %s", corrupt_list_path)
    if mode == "move":
        logger.debug("  Output:  %s", output_folder)
    if mode == "report":
        logger.info("  (report mode - no files will be moved)")


def _finalize_scan(log_dir, corrupt_list_path, corrupt_details,
                   output_folder, scan_stats,
                   lidarr_url=None, lidarr_api_key=None):
    """Post-scan cleanup: update state files and write summary."""
    details_path = os.path.join(log_dir, "corrupt_details.json")

    # Prune corrupt_details to existing files + moved files in output dir
    output_real = os.path.realpath(output_folder) if output_folder else ""
    corrupt_details = {
        p: r for p, r in corrupt_details.items()
        if os.path.exists(p) or (output_real and
                                 os.path.realpath(p).startswith(output_real))
    }

    # Resolve Lidarr trackfile/album IDs for corrupt files
    if lidarr_url and lidarr_api_key and corrupt_details:
        _resolve_lidarr_ids(corrupt_details, lidarr_url, lidarr_api_key)

    write_json_atomic(details_path, corrupt_details)

    # Clean stale entries from corrupt.txt (e.g. files moved in move mode)
    current_corrupt = _load_lines_as_set(corrupt_list_path)
    live_corrupt = [p for p in current_corrupt if os.path.exists(p)]
    with open(corrupt_list_path, 'w', encoding='utf-8') as f:
        for p in live_corrupt:
            f.write(p + "\n")

    # Write machine-readable summary
    summary_path = os.path.join(log_dir, "summary.json")
    write_json_atomic(summary_path, scan_stats)

    if scan_stats["corrupted"] > 0:
        logger.info("Corrupt file list: %s", corrupt_list_path)
        logger.info("Review with: cat %s", corrupt_list_path)


def _run_scan_inner(input_folder, output_folder, log_file, log_dir,
                    mode, workers, corrupt_list_path, min_age_minutes,
                    lidarr_url=None, lidarr_api_key=None):
    """Inner scan logic."""
    already_processed = _load_lines_as_set(os.path.join(log_dir, "processed.txt"))
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
        existing_details = _load_json(
            os.path.join(log_dir, "corrupt_details.json"))
        _finalize_scan(log_dir, corrupt_list_path, existing_details,
                       output_folder, {
                           "version": __version__,
                           "finished": time.strftime('%Y-%m-%d %H:%M:%S'),
                           "duration": "0s",
                           "library_files": len(all_files),
                           "library_size": total_library_size,
                           "library_size_human": format_size(total_library_size),
                           "files_checked": 0,
                           "corrupted": 0,
                           "corrupt_size": 0,
                           "corrupt_size_human": format_size(0),
                           "mode": mode,
                       }, lidarr_url, lidarr_api_key)
        return 0

    checked = 0
    corrupted = 0
    corrupt_size = 0
    start_time = time.time()

    corrupt_details = _load_json(
        os.path.join(log_dir, "corrupt_details.json"))
    existing_corrupt = _load_lines_as_set(corrupt_list_path)

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

        max_pending = workers * 4
        pending = {}

        def _process_future(future):
            nonlocal checked, corrupted, corrupt_size
            file_path = pending.pop(future)
            try:
                file_path, is_corrupt, reason = future.result()
            except Exception as e:
                logger.error("Unexpected error checking %s: %s",
                             file_path, e)
                log.write(f"ERROR: {file_path} - {e}\n")
                log.flush()
                checked += 1
                return

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
                    existing_corrupt, corrupt_details,
                    log_dir=log_dir)

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

            # Update WebUI progress (every 10 files to reduce overhead)
            if checked % 10 == 0 or checked == total:
                _webui_progress(checked, total, corrupted, file_path)

        with ThreadPoolExecutor(max_workers=workers) as pool:
            for f in files_to_check:
                if shutdown_requested or scan_cancelled:
                    remaining = sum(1 for ft in pending
                                    if ft.running())
                    logger.info(
                        "Cancelling — waiting for %d in-progress "
                        "file(s) to finish.", remaining)
                    pool.shutdown(wait=True, cancel_futures=True)
                    # Process results from completed futures
                    for fut in list(pending):
                        if fut.done() and not fut.cancelled():
                            _process_future(fut)
                    break
                pending[pool.submit(check_audio_file, f)] = f
                # Drain completed futures to bound memory usage
                while len(pending) >= max_pending:
                    done, _ = concurrent.futures.wait(
                        pending, return_when=concurrent.futures.FIRST_COMPLETED)
                    for fut in done:
                        _process_future(fut)
                    if shutdown_requested or scan_cancelled:
                        break
            else:
                # Normal completion — drain remaining futures
                for future in as_completed(pending.copy()):
                    if shutdown_requested or scan_cancelled:
                        remaining = sum(1 for ft in pending
                                        if ft.running())
                        logger.info(
                            "Cancelling — waiting for %d "
                            "in-progress file(s) to finish.",
                            remaining)
                        pool.shutdown(wait=True, cancel_futures=True)
                        for fut in list(pending):
                            if fut.done() and not fut.cancelled():
                                _process_future(fut)
                        break
                    _process_future(future)

        elapsed = time.time() - start_time

        if scan_cancelled or shutdown_requested:
            summary = (
                f"\n{'='*60}\n"
                f"Scan cancelled: {time.strftime('%Y-%m-%d %H:%M:%S')}\n"
                f"Duration: {format_eta(elapsed)}\n"
                f"Library: {len(all_files)} files"
                f" ({format_size(total_library_size)})\n"
                f"Files checked: {checked} of {total}\n"
                f"Corrupted: {corrupted}"
                f" ({format_size(corrupt_size)})\n"
                f"{'='*60}\n"
            )
            logger.info(summary.strip())
            log.write(summary)
            log.flush()
            return corrupted

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

    _finalize_scan(log_dir, corrupt_list_path, corrupt_details,
                   output_folder, {
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
                   }, lidarr_url, lidarr_api_key)

    return corrupted


def run_scan(input_folder, output_folder, log_file, log_dir, mode, workers,
             min_age_minutes=30, lidarr_url=None, lidarr_api_key=None):
    """Scan mode: decode-test all audio files with parallel workers."""
    corrupt_list_path = os.path.join(log_dir, "corrupt.txt")

    lock_fd = _acquire_scan_lock(log_dir)
    try:
        return _run_scan_inner(input_folder, output_folder, log_file, log_dir,
                               mode, workers, corrupt_list_path,
                               min_age_minutes, lidarr_url, lidarr_api_key)
    finally:
        try:
            fcntl.flock(lock_fd.fileno(), fcntl.LOCK_UN)
            lock_fd.close()
            os.remove(os.path.join(log_dir, ".scanning"))
        except OSError:
            pass


# --- Delete mode ---

def _get_detail_reason(corrupt_details, path):
    """Extract reason string from corrupt_details (handles old/new format)."""
    val = corrupt_details.get(path, "")
    if isinstance(val, dict):
        return val.get("reason", "")
    return val


def _display_folder_files(corrupt_files, corrupt_details):
    """Display corrupt files in a folder with reasons and sizes."""
    for cf in corrupt_files:
        name = os.path.basename(cf)
        reason = _get_detail_reason(corrupt_details, cf)
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


def _handle_folder_action(choice, folder, existing, log,
                          input_folder=None, corrupt_details=None,
                          lidarr_url=None, lidarr_api_key=None,
                          lidarr_blocklist=False):
    """Execute a delete action on a folder.
    Returns (folders_del, files_del, skipped, album_ids).
    When Lidarr is configured, deletes via API using stored trackfile IDs."""
    album_ids = set()

    if choice == 'y' and os.path.isdir(folder):
        if lidarr_url and lidarr_api_key and corrupt_details:
            return _handle_folder_delete_lidarr(
                folder, existing, log, input_folder, corrupt_details,
                lidarr_url, lidarr_api_key, lidarr_blocklist)

        # No Lidarr — direct filesystem delete
        if folder == input_folder:
            # Never rmtree the mount root — delete files individually
            deleted = 0
            for entry in os.listdir(folder):
                fp = os.path.join(folder, entry)
                try:
                    if os.path.isfile(fp):
                        os.remove(fp)
                        deleted += 1
                except OSError as e:
                    print(f"           ERROR: {os.path.basename(fp)}: {e}")
                    log.write(f"ERROR deleting {fp}: {e}\n")
            log.write(f"DELETED FOLDER CONTENTS: {folder} "
                      f"({deleted} files)\n")
            print(f"           -> {deleted} files deleted "
                  f"(mount root preserved)\n")
            return (1, deleted, 0, album_ids)
        try:
            shutil.rmtree(folder)
            log.write(f"DELETED FOLDER: {folder} "
                      f"({len(existing)} corrupt files)\n")
            print("           -> Folder deleted\n")
            return (1, len(existing), 0, album_ids)
        except OSError as e:
            print(f"           ERROR: {e}\n")
            log.write(f"ERROR deleting folder {folder}: {e}\n")
            return (0, 0, 0, album_ids)

    elif choice == 'f':
        if lidarr_url and lidarr_api_key and corrupt_details:
            return _handle_files_delete_lidarr(
                existing, log, corrupt_details,
                lidarr_url, lidarr_api_key, lidarr_blocklist)

        # No Lidarr — direct filesystem delete
        deleted = 0
        for cf in existing:
            try:
                os.remove(cf)
                deleted += 1
                log.write(f"DELETED FILE: {cf}\n")
            except OSError as e:
                print(f"           ERROR deleting "
                      f"{os.path.basename(cf)}: {e}")
                log.write(f"ERROR deleting {cf}: {e}\n")
        print(f"           -> {deleted} corrupt files deleted\n")
        return (0, deleted, 0, album_ids)

    else:
        log.write(f"SKIPPED: {folder}\n")
        print()
        return (0, 0, 1, album_ids)


def _handle_files_delete_lidarr(file_paths, log, corrupt_details,
                                lidarr_url, lidarr_api_key,
                                lidarr_blocklist):
    """Delete specific files via Lidarr API, one album at a time.
    Returns (folders_del, files_del, skipped, album_ids)."""
    # Group by album
    album_to_tfids = {}
    direct_paths = []

    for fp in file_paths:
        detail = corrupt_details.get(fp, {})
        if isinstance(detail, dict) and "trackfileId" in detail:
            aid = detail.get("albumId", 0)
            if aid not in album_to_tfids:
                album_to_tfids[aid] = []
            album_to_tfids[aid].append(detail["trackfileId"])
        else:
            direct_paths.append(fp)

    deleted = 0
    album_ids = set()
    total = len(album_to_tfids)

    for i, (album_id, tf_ids) in enumerate(
            album_to_tfids.items(), 1):
        # Blocklist before deletion
        if lidarr_blocklist:
            bl_ok, bl_fail = _lidarr_blocklist_albums(
                lidarr_url, lidarr_api_key, [album_id])
            if bl_fail:
                print(f"           -> ERROR: Blocklist failed for "
                      f"album {album_id}. Skipping.\n")
                log.write(f"ERROR: Blocklist failed for album "
                          f"{album_id} — skipped\n")
                continue
            if bl_ok:
                print(f"           -> Lidarr: blocklisted album "
                      f"[{i}/{total}]")

        # Delete this album's trackfiles
        result = _lidarr_delete_trackfiles_bulk(
            lidarr_url, lidarr_api_key, tf_ids)
        if result is not None:
            deleted += len(tf_ids)
            album_ids.add(album_id)
            log.write(f"LIDARR DELETE: album {album_id} — "
                      f"{len(tf_ids)} track files\n")
            album = _lidarr_get_album(
                lidarr_url, lidarr_api_key, album_id)
            monitored = album.get("monitored", True) if album else True
            if monitored:
                print(f"           -> [{i}/{total}] Deleted "
                      f"{len(tf_ids)} trackfiles — waiting "
                      f"for Lidarr search")
                msg = _lidarr_wait_for_search(
                    lidarr_url, lidarr_api_key)
                if msg:
                    print(f"           -> [{i}/{total}] {msg}")
                else:
                    print(f"           -> [{i}/{total}] "
                          f"Search complete")
            else:
                print(f"           -> [{i}/{total}] Deleted "
                      f"{len(tf_ids)} trackfiles (unmonitored"
                      f" — Lidarr will not re-download)")
        else:
            print(f"           -> ERROR: Bulk delete failed for "
                  f"album {album_id}. Skipping.\n")
            log.write(f"ERROR: Bulk delete failed for album "
                      f"{album_id}\n")

    # Direct delete for files not tracked by Lidarr
    for fp in direct_paths:
        try:
            os.remove(fp)
            deleted += 1
            log.write(f"DIRECT DELETE (not in Lidarr): {fp}\n")
        except OSError as e:
            print(f"           ERROR: {os.path.basename(fp)}: {e}")
            log.write(f"ERROR deleting {fp}: {e}\n")

    print(f"           -> {deleted} files deleted\n")
    return (0, deleted, 0, album_ids)


def _resolve_folder_trackfiles(folder, existing, corrupt_details,
                               album_ids, lidarr_url, lidarr_api_key):
    """Build trackfile ID map for all files in a folder.
    Uses stored IDs from corrupt_details, fetches remaining from Lidarr.
    Returns (all_files, file_to_tfid, unmatched) or None on error."""
    all_files = []
    try:
        for entry in os.listdir(folder):
            fp = os.path.join(folder, entry)
            if os.path.isfile(fp):
                all_files.append(fp)
    except OSError:
        return None

    # Start with stored IDs from corrupt_details
    file_to_tfid = {}
    for fp in all_files:
        detail = corrupt_details.get(fp, {})
        if isinstance(detail, dict) and "trackfileId" in detail:
            file_to_tfid[fp] = detail["trackfileId"]

    # Fetch remaining trackfile IDs from Lidarr by album
    unmatched = [fp for fp in all_files if fp not in file_to_tfid]
    if unmatched and album_ids:
        for aid in album_ids:
            album_tfs = _lidarr_get_trackfiles_by_album(
                lidarr_url, lidarr_api_key, aid)
            for tf in album_tfs:
                for fp in unmatched:
                    score = _suffix_match_path(fp, tf["path"])
                    if score >= 1:
                        file_to_tfid[fp] = tf["id"]
                        if tf.get("albumId"):
                            album_ids.add(tf["albumId"])
                        break
        unmatched = [fp for fp in all_files if fp not in file_to_tfid]

    return all_files, file_to_tfid, unmatched


def _handle_folder_delete_lidarr(folder, existing, log, input_folder,
                                 corrupt_details, lidarr_url,
                                 lidarr_api_key, lidarr_blocklist):
    """Delete entire folder via Lidarr API.
    Returns (folders_del, files_del, skipped, album_ids)."""
    # Collect album IDs from corrupt file details
    album_ids = set()
    for cf in existing:
        detail = corrupt_details.get(cf, {})
        if isinstance(detail, dict) and detail.get("albumId"):
            album_ids.add(detail["albumId"])

    resolved = _resolve_folder_trackfiles(
        folder, existing, corrupt_details, album_ids,
        lidarr_url, lidarr_api_key)
    if resolved is None:
        print("           ERROR listing folder\n")
        log.write(f"ERROR listing {folder}\n")
        return (0, 0, 0, album_ids)

    all_files, file_to_tfid, direct_paths = resolved
    tf_ids = list(file_to_tfid.values())
    deleted = 0

    # Blocklist before deletion
    if lidarr_blocklist and album_ids:
        bl_ok, bl_fail = _lidarr_blocklist_albums(
            lidarr_url, lidarr_api_key, album_ids)
        if bl_fail:
            print(f"           -> ERROR: Blocklist failed for "
                  f"{bl_fail} albums. Delete aborted.\n")
            log.write(f"ERROR: Blocklist failed for {bl_fail} albums "
                      f"— delete aborted\n")
            return (0, 0, 0, set())
        if bl_ok:
            log.write(f"LIDARR BLOCKLIST: {bl_ok} albums\n")
            print(f"           -> Lidarr: blocklisted {bl_ok} albums")

    # Bulk delete via Lidarr API
    if tf_ids:
        result = _lidarr_delete_trackfiles_bulk(
            lidarr_url, lidarr_api_key, tf_ids)
        if result is not None:
            deleted += len(tf_ids)
            log.write(f"LIDARR DELETE: {len(tf_ids)} track files "
                      f"({len(album_ids)} albums)\n")
            # Check monitored status and wait for search
            has_monitored = False
            for aid in album_ids:
                album = _lidarr_get_album(
                    lidarr_url, lidarr_api_key, aid)
                monitored = album.get("monitored", True) if album else True
                if monitored:
                    has_monitored = True
                else:
                    print(f"           -> Lidarr: album {aid} "
                          f"unmonitored — will not re-download")
            print(f"           -> Lidarr: deleted {len(tf_ids)} "
                  f"track files ({len(album_ids)} albums)")
            if has_monitored:
                print("           -> Waiting for Lidarr search")
                msg = _lidarr_wait_for_search(
                    lidarr_url, lidarr_api_key)
                if msg:
                    print(f"           -> {msg}")
                else:
                    print("           -> Search complete")
        else:
            print("           -> ERROR: Lidarr bulk delete failed. "
                  "Check Lidarr logs.\n")
            log.write("ERROR: Lidarr bulk delete API failed\n")
            return (0, 0, 0, set())

    # Direct delete for non-Lidarr files (non-audio, etc.)
    for fp in direct_paths:
        try:
            os.remove(fp)
            deleted += 1
            log.write(f"DIRECT DELETE (not in Lidarr): {fp}\n")
        except OSError as e:
            print(f"           ERROR: {os.path.basename(fp)}: {e}")
            log.write(f"ERROR deleting {fp}: {e}\n")

    # Remove empty folder (but never the mount root)
    if folder != input_folder and os.path.isdir(folder):
        try:
            remaining_entries = os.listdir(folder)
            if not remaining_entries:
                os.rmdir(folder)
                log.write(f"REMOVED EMPTY FOLDER: {folder}\n")
        except OSError:
            pass

    print(f"           -> {deleted} files deleted\n")
    return (1, deleted, 0, album_ids)


def _load_corrupt_file_list(corrupt_list_path, log_dir):
    """Load and deduplicate corrupt files, group by folder."""
    with open(corrupt_list_path, 'r', encoding='utf-8') as f:
        all_paths = [line.strip() for line in f if line.strip()]

    corrupt_details = _load_json(os.path.join(log_dir, "corrupt_details.json"))

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


def _run_interactive_delete(folders, total_folders, corrupt_details,
                            log_file, files, corrupt_list_path,
                            input_folder=None, lidarr_url=None,
                            lidarr_api_key=None, lidarr_blocklist=False):
    """Interactive per-folder delete loop.
    Returns list of affected album IDs (for Lidarr search queueing)."""
    print("\nFor each folder:")
    print("  [y] delete entire folder (album)    [f] delete corrupt files only")
    print("  [n] skip                             [a] delete all remaining folders")
    print("  [q] quit\n")

    folders_deleted = 0
    files_deleted = 0
    skipped_folders = 0
    missing_files = 0
    delete_all = False
    all_album_ids = set()

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

            fd, fid, sk, aids = _handle_folder_action(
                choice, folder, existing, log,
                input_folder, corrupt_details,
                lidarr_url, lidarr_api_key, lidarr_blocklist)
            folders_deleted += fd
            files_deleted += fid
            skipped_folders += sk
            all_album_ids.update(aids)

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

    return list(all_album_ids)


def run_mass_delete(files, log_file, log_dir, corrupt_details=None,
                    lidarr_url=None, lidarr_api_key=None,
                    lidarr_blocklist=False):
    """Delete all corrupt files without prompts.
    Routes through Lidarr API when configured."""
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
            f"\nConfirm: delete {len(existing)} files "
            f"({format_size(total_size)})? [yes/no] "
        ).strip().lower()
    except EOFError:
        print("\nNo input available.")
        return

    if confirm != "yes":
        print("Cancelled.")
        return

    print()

    if lidarr_url and lidarr_api_key and corrupt_details:
        result = _lidarr_delete_corrupt(
            lidarr_url, lidarr_api_key, existing, log_file,
            log_dir=log_dir, blocklist=lidarr_blocklist)
        if result is not None:
            deleted, album_ids = result
            print(f"\n{deleted}/{len(existing)} files deleted")
            remaining = [f for f in files if os.path.exists(f)]
            with open(corrupt_list_path, 'w', encoding='utf-8') as f:
                for fp in remaining:
                    f.write(fp + "\n")
            if not remaining:
                print("corrupt.txt cleared.")
            return album_ids

    # Fallback: direct filesystem delete
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

        log.write(f"Mass delete complete: "
                  f"{deleted}/{len(existing)} deleted\n")

    print(f"\n{deleted}/{len(existing)} files deleted")

    remaining = [f for f in files if os.path.exists(f)]
    with open(corrupt_list_path, 'w', encoding='utf-8') as f:
        for fp in remaining:
            f.write(fp + "\n")
    if not remaining:
        print("corrupt.txt cleared.")
    return None


def run_delete_mode(corrupt_list_path, log_file, log_dir,
                    input_folder=None, lidarr_url=None,
                    lidarr_api_key=None, lidarr_blocklist=False):
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
        run_mass_delete(files, log_file, log_dir, corrupt_details,
                        lidarr_url, lidarr_api_key, lidarr_blocklist)
        return
    if action != 'i':
        print("Invalid choice.")
        return

    _run_interactive_delete(
        folders, total_folders, corrupt_details,
        log_file, files, corrupt_list_path,
        input_folder, lidarr_url, lidarr_api_key, lidarr_blocklist)


# --- Auto-delete ---

def run_auto_delete(log_dir, log_file, delete_after_days, max_deletes=50,
                    lidarr_url=None, lidarr_api_key=None,
                    lidarr_search=False, lidarr_blocklist=False):
    """Auto-delete corrupt files that have been known for longer than DELETE_AFTER days.
    Aborts if more than max_deletes files would be removed (safety threshold)."""
    tracking_path = os.path.join(log_dir, "corrupt_tracking.json")
    corrupt_list_path = os.path.join(log_dir, "corrupt.txt")

    tracking = _load_json(tracking_path)

    now = time.strftime('%Y-%m-%dT%H:%M:%S')
    for path in _load_lines_as_set(corrupt_list_path):
        if path not in tracking:
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
    direct_delete_paths = []

    if lidarr_url and lidarr_api_key:
        logger.info("  Using Lidarr API for deletion")
        result = _lidarr_delete_corrupt(
            lidarr_url, lidarr_api_key, to_delete, log_file,
            log_dir=log_dir, blocklist=lidarr_blocklist)
        if result is not None:
            deleted, album_ids = result
            for path in to_delete:
                if not os.path.exists(path):
                    tracking.pop(path, None)
            if lidarr_search and album_ids:
                # Only queue search for unmonitored albums —
                # Lidarr auto-searches monitored ones after deletion
                unmonitored = []
                for aid in album_ids:
                    album = _lidarr_get_album(
                        lidarr_url, lidarr_api_key, aid)
                    if album and not album.get("monitored", True):
                        unmonitored.append(aid)
                if unmonitored:
                    _search_queue_add(log_dir, unmonitored)
                    logger.info("  Queued search for %d "
                                "unmonitored albums",
                                len(unmonitored))
        else:
            logger.error("  Lidarr API failed — auto-delete aborted. "
                         "Files will be retried next run.")
    else:
        direct_delete_paths = to_delete

    if direct_delete_paths:
        with open(log_file, 'a', encoding='utf-8') as log:
            log.write(f"\nAuto-delete ({delete_after_days}d threshold): "
                      f"{len(direct_delete_paths)} files\n")
            for path in direct_delete_paths:
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


# --- Lidarr ---

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
    except json.JSONDecodeError:
        logger.error("Lidarr API %s /api/%s -> non-JSON response",
                     method, api_path)
        return None
    except (urllib.error.URLError, OSError) as e:
        # Log error type only — exception may contain hostname
        logger.error("Lidarr API connection failed (%s /api/%s): %s",
                     method, api_path, type(e).__name__)
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


def _suffix_match_path(container_path, lidarr_path):
    """Check if two paths refer to the same file by comparing path
    components from the right. Returns the number of matching suffix
    components (0 = no match, higher = more specific match)."""
    c_parts = container_path.replace("\\", "/").split("/")
    l_parts = lidarr_path.replace("\\", "/").split("/")
    # Compare from the right — filename must match at minimum
    matches = 0
    for cp, lp in zip(reversed(c_parts), reversed(l_parts)):
        if cp == lp:
            matches += 1
        else:
            break
    return matches


def _resolve_lidarr_ids(corrupt_details, base_url, api_key):
    """Post-scan: enrich corrupt_details with Lidarr trackfileId and albumId.
    Uses suffix matching to handle different container mount paths."""
    unresolved = [p for p, v in corrupt_details.items()
                  if isinstance(v, dict) and "trackfileId" not in v]
    if not unresolved:
        return

    artists = _lidarr_get_artists(base_url, api_key)
    if not artists:
        logger.warning("Lidarr: could not fetch artists for ID resolution")
        return

    # Pre-filter: only fetch trackfiles for artists that could match.
    # Build set of corrupt file basenames for quick lookup.
    unresolved_basenames = {os.path.basename(p) for p in unresolved}
    remaining = set(unresolved)

    # Build trackfile map incrementally — stop when all resolved
    all_trackfiles = {}
    artists_checked = 0
    for artist in artists:
        if not remaining:
            break
        trackfiles = _lidarr_get_trackfiles(base_url, api_key, artist["id"])
        artists_checked += 1
        # Only index this artist's trackfiles if any basename matches
        artist_basenames = {os.path.basename(tf["path"]) for tf in trackfiles}
        if not artist_basenames & unresolved_basenames:
            continue
        for tf in trackfiles:
            all_trackfiles[tf["path"]] = {
                "id": tf["id"],
                "albumId": tf["albumId"],
                "artistId": artist["id"],
            }
        # Try to resolve after each relevant artist
        newly_matched = []
        for container_path in list(remaining):
            best_match = None
            best_score = 0
            best_lidarr_path = None
            ambiguous = False
            for lidarr_path, tf_info in all_trackfiles.items():
                score = _suffix_match_path(container_path, lidarr_path)
                if score > best_score:
                    best_score = score
                    best_match = tf_info
                    best_lidarr_path = lidarr_path
                    ambiguous = False
                elif score == best_score and score >= 1:
                    ambiguous = True
            if best_match and best_score >= 2 and not ambiguous:
                # Strong match (filename + folder) — resolve now
                newly_matched.append(
                    (container_path, best_match, best_lidarr_path))
                remaining.discard(container_path)
        for container_path, match, lpath in newly_matched:
            detail = corrupt_details[container_path]
            detail["trackfileId"] = match["id"]
            detail["albumId"] = match["albumId"]
            detail["artistId"] = match["artistId"]
            detail["lidarrPath"] = lpath

    # Second pass for remaining: accept score==1 if unambiguous
    matched = len(unresolved) - len(remaining)
    for container_path in list(remaining):
        best_match = None
        best_score = 0
        best_lidarr_path = None
        ambiguous = False
        for lidarr_path, tf_info in all_trackfiles.items():
            score = _suffix_match_path(container_path, lidarr_path)
            if score > best_score:
                best_score = score
                best_match = tf_info
                best_lidarr_path = lidarr_path
                ambiguous = False
            elif score == best_score and score >= 1:
                ambiguous = True
        if best_match and best_score >= 1 and not ambiguous:
            detail = corrupt_details[container_path]
            detail["trackfileId"] = best_match["id"]
            detail["albumId"] = best_match["albumId"]
            detail["artistId"] = best_match["artistId"]
            detail["lidarrPath"] = best_lidarr_path
            matched += 1
            remaining.discard(container_path)
        elif ambiguous:
            logger.debug("  Lidarr: ambiguous match for %s "
                         "(multiple trackfiles with same name)",
                         os.path.basename(container_path))

    if matched:
        logger.info("  Lidarr: resolved %d/%d corrupt files to "
                    "trackfile IDs (checked %d/%d artists)",
                    matched, len(unresolved),
                    artists_checked, len(artists))
    elif unresolved:
        logger.warning("  Lidarr: could not match any corrupt files "
                       "to Lidarr trackfiles")


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


def _lidarr_get_trackfiles_by_album(base_url, api_key, album_id):
    """Fetch all track files for an album."""
    url = f"{base_url}/api/v1/trackfile?albumId={album_id}"
    result = _lidarr_request(url, api_key)
    if result is None:
        return []
    return [{"id": tf["id"], "path": tf["path"],
             "albumId": tf.get("albumId", 0)} for tf in result]


def _lidarr_get_album(base_url, api_key, album_id):
    """Fetch album details. Returns dict with 'monitored' field, or None."""
    url = f"{base_url}/api/v1/album/{album_id}"
    return _lidarr_request(url, api_key)


def _lidarr_delete_trackfiles_bulk(base_url, api_key, track_file_ids):
    """Bulk delete track files via Lidarr API."""
    url = f"{base_url}/api/v1/trackfile/bulk"
    data = {"trackFileIds": track_file_ids}
    return _lidarr_request(url, api_key, method="DELETE", data=data)


def _lidarr_wait_for_search(base_url, api_key, log_dir=None):
    """Wait for any active AlbumSearch commands in Lidarr to complete.
    Called after each album deletion so Lidarr finishes searching
    before the next album is deleted. Polls every 10s, 5 min timeout.
    Returns the completion message from the last search command."""
    heartbeat_path = os.path.join(log_dir, ".heartbeat") if log_dir else None
    last_message = None
    timeout = time.time() + 300
    while time.time() < timeout and not shutdown_requested:
        result = _lidarr_request(
            f"{base_url}/api/v1/command", api_key)
        if result is None:
            return last_message
        search_cmds = [c for c in result
                       if c.get("name") == "AlbumSearch"]
        active = [c for c in search_cmds
                  if c.get("status", "").lower()
                  in ("queued", "started")]
        if not active:
            # Capture completion message from finished searches
            for c in search_cmds:
                msg = c.get("message", "")
                if msg:
                    last_message = msg
            return last_message
        if heartbeat_path:
            _write_heartbeat(heartbeat_path)
        time.sleep(10)
    return last_message


def _lidarr_delete_corrupt(base_url, api_key, corrupt_paths, log_file,
                           log_dir=None, blocklist=False, search=True):
    """Delete corrupt files via Lidarr API, one album at a time.
    Reads IDs from corrupt_details.json (resolved at scan time).
    Sequential processing prevents flooding indexers with searches.
    When search=False, skips waiting for Lidarr search (used by WebUI).
    Returns (deleted_count, affected_album_ids)."""
    details_path = os.path.join(
        log_dir or os.path.dirname(log_file), "corrupt_details.json")
    corrupt_details = _load_json(details_path)

    # Group trackfile IDs by album
    album_to_tfids = {}
    non_lidarr_paths = []

    for path in corrupt_paths:
        detail = corrupt_details.get(path, {})
        if isinstance(detail, dict) and "trackfileId" in detail:
            aid = detail.get("albumId", 0)
            if aid not in album_to_tfids:
                album_to_tfids[aid] = []
            album_to_tfids[aid].append(detail["trackfileId"])
        else:
            non_lidarr_paths.append(path)

    if not album_to_tfids and not non_lidarr_paths:
        return (0, [])

    deleted = 0
    affected_albums = []
    total_albums = len(album_to_tfids)

    with open(log_file, 'a', encoding='utf-8') as log:
        # Process one album at a time
        for i, (album_id, tf_ids) in enumerate(
                album_to_tfids.items(), 1):
            prefix = f"  [{i}/{total_albums}] Album {album_id}"

            # Blocklist before deletion
            if blocklist:
                bl_ok, bl_fail = _lidarr_blocklist_albums(
                    base_url, api_key, [album_id])
                if bl_fail:
                    logger.error("%s: blocklist failed — skipping",
                                 prefix)
                    log.write(f"ERROR: Blocklist failed for album "
                              f"{album_id} — skipped\n")
                    continue
                if bl_ok:
                    logger.debug("%s: blocklisted", prefix)

            # Bulk delete this album's trackfiles
            result = _lidarr_delete_trackfiles_bulk(
                base_url, api_key, tf_ids)
            if result is not None:
                deleted += len(tf_ids)
                affected_albums.append(album_id)
                log.write(f"LIDARR DELETE: album {album_id} — "
                          f"{len(tf_ids)} track files\n")

                if search:
                    # Check monitored status and wait for search
                    album = _lidarr_get_album(
                        base_url, api_key, album_id)
                    monitored = (album.get("monitored", True)
                                 if album else True)
                    if monitored:
                        logger.info(
                            "%s: deleted %d trackfiles — "
                            "waiting for search",
                            prefix, len(tf_ids))
                        msg = _lidarr_wait_for_search(
                            base_url, api_key, log_dir)
                        if msg:
                            logger.info("%s: %s", prefix, msg)
                        else:
                            logger.info(
                                "%s: search complete", prefix)
                    else:
                        logger.info(
                            "%s: deleted %d trackfiles "
                            "(unmonitored — Lidarr will not "
                            "re-download)",
                            prefix, len(tf_ids))
                else:
                    logger.info(
                        "%s: deleted %d trackfiles",
                        prefix, len(tf_ids))
            else:
                logger.error("%s: bulk delete failed — skipping",
                             prefix)
                log.write(f"ERROR: Bulk delete failed for album "
                          f"{album_id}\n")

        # Direct delete for files not tracked by Lidarr
        for path in non_lidarr_paths:
            try:
                os.remove(path)
                deleted += 1
                log.write(f"DIRECT DELETE (not in Lidarr): {path}\n")
                logger.info("  Deleted (not in Lidarr): %s", path)
            except OSError as e:
                log.write(f"ERROR deleting {path}: {e}\n")
                logger.error("  ERROR: %s - %s", path, e)

    return (deleted, affected_albums)


def _lidarr_get_album_history(base_url, api_key, album_id):
    """Fetch the most recent grabbed history record for an album.
    Returns list of history records (at most 1)."""
    url = (f"{base_url}/api/v1/history"
           f"?albumId={album_id}&eventType=1"
           f"&sortKey=date&sortDirection=descending"
           f"&pageSize=1&page=1")
    result = _lidarr_request(url, api_key)
    if result is None:
        return []
    return result.get("records", [])


def _lidarr_mark_history_failed(base_url, api_key, history_id):
    """Mark a history record as failed, creating a blocklist entry.
    Lidarr auto-blocklists the release on DownloadFailedEvent."""
    url = f"{base_url}/api/v1/history/failed/{history_id}"
    return _lidarr_request(url, api_key, method="POST")


def _lidarr_blocklist_albums(base_url, api_key, album_ids):
    """Blocklist the most recent grab for each album by marking it as failed.
    Returns (blocklisted_count, failed_count). A failed_count > 0 means
    some albums could not be blocklisted (API error, not missing history)."""
    blocklisted = 0
    failed = 0
    for album_id in album_ids:
        history = _lidarr_get_album_history(base_url, api_key, album_id)
        if not history:
            logger.debug("  Blocklist: no grab history for album %d "
                         "— may have been imported manually", album_id)
            continue
        record = history[0]
        history_id = record.get("id")
        if not history_id:
            logger.debug("  Blocklist: no history ID for album %d",
                         album_id)
            continue
        result = _lidarr_mark_history_failed(base_url, api_key, history_id)
        if result is not None:
            blocklisted += 1
            logger.debug("  Blocklist: marked history %d as failed "
                         "(album %d)", history_id, album_id)
        else:
            failed += 1
            logger.error("  Blocklist: failed to mark history %d "
                         "for album %d", history_id, album_id)
    if blocklisted:
        logger.info("  Lidarr: blocklisted %d/%d albums",
                    blocklisted, len(album_ids))
    return blocklisted, failed


def _log_lidarr_status(lidarr_url, lidarr_api_key, lidarr_search,
                       lidarr_blocklist=False):
    """Log Lidarr integration status without exposing credentials."""
    if lidarr_url and not lidarr_url.startswith(("http://", "https://")):
        logger.error("LIDARR_URL must start with http:// or https://")
        sys.exit(1)
    if lidarr_url and lidarr_api_key:
        logger.info("  Lidarr integration: enabled")
        if lidarr_search:
            logger.info("  Lidarr search: enabled (5/hour)")
        if lidarr_blocklist:
            logger.info("  Lidarr blocklist: enabled")
        masked = re.sub(r'(https?://)(.+)', r'\1****', lidarr_url)
        logger.debug("  Lidarr URL: %s", masked)
    elif lidarr_url:
        logger.warning("  Lidarr URL set but API key missing — disabled")


def _search_queue_path(log_dir):
    return os.path.join(log_dir, "search_queue.json")


def _search_queue_add(log_dir, album_ids):
    """Append album IDs to the search queue, deduplicating."""
    path = _search_queue_path(log_dir)
    queue = _load_json(path, default=[])
    if not isinstance(queue, list):
        logger.warning("Corrupt search_queue.json — resetting to empty list")
        queue = []
    existing = set(queue)
    added = 0
    for aid in album_ids:
        if aid not in existing:
            queue.append(aid)
            existing.add(aid)
            added += 1
    if added:
        write_json_atomic(path, queue)
        logger.info("  Queued Lidarr search for %d albums "
                    "(%d total in queue)", added, len(queue))


def _search_queue_drain_one(log_dir, base_url, api_key):
    """Process one album from the search queue. Sends AlbumSearch command,
    polls until complete. Returns True if a search was attempted."""
    path = _search_queue_path(log_dir)
    queue = _load_json(path, default=[])
    if not isinstance(queue, list) or not queue:
        return False

    album_id = queue[0]
    logger.info("  Lidarr search: album %d (%d in queue)", album_id,
                len(queue))

    # Trigger the search
    result = _lidarr_request(
        f"{base_url}/api/v1/command", api_key, method="POST",
        data={"name": "AlbumSearch", "albumIds": [album_id]})

    if result is None:
        logger.warning("  Lidarr search: failed to start for album %d",
                       album_id)
        return True  # attempted but failed — don't remove, retry next cycle

    command_id = result.get("id")
    if not command_id:
        logger.warning("  Lidarr search: no command ID returned for album %d",
                       album_id)
        queue.pop(0)
        write_json_atomic(path, queue)
        return True

    # Poll until complete (30s intervals, 10 min timeout)
    # Write heartbeat during poll to prevent healthcheck failure
    heartbeat_path = os.path.join(log_dir, ".heartbeat")
    poll_failures = 0
    timeout = time.time() + 600
    while time.time() < timeout and not shutdown_requested:
        time.sleep(30)
        _write_heartbeat(heartbeat_path)
        status = _lidarr_request(
            f"{base_url}/api/v1/command/{command_id}", api_key)
        if status is None:
            poll_failures += 1
            # Command may have been cleaned from DB (5 min retention)
            if poll_failures >= 3:
                logger.warning("  Lidarr search: lost track of command "
                               "for album %d, assuming complete", album_id)
                queue.pop(0)
                write_json_atomic(path, queue)
                return True
            continue
        poll_failures = 0
        state = status.get("status", "").lower()
        if state == "completed":
            queue.pop(0)
            write_json_atomic(path, queue)
            logger.info("  Lidarr search: album %d complete "
                        "(%d remaining)", album_id, len(queue))
            return True
        if state in ("failed", "aborted", "cancelled", "orphaned"):
            queue.pop(0)
            write_json_atomic(path, queue)
            logger.warning("  Lidarr search: album %d %s "
                           "(%d remaining)", album_id, state, len(queue))
            return True

    if shutdown_requested:
        logger.info("  Lidarr search: interrupted by shutdown")
    else:
        logger.warning("  Lidarr search: album %d timed out, "
                       "keeping in queue", album_id)
    return True


# --- Config ---

def setup_logging(log_level):
    """Configure logging with console handler."""
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


_DEFAULT_CONFIG = """\
#######################################################
##       BeatsCheck Configuration                    ##
#######################################################
##                                                   ##
##  Change the values below to configure BeatsCheck. ##
##  Lines starting with # are ignored — remove the   ##
##  # to enable a setting.                           ##
##                                                   ##
##  Settings can also be changed from the WebUI      ##
##  (enable at the bottom of this file).             ##
##                                                   ##
##  https://github.com/chodeus/BeatsCheck            ##
#######################################################


##----- Scanning ---------------------------------------
## How and when BeatsCheck scans your library.

## music_dir — folder to scan for audio files
##   Defaults to /data. Set a subfolder if your music
##   is under e.g. /data/media/music
# music_dir = /data

## mode — what happens when BeatsCheck runs
##   setup  = sit idle, don't scan (default)
##   report = scan and log corrupt files (nothing deleted)
##   move   = move corrupt files to a quarantine folder
mode = setup

## workers — files checked in parallel
##   More = faster scans but more CPU. 2-4 recommended.
workers = 4

## run_interval — hours between automatic scans
##   0   = scan once then wait (trigger manually or via WebUI)
##   24  = daily
##   168 = weekly
run_interval = 0

## min_file_age — skip files modified in the last N minutes
##   Avoids flagging files being actively downloaded.
min_file_age = 30


##----- When Corrupt Files Are Found -------------------
## By default, corrupt files are only logged — nothing
## is deleted unless you configure auto-delete here or
## use the Corrupt Files page in the WebUI.

## delete_after — auto-delete corrupt files after N days
##   Gives you time to review before anything is removed.
##   0 = never auto-delete (delete manually via WebUI)
##   7 = delete after 7 days
delete_after = 0

## max_auto_delete — safety limit
##   Abort if more than N files would be auto-deleted
##   in one run. Prevents mass deletion from filesystem
##   issues. 0 = no limit.
max_auto_delete = 50

## output_dir — quarantine folder (move mode only)
##   Must be within a mounted volume (e.g. /data/corrupted)
# output_dir = /data/corrupted


##----- Lidarr (Automatic Re-download) ----------------
## Connect to Lidarr so deleted corrupt files are
## automatically re-downloaded as clean copies.
##
## To enable: remove the # from lidarr_url and
## lidarr_api_key below and fill in your values.
##
## How it works:
##   1. Corrupt files are deleted via the Lidarr API
##   2. Monitored albums are automatically re-searched
##      by Lidarr (no extra config needed)
##   3. Blocklist prevents re-downloading the same
##      bad release
##
## Your API key is stored securely here — it won't
## appear in docker inspect or process listings.

## lidarr_url — your Lidarr address
##   e.g. http://lidarr:8686 or http://192.168.1.100:8686
# lidarr_url = http://lidarr:8686

## lidarr_api_key — find in Lidarr: Settings > General
# lidarr_api_key =

## lidarr_blocklist — blocklist the corrupt release so
##   Lidarr downloads a different copy
##   true = blocklist before deleting, false = just delete
# lidarr_blocklist = false

## lidarr_search — search for unmonitored albums after
##   auto-delete. Monitored albums are searched
##   automatically by Lidarr — this is only for albums
##   you've stopped monitoring.
# lidarr_search = false


##----- Logging ----------------------------------------

## log_level — how much detail to log
##   INFO = normal, DEBUG = verbose, WARNING/ERROR = quiet
log_level = INFO

## max_log_mb — rotate log at this size (MB)
##   Rotation triggers a fresh full rescan. 0 = never.
max_log_mb = 50


##----- Web Interface ----------------------------------
## Enable the WebUI for monitoring, config, and managing
## corrupt files from your browser.
##
## Also publish the port in Docker (e.g. ports: 8484:8484)
## On first visit you'll create a username and password.

## webui — enable or disable
##   true = start web server, false = no web interface
webui = false

## webui_port — port number
webui_port = 8484
"""

# Maps config-file keys (lowercase) to environment variable names.
_CONFIG_KEY_MAP = {
    'music_dir': 'MUSIC_DIR',
    'output_dir': 'OUTPUT_DIR',
    'mode': 'MODE',
    'workers': 'WORKERS',
    'run_interval': 'RUN_INTERVAL',
    'delete_after': 'DELETE_AFTER',
    'max_auto_delete': 'MAX_AUTO_DELETE',
    'min_file_age': 'MIN_FILE_AGE',
    'log_level': 'LOG_LEVEL',
    'max_log_mb': 'MAX_LOG_MB',
    'lidarr_url': 'LIDARR_URL',
    'lidarr_api_key': 'LIDARR_API_KEY',
    'lidarr_search': 'LIDARR_SEARCH',
    'lidarr_blocklist': 'LIDARR_BLOCKLIST',
    'webui': 'WEBUI',
    'webui_port': 'WEBUI_PORT',
}


def _write_default_config(config_dir):
    """Write the default beatscheck.conf template if it doesn't exist."""
    path = os.path.join(config_dir, "beatscheck.conf")
    try:
        with open(path, 'x') as f:
            f.write(_DEFAULT_CONFIG)
        print(f"Created default config: {path}")
    except FileExistsError:
        pass
    except OSError as e:
        print(f"Warning: could not write default config {path}: {e}")


# Track env vars set by Docker (before _apply_config_file).
# Used by _reload_config to know which values should not be
# overridden by the config file.
_docker_env_vars = set()


def _snapshot_docker_env():
    """Record which config-related env vars were set by Docker."""
    for env_name in _CONFIG_KEY_MAP.values():
        if env_name in os.environ:
            _docker_env_vars.add(env_name)


def _apply_config_file(config_dir):
    """Load beatscheck.conf and set env vars for any values not already
    set in the environment.  This gives env vars priority over the file."""
    path = os.path.join(config_dir, "beatscheck.conf")
    if not os.path.isfile(path):
        return
    try:
        with open(path, 'r') as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith('#'):
                    continue
                if '=' not in line:
                    continue
                key, _, value = line.partition('=')
                key = key.strip().lower()
                value = value.strip()
                # Strip inline comments (space+hash outside quotes)
                if ' #' in value and not (
                        len(value) >= 2 and value[0] == value[-1]
                        and value[0] in ('"', "'")):
                    value = value[:value.index(' #')].rstrip()
                # Strip surrounding quotes
                if (len(value) >= 2 and value[0] == value[-1]
                        and value[0] in ('"', "'")):
                    value = value[1:-1]
                env_name = _CONFIG_KEY_MAP.get(key)
                if env_name and env_name not in os.environ:
                    os.environ[env_name] = value
    except OSError as e:
        print(f"Warning: could not read config file {path}: {e}")


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


def _parse_env_bool(name, default=False):
    """Parse a boolean environment variable (true/false/1/0/yes/no)."""
    val = os.environ.get(name, str(default)).lower().strip()
    return val in ("true", "1", "yes")


def _load_config():
    """Load configuration from CLI args, config file, or environment.

    Priority: env vars > beatscheck.conf > defaults.
    """
    if len(sys.argv) == 4:
        input_folder = sys.argv[1].rstrip("/")
        output_folder = sys.argv[2].rstrip("/")
        log_file = sys.argv[3]
        log_dir = os.path.dirname(log_file)
    else:
        log_dir = os.environ.get("CONFIG_DIR", "/config").rstrip("/")
        # Write default config template on first run, then load values.
        # Config file values fill in for any env vars not already set.
        _write_default_config(log_dir)
        _snapshot_docker_env()
        _apply_config_file(log_dir)
        input_folder = os.environ.get("MUSIC_DIR", "/data").rstrip("/")
        output_folder = os.environ.get("OUTPUT_DIR", "/data/corrupted").rstrip("/")
        log_file = os.path.join(log_dir, "beats_check.log")

    mode = (os.environ.get("MODE") or "setup").lower()
    if mode not in ("report", "move", "delete", "setup"):
        print(f"Invalid MODE '{mode}'. Must be: report, move, delete, setup")
        sys.exit(1)

    workers = _parse_env_int("WORKERS", 4)
    if workers < 1:
        print("Invalid WORKERS value. Must be a positive integer.")
        sys.exit(1)

    return types.SimpleNamespace(
        input_folder=input_folder,
        output_folder=output_folder,
        log_dir=log_dir,
        log_file=log_file,
        mode=mode,
        log_level=os.environ.get("LOG_LEVEL", "INFO"),
        workers=workers,
        run_interval=_parse_env_float("RUN_INTERVAL", 0, "hours"),
        delete_after=_parse_env_float("DELETE_AFTER", 0, "days"),
        max_auto_delete=_parse_env_int("MAX_AUTO_DELETE", 50),
        min_age_minutes=_parse_env_int("MIN_FILE_AGE", 30, "minutes"),
        max_log_mb=_parse_env_int("MAX_LOG_MB", 50, "MB"),
        lidarr_url=os.environ.get("LIDARR_URL", "").rstrip("/"),
        lidarr_api_key=_load_lidarr_api_key(),
        lidarr_search=_parse_env_bool("LIDARR_SEARCH", False),
        lidarr_blocklist=_parse_env_bool("LIDARR_BLOCKLIST", False),
        webui=_parse_env_bool("WEBUI", False),
        webui_port=_parse_env_int("WEBUI_PORT", 8484),
    )


def _reload_config(cfg):
    """Re-read beatscheck.conf and update cfg for the next scan.

    Docker env vars (set before startup) always win. Config file values
    update the os.environ entries that _apply_config_file originally set,
    then we refresh the cfg namespace from os.environ.
    """
    path = os.path.join(cfg.log_dir, "beatscheck.conf")
    if not os.path.isfile(path):
        return
    try:
        file_vals = {}
        with open(path, 'r') as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith('#'):
                    continue
                if '=' not in line:
                    continue
                key, _, value = line.partition('=')
                key = key.strip().lower()
                value = value.strip()
                if ' #' in value and not (
                        len(value) >= 2 and value[0] == value[-1]
                        and value[0] in ('"', "'")):
                    value = value[:value.index(' #')].rstrip()
                if (len(value) >= 2 and value[0] == value[-1]
                        and value[0] in ('"', "'")):
                    value = value[1:-1]
                env_name = _CONFIG_KEY_MAP.get(key)
                if env_name:
                    file_vals[env_name] = value
    except OSError:
        return

    # Update env vars from config file (skip Docker-set vars)
    for env_name, value in file_vals.items():
        if env_name not in _docker_env_vars:
            os.environ[env_name] = value

    # Refresh cfg attributes from env vars
    cfg.input_folder = os.environ.get(
        "MUSIC_DIR", cfg.input_folder).rstrip("/")
    cfg.output_folder = os.environ.get(
        "OUTPUT_DIR", cfg.output_folder).rstrip("/")
    cfg.mode = (os.environ.get("MODE") or cfg.mode).lower()
    try:
        cfg.workers = max(1, int(os.environ.get(
            "WORKERS", cfg.workers)))
    except (ValueError, TypeError):
        pass
    try:
        cfg.run_interval = float(os.environ.get(
            "RUN_INTERVAL", cfg.run_interval))
    except (ValueError, TypeError):
        pass
    try:
        cfg.delete_after = float(os.environ.get(
            "DELETE_AFTER", cfg.delete_after))
    except (ValueError, TypeError):
        pass
    try:
        cfg.max_auto_delete = int(os.environ.get(
            "MAX_AUTO_DELETE", cfg.max_auto_delete))
    except (ValueError, TypeError):
        pass
    try:
        cfg.min_age_minutes = int(os.environ.get(
            "MIN_FILE_AGE", cfg.min_age_minutes))
    except (ValueError, TypeError):
        pass
    try:
        cfg.max_log_mb = int(os.environ.get(
            "MAX_LOG_MB", cfg.max_log_mb))
    except (ValueError, TypeError):
        pass
    cfg.lidarr_url = os.environ.get(
        "LIDARR_URL", cfg.lidarr_url).rstrip("/")
    cfg.lidarr_api_key = _load_lidarr_api_key()
    cfg.lidarr_search = _parse_env_bool(
        "LIDARR_SEARCH", cfg.lidarr_search)
    cfg.lidarr_blocklist = _parse_env_bool(
        "LIDARR_BLOCKLIST", cfg.lidarr_blocklist)


def _run_setup_idle(log_dir, lidarr_url=None, lidarr_api_key=None):
    """Setup mode — sit idle until rescan with a mode is triggered.
    Bare 'rescan' (no mode) defaults to report.
    Drains the Lidarr search queue during idle if configured."""
    logger.info("Setup mode — container is idle. "
                "Start scanning with: rescan report")
    result = _idle_wait(log_dir, None, lidarr_url, lidarr_api_key)
    if isinstance(result, str) and result in ("report", "move"):
        return result
    if result is True:
        return "report"
    return None


_webui_app_state = None


def _webui_update(cfg, **kwargs):
    """Update WebUI state if enabled. Safe to call even when WebUI is off."""
    if _webui_app_state is not None:
        _webui_app_state.update(**kwargs)


def _webui_progress(current, total, corrupted, current_file):
    """Update WebUI scan progress. Called from scan loop."""
    if _webui_app_state is not None:
        _webui_app_state.update(
            scan_progress={
                "current": current, "total": total,
                "file": current_file},
            corrupt_count=corrupted,
            total_scanned=current,
        )


# --- Main helpers ---


def _start_webui(cfg):
    """Start optional WebUI server in a daemon thread."""
    global _webui_app_state
    try:
        from webui import start_webui, app_state
        _webui_app_state = app_state
        app_state.update(version=__version__, mode=cfg.mode,
                         status="starting")
        start_webui(cfg.log_dir, cfg.webui_port)
    except Exception as e:
        logger.error("WebUI failed to start: %s", e)


def _maybe_rotate_logs(cfg):
    """Rotate log and processed.txt if log exceeds max size."""
    if cfg.max_log_mb <= 0 or not os.path.exists(cfg.log_file):
        return
    try:
        log_size = os.path.getsize(cfg.log_file)
        if log_size > cfg.max_log_mb * 1024 * 1024:
            _rotate_file(cfg.log_file, keep=3)
            processed = os.path.join(cfg.log_dir, "processed.txt")
            if os.path.exists(processed):
                _rotate_file(processed, keep=3)
            logger.info(
                "Log rotated (%s > %dMB limit). "
                "Starting fresh full scan.",
                format_size(log_size), cfg.max_log_mb)
    except OSError:
        pass


def _validate_move_mode(cfg):
    """Validate output_dir for move mode. Falls back to report if invalid.
    Returns True if move mode is valid and ready."""
    if not cfg.output_folder:
        logger.error("Move mode requires output_dir to be "
                     "configured. Falling back to report mode.")
        return False
    if not os.path.isdir(
            os.path.dirname(cfg.output_folder.rstrip("/"))):
        logger.error(
            "Output directory parent does not exist: %s. "
            "Check your volume mount. Falling back to "
            "report mode.", cfg.output_folder)
        return False
    os.makedirs(cfg.output_folder, exist_ok=True)
    return True


def _wait_after_cancel(cfg):
    """After a cancelled scan, go idle and wait for next rescan trigger.
    Returns True if shutdown requested (caller should break)."""
    logger.info("Scan cancelled. Container is idle.")
    _webui_update(cfg, status="idle", scan_progress=None)
    result = _idle_wait(
        cfg.log_dir, None, cfg.lidarr_url, cfg.lidarr_api_key)
    if result is False:
        return True
    if isinstance(result, str) and result in ("report", "move"):
        cfg.mode = result
        logger.info("Mode changed to: %s", cfg.mode)
    return False


# --- Main ---

def main():
    signal.signal(signal.SIGTERM, handle_shutdown)
    signal.signal(signal.SIGINT, handle_shutdown)

    try:
        os.nice(10)
    except OSError:
        pass

    cfg = _load_config()

    os.makedirs(cfg.log_dir, exist_ok=True)

    setup_logging(cfg.log_level)

    logger.info("BeatsCheck v%s starting", __version__)

    if cfg.webui:
        _start_webui(cfg)

    _log_lidarr_status(cfg.lidarr_url, cfg.lidarr_api_key, cfg.lidarr_search,
                       cfg.lidarr_blocklist)

    queue = _load_json(_search_queue_path(cfg.log_dir), default=[])
    if isinstance(queue, list) and queue:
        logger.info("  Search queue: %d albums pending", len(queue))

    if cfg.mode == "setup":
        _webui_update(cfg, status="setup", mode="setup")
        new_mode = _run_setup_idle(cfg.log_dir, cfg.lidarr_url,
                                   cfg.lidarr_api_key)
        if new_mode:
            cfg.mode = new_mode
            logger.info("Mode changed to: %s", cfg.mode)
        else:
            return

    if cfg.mode == "delete":
        corrupt_list_path = os.path.join(cfg.log_dir, "corrupt.txt")
        run_delete_mode(corrupt_list_path, cfg.log_file, cfg.log_dir,
                        cfg.input_folder, cfg.lidarr_url,
                        cfg.lidarr_api_key, cfg.lidarr_blocklist)
        return

    if not os.path.isdir(cfg.input_folder):
        logger.error("Music directory not found: %s", cfg.input_folder)
        logger.error("Check your volume mount. Container will stay idle.")
        cfg.mode = "setup"

    while True:
        global scan_cancelled
        scan_cancelled = False
        _reload_config(cfg)
        _maybe_rotate_logs(cfg)

        # Validate paths before scanning
        if not os.path.isdir(cfg.input_folder):
            logger.error("Music directory not found: %s",
                         cfg.input_folder)
            _webui_update(cfg, status="idle", mode=cfg.mode)
            result = _idle_wait(
                cfg.log_dir, None, cfg.lidarr_url,
                cfg.lidarr_api_key)
            if result is False:
                break
            if isinstance(result, str) and result in ("report", "move"):
                cfg.mode = result
            continue

        if cfg.mode == "move":
            if not _validate_move_mode(cfg):
                cfg.mode = "report"
                _webui_update(cfg, mode="report")

        _webui_update(cfg, status="scanning", mode=cfg.mode, scan_progress=None)
        run_scan(cfg.input_folder, cfg.output_folder, cfg.log_file,
                 cfg.log_dir, cfg.mode, cfg.workers, cfg.min_age_minutes,
                 cfg.lidarr_url, cfg.lidarr_api_key)

        if scan_cancelled:
            if _wait_after_cancel(cfg):
                break
            continue

        if cfg.delete_after > 0:
            run_auto_delete(
                cfg.log_dir, cfg.log_file, cfg.delete_after,
                cfg.max_auto_delete, cfg.lidarr_url, cfg.lidarr_api_key,
                cfg.lidarr_search, cfg.lidarr_blocklist)

        if shutdown_requested:
            break

        if cfg.run_interval <= 0:
            logger.info("Scan complete. Container is idle. "
                        "Rescan with: rescan [report|move]")
            _webui_update(cfg, status="idle", scan_progress=None)
            result = _idle_wait(
                cfg.log_dir, None, cfg.lidarr_url, cfg.lidarr_api_key)
            if result is False:
                break
            if isinstance(result, str) and result in ("report", "move"):
                cfg.mode = result
                logger.info("Mode changed to: %s", cfg.mode)
            continue

        next_run = time.strftime(
            '%Y-%m-%d %H:%M:%S',
            time.localtime(time.time() + cfg.run_interval * 3600)
        )
        logger.info(
            "Next scan at %s (%sh interval). Waiting...",
            next_run, cfg.run_interval
        )
        _webui_update(cfg, status="idle", scan_progress=None)

        result = _idle_wait(cfg.log_dir, cfg.run_interval * 3600,
                            cfg.lidarr_url, cfg.lidarr_api_key)
        if result is False:
            if shutdown_requested:
                break
            logger.info("Scheduled scan starting.")
        else:
            if isinstance(result, str) and result in ("report", "move"):
                cfg.mode = result
                logger.info("Mode changed to: %s", cfg.mode)


if __name__ == "__main__":
    main()
