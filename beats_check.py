import json
import logging
import os
import subprocess
import sys
import shutil
import signal
import time
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
        errors = result.stderr.strip().replace('\n', ' | ')
        if len(errors) > 300:
            errors = errors[:300] + "..."
        reason = errors if errors else "Non-zero exit code"
        return (file_path, True, reason)

    return (file_path, False, None)


def collect_audio_files(input_folder, min_age_minutes=30):
    """Walk the input folder and return all audio file paths.
    Does not follow symlinks to prevent traversal outside the music dir.
    Skips files modified within min_age_minutes to avoid flagging files
    being actively written by download clients or Lidarr."""
    real_root = os.path.realpath(input_folder)
    age_threshold = time.time() - (min_age_minutes * 60)
    files = []
    skipped_young = 0
    for root, _, filenames in os.walk(input_folder, followlinks=False):
        for f in sorted(filenames):
            file_path = os.path.join(root, f)
            # Skip symlinks that point outside the music directory
            real_path = os.path.realpath(file_path)
            if not real_path.startswith(real_root + os.sep) and real_path != real_root:
                continue
            if os.path.splitext(f)[1].lower() not in AUDIO_EXTENSIONS:
                continue
            # Skip files still being written (modified recently)
            try:
                if os.path.getmtime(file_path) > age_threshold:
                    skipped_young += 1
                    continue
            except OSError:
                continue
            files.append(file_path)
    if skipped_young > 0:
        logger.info(
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


LOG_PREFIX = "Checking "
LOG_SUFFIX = "..."


def get_already_processed_files(log_file):
    if not os.path.exists(log_file):
        return set()
    processed = set()
    with open(log_file, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.rstrip("\n")
            if line.startswith(LOG_PREFIX) and line.endswith(LOG_SUFFIX):
                path = line[len(LOG_PREFIX):-len(LOG_SUFFIX)]
                processed.add(path)
    return processed


def write_json_atomic(path, data):
    """Write JSON data atomically using a temp file + rename."""
    tmp_path = path + ".tmp"
    with open(tmp_path, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2)
    os.rename(tmp_path, path)


def run_delete_mode(corrupt_list_path, log_file, log_dir):
    """Interactive delete mode. Groups corrupt files by album folder and prompts."""
    # Wait for any active scan to finish
    lock_path = os.path.join(log_dir, ".scanning")
    if os.path.exists(lock_path):
        logger.info("A scan is currently running. Waiting for it to finish...")
        while os.path.exists(lock_path) and not shutdown_requested:
            time.sleep(5)
        if shutdown_requested:
            return
        logger.info("Scan finished. Starting delete mode.")

    if not os.path.exists(corrupt_list_path):
        logger.error("No corrupt file list found at %s", corrupt_list_path)
        logger.info("Run a scan first with MODE=report")
        sys.exit(1)

    with open(corrupt_list_path, 'r', encoding='utf-8') as f:
        all_paths = [line.strip() for line in f if line.strip()]

    if not all_paths:
        logger.info("corrupt.txt is empty — no corrupt files found.")
        return

    # Load corruption reasons
    details_path = os.path.join(log_dir, "corrupt_details.json")
    corrupt_details = {}
    if os.path.exists(details_path):
        with open(details_path, 'r', encoding='utf-8') as f:
            corrupt_details = json.load(f)

    # Deduplicate while preserving order
    seen = set()
    files = []
    for fp in all_paths:
        if fp not in seen:
            seen.add(fp)
            files.append(fp)

    # Group by parent folder (album directory)
    folders = {}
    for fp in files:
        folder = os.path.dirname(fp)
        if folder not in folders:
            folders[folder] = []
        folders[folder].append(fp)

    total_files = len(files)
    total_folders = len(folders)

    # Calculate total size of existing corrupt files
    total_corrupt_size = 0
    for f in files:
        try:
            total_corrupt_size += os.path.getsize(f)
        except OSError:
            pass

    print(f"Found {total_files} corrupt files across {total_folders} folders ({format_size(total_corrupt_size)})\n")

    try:
        action = input(
            "  [a] delete ALL corrupt files now\n"
            "  [i] interactive (decide per folder)\n"
            "  [q] quit\n\n"
            "  Choice: "
        ).strip().lower()
    except EOFError:
        print("\nNo input available. Run with: docker exec -it BeatsCheck /app/delete.sh")
        return

    if action == 'q':
        return

    if action == 'a':
        run_mass_delete(files, log_file, log_dir)
        return

    if action != 'i':
        print("Invalid choice.")
        return

    # Interactive mode — go through each folder
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
        log.write(f"Interactive delete started: {time.strftime('%Y-%m-%d %H:%M:%S')}\n")
        log.write(f"{'='*60}\n")

        for i, (folder, corrupt_files) in enumerate(folders.items(), 1):
            # Check which files still exist
            existing = [f for f in corrupt_files if os.path.exists(f)]
            gone = len(corrupt_files) - len(existing)
            if gone:
                missing_files += gone

            if not existing and not os.path.isdir(folder):
                print(f"  [{i}/{total_folders}] GONE: {folder}")
                log.write(f"MISSING FOLDER: {folder}\n")
                continue

            # Show folder and its corrupt files with reasons
            print(f"  [{i}/{total_folders}] {folder}/")
            for cf in corrupt_files:
                name = os.path.basename(cf)
                reason = corrupt_details.get(cf, "")
                try:
                    size = os.path.getsize(cf)
                    size_str = format_size(size)
                    print(f"           {name} ({size_str})")
                    if reason:
                        # Shorten for display
                        short = reason[:120] + "..." if len(reason) > 120 else reason
                        print(f"             -> {short}")
                except OSError:
                    print(f"           {name} (already deleted)")

            # Count total files in folder (not just corrupt ones)
            try:
                entries = os.listdir(folder)
                total_in_folder = sum(
                    1 for e in entries
                    if os.path.isfile(os.path.join(folder, e))
                )
                print(f"           ({len(existing)} corrupt / {total_in_folder} total files in folder)")
            except OSError:
                pass

            if delete_all:
                choice = 'y'
            else:
                try:
                    choice = input("           Action? [y/f/n/a/q] ").strip().lower()
                except EOFError:
                    print("\nNo input available. Run with: docker run -it ...")
                    break

            if choice == 'q':
                print("\nQuitting.")
                break
            elif choice == 'a':
                delete_all = True
                choice = 'y'

            if choice == 'y' and os.path.isdir(folder):
                # Delete entire folder
                try:
                    shutil.rmtree(folder)
                    folders_deleted += 1
                    files_deleted += len(existing)
                    log.write(f"DELETED FOLDER: {folder} ({len(existing)} corrupt files)\n")
                    print("           -> Folder deleted\n")
                except OSError as e:
                    print(f"           ERROR: {e}\n")
                    log.write(f"ERROR deleting folder {folder}: {e}\n")

            elif choice == 'f':
                # Delete only the corrupt files
                for cf in existing:
                    try:
                        os.remove(cf)
                        files_deleted += 1
                        log.write(f"DELETED FILE: {cf}\n")
                    except OSError as e:
                        print(f"           ERROR deleting {os.path.basename(cf)}: {e}")
                        log.write(f"ERROR deleting {cf}: {e}\n")
                print(f"           -> {len(existing)} corrupt files deleted\n")

            else:
                skipped_folders += 1
                log.write(f"SKIPPED: {folder}\n")
                print()

        summary = (
            f"\nDelete summary:\n"
            f"  Folders deleted:  {folders_deleted}\n"
            f"  Files deleted:    {files_deleted}\n"
            f"  Folders skipped:  {skipped_folders}\n"
            f"  Already missing:  {missing_files}\n"
        )
        print(summary)
        log.write(summary)

    # Update corrupt.txt — remove entries for deleted/missing files
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


def run_scan(input_folder, output_folder, log_file, log_dir, mode, workers,
             min_age_minutes=30):
    """Scan mode: decode-test all audio files with parallel workers."""
    corrupt_list_path = os.path.join(log_dir, "corrupt.txt")
    lock_path = os.path.join(log_dir, ".scanning")

    # Create lock file so delete mode knows a scan is in progress
    with open(lock_path, 'w') as lf:
        lf.write(str(os.getpid()))

    try:
        return _run_scan_inner(input_folder, output_folder, log_file, log_dir,
                               mode, workers, corrupt_list_path, min_age_minutes)
    finally:
        # Always remove lock file when scan ends
        try:
            os.remove(lock_path)
        except OSError:
            pass


def _run_scan_inner(input_folder, output_folder, log_file, log_dir,
                    mode, workers, corrupt_list_path, min_age_minutes):
    """Inner scan logic."""
    # Resume support
    already_processed = get_already_processed_files(log_file)

    # Collect all audio files and calculate total size
    logger.info("Scanning for audio files...")
    all_files = collect_audio_files(input_folder, min_age_minutes)
    total_library_size = 0
    for f in all_files:
        try:
            total_library_size += os.path.getsize(f)
        except OSError:
            pass

    files_to_check = [f for f in all_files if f not in already_processed]
    skipped = len(all_files) - len(files_to_check)
    total = len(files_to_check)

    logger.info("BeatsCheck v%s", __version__)
    logger.info("  Mode:    %s", mode)
    logger.info("  Workers: %d", workers)
    logger.info("  Music:   %s", input_folder)
    logger.info("  Log:     %s", log_file)
    logger.info("  Corrupt: %s", corrupt_list_path)
    if mode == "move":
        logger.info("  Output:  %s", output_folder)
    logger.info("  Library: %d files (%s)", len(all_files), format_size(total_library_size))
    logger.info("  To scan: %d files (%d already processed)", total, skipped)
    if mode == "report":
        logger.info("  (report mode - no files will be moved)")

    if total == 0:
        logger.info("Nothing to do.")
        return

    checked = 0
    corrupted = 0
    corrupt_size = 0
    start_time = time.time()

    # Load existing corruption details (reasons from previous scans)
    details_path = os.path.join(log_dir, "corrupt_details.json")
    corrupt_details = {}
    if os.path.exists(details_path):
        with open(details_path, 'r', encoding='utf-8') as f:
            corrupt_details = json.load(f)

    # Load existing corrupt paths to deduplicate appends
    existing_corrupt = set()
    if os.path.exists(corrupt_list_path):
        with open(corrupt_list_path, 'r', encoding='utf-8') as f:
            for line in f:
                path = line.strip()
                if path:
                    existing_corrupt.add(path)

    # Keep log files open for the duration of the scan to avoid
    # opening/closing per file (100K+ files would thrash the filesystem)
    with open(log_file, 'a', encoding='utf-8') as log, \
         open(corrupt_list_path, 'a', encoding='utf-8') as corrupt_log:

        log.write(f"\n{'='*60}\n")
        log.write(f"Scan started: {time.strftime('%Y-%m-%d %H:%M:%S')}\n")
        log.write(f"Mode: {mode} | Workers: {workers}\n")
        log.write(f"Library: {len(all_files)} files ({format_size(total_library_size)})\n")
        log.write(f"Files to check: {total} (skipped {skipped} already processed)\n")
        log.write(f"{'='*60}\n")
        log.flush()

        with ThreadPoolExecutor(max_workers=workers) as pool:
            futures = {pool.submit(check_audio_file, f): f for f in files_to_check}

            for future in as_completed(futures):
                if shutdown_requested:
                    pool.shutdown(wait=True, cancel_futures=True)
                    break

                try:
                    file_path, is_corrupt, reason = future.result()
                except Exception as e:
                    file_path = futures[future]
                    logger.error("Unexpected error checking %s: %s", file_path, e)
                    log.write(f"ERROR: {file_path} - {e}\n")
                    log.flush()
                    checked += 1
                    continue

                checked += 1

                # Log every file checked (for resume support)
                log.write(f"Checking {file_path}...\n")
                if is_corrupt:
                    log.write(f"CORRUPT: {file_path} - {reason}\n")
                log.flush()

                if is_corrupt:
                    corrupted += 1
                    try:
                        corrupt_size += os.path.getsize(file_path)
                    except OSError:
                        pass
                    logger.info("CORRUPT: %s", file_path)
                    logger.info("         %s", reason)

                    # Only append if not already in corrupt.txt
                    if file_path not in existing_corrupt:
                        corrupt_log.write(file_path + "\n")
                        corrupt_log.flush()
                        existing_corrupt.add(file_path)
                    corrupt_details[file_path] = reason

                    # Quarantine if in move mode
                    if mode == "move":
                        relative_path = os.path.relpath(
                            os.path.dirname(file_path), input_folder
                        )
                        dest_dir = os.path.join(output_folder, relative_path)
                        os.makedirs(dest_dir, exist_ok=True)
                        dest = os.path.join(dest_dir, os.path.basename(file_path))
                        try:
                            shutil.move(file_path, dest)
                            log.write(f"File moved: {file_path} -> {dest}\n")
                            log.flush()
                            logger.info("         moved -> %s", dest)
                        except (OSError, shutil.Error) as e:
                            log.write(f"ERROR: Failed to move {file_path}: {e}\n")
                            log.flush()
                            logger.error("         ERROR: failed to move: %s", e)

                # Progress every 100 files or at the end
                if checked % 100 == 0 or checked == total:
                    elapsed = time.time() - start_time
                    rate = checked / elapsed if elapsed > 0 else 0
                    eta = (total - checked) / rate if rate > 0 else 0
                    pct = checked * 100 // total
                    logger.info(
                        "[%d%%] %d/%d checked, %d corrupt, ETA %s",
                        pct, checked, total, corrupted, format_eta(eta)
                    )

        # Summary
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

    # Save corruption details (reasons) for delete mode display
    # Remove entries for files that no longer exist
    corrupt_details = {p: r for p, r in corrupt_details.items() if os.path.exists(p)}
    write_json_atomic(details_path, corrupt_details)

    # Write machine-readable summary for notification scripts
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


def run_auto_delete(log_dir, log_file, delete_after_days, max_deletes=50):
    """Auto-delete corrupt files that have been known for longer than DELETE_AFTER days.
    Aborts if more than max_deletes files would be removed (safety threshold)."""
    tracking_path = os.path.join(log_dir, "corrupt_tracking.json")
    corrupt_list_path = os.path.join(log_dir, "corrupt.txt")

    # Load or create tracking data (maps file path -> first-seen ISO timestamp)
    tracking = {}
    if os.path.exists(tracking_path):
        with open(tracking_path, 'r', encoding='utf-8') as f:
            tracking = json.load(f)

    # Add any new entries from corrupt.txt
    now = time.strftime('%Y-%m-%dT%H:%M:%S')
    if os.path.exists(corrupt_list_path):
        with open(corrupt_list_path, 'r', encoding='utf-8') as f:
            for line in f:
                path = line.strip()
                if path and path not in tracking:
                    tracking[path] = now

    # Remove entries for files that no longer exist
    tracking = {p: t for p, t in tracking.items() if os.path.exists(p)}

    if not tracking:
        # Save clean tracking file
        write_json_atomic(tracking_path, tracking)
        return

    # Find files older than threshold
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
        # Save updated tracking
        write_json_atomic(tracking_path, tracking)
        if to_keep:
            logger.info("%d corrupt files still within %d-day review window",
                        len(to_keep), delete_after_days)
        return

    # Safety threshold — abort if too many files would be deleted
    # (could indicate a filesystem issue or misconfigured scanner)
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
        # Still save tracking so timestamps aren't lost
        write_json_atomic(tracking_path, tracking)
        return

    logger.info("Auto-deleting %d corrupt files (older than %d days):",
                len(to_delete), delete_after_days)
    deleted = 0
    with open(log_file, 'a', encoding='utf-8') as log:
        log.write(f"\nAuto-delete ({delete_after_days}d threshold): {len(to_delete)} files\n")
        for path in to_delete:
            try:
                os.remove(path)
                deleted += 1
                del tracking[path]
                log.write(f"AUTO-DELETED: {path}\n")
                logger.info("  Deleted: %s", path)
            except OSError as e:
                log.write(f"ERROR auto-deleting {path}: {e}\n")
                logger.error("  ERROR: %s - %s", path, e)
                # Keep in tracking so timestamp is preserved (don't reset countdown)

    logger.info("  %d/%d files deleted", deleted, len(to_delete))
    if to_keep:
        logger.info("  %d files still within review window", len(to_keep))

    # Save updated tracking and corrupt.txt
    write_json_atomic(tracking_path, tracking)
    with open(corrupt_list_path, 'w', encoding='utf-8') as f:
        for path in tracking:
            f.write(path + "\n")


def setup_logging(log_level, log_file):
    """Configure logging with console and file handlers."""
    level = getattr(logging, log_level.upper(), logging.INFO)

    # Root logger setup
    root = logging.getLogger()
    root.setLevel(level)

    # Console handler
    console = logging.StreamHandler(sys.stdout)
    console.setLevel(level)
    console_fmt = logging.Formatter(
        "%(asctime)s | %(levelname)-8s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    console.setFormatter(console_fmt)
    root.addHandler(console)


def main():
    signal.signal(signal.SIGTERM, handle_shutdown)
    signal.signal(signal.SIGINT, handle_shutdown)

    try:
        os.nice(10)
    except OSError:
        pass

    # Config: env vars (Docker/Unraid) or CLI args (standalone)
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
    log_level = os.environ.get("LOG_LEVEL", "INFO")

    try:
        workers = int(os.environ.get("WORKERS", "4"))
        if workers < 1:
            raise ValueError
    except ValueError:
        print("Invalid WORKERS value. Must be a positive integer.")
        sys.exit(1)

    try:
        run_interval = float(os.environ.get("RUN_INTERVAL", "0"))
    except ValueError:
        print("Invalid RUN_INTERVAL value. Must be a number (hours).")
        sys.exit(1)

    try:
        delete_after = float(os.environ.get("DELETE_AFTER", "0"))
    except ValueError:
        print("Invalid DELETE_AFTER value. Must be a number (days).")
        sys.exit(1)

    try:
        max_auto_delete = int(os.environ.get("MAX_AUTO_DELETE", "50"))
    except ValueError:
        print("Invalid MAX_AUTO_DELETE value. Must be an integer.")
        sys.exit(1)

    try:
        min_age_minutes = int(os.environ.get("MIN_FILE_AGE", "30"))
    except ValueError:
        print("Invalid MIN_FILE_AGE value. Must be an integer (minutes).")
        sys.exit(1)

    try:
        max_log_mb = int(os.environ.get("MAX_LOG_MB", "50"))
    except ValueError:
        print("Invalid MAX_LOG_MB value. Must be an integer (MB).")
        sys.exit(1)

    if mode not in ("report", "move", "delete"):
        print(f"Invalid MODE '{mode}'. Must be: report, move, delete")
        sys.exit(1)

    os.makedirs(log_dir, exist_ok=True)

    # Set up logging after log_dir exists
    setup_logging(log_level, log_file)

    logger.info("BeatsCheck v%s starting", __version__)

    if mode == "delete":
        corrupt_list_path = os.path.join(log_dir, "corrupt.txt")
        run_delete_mode(corrupt_list_path, log_file, log_dir)
        return

    if not os.path.isdir(input_folder):
        logger.error("Music directory not found: %s", input_folder)
        sys.exit(1)

    if mode == "move":
        if not output_folder or output_folder == "/corrupted":
            # Check if the volume was actually mounted
            if not os.path.isdir("/corrupted"):
                logger.error("Move mode requires the Corrupted Output path to be configured.")
                logger.error("Set the OUTPUT_DIR variable or mount a volume to /corrupted.")
                sys.exit(1)
        os.makedirs(output_folder, exist_ok=True)

    # Run scan (once or on a schedule)
    while True:
        # Rotate log if it's too large (resume cache is in the log,
        # so rotating means the next scan re-checks everything)
        if max_log_mb > 0 and os.path.exists(log_file):
            try:
                log_size = os.path.getsize(log_file)
                if log_size > max_log_mb * 1024 * 1024:
                    rotated = log_file + ".old"
                    shutil.move(log_file, rotated)
                    logger.info("Log rotated (%s > %dMB limit)",
                                format_size(log_size), max_log_mb)
                    logger.info("Previous log saved as %s", rotated)
                    logger.info("Starting fresh full scan.")
            except OSError:
                pass

        run_scan(input_folder, output_folder, log_file, log_dir, mode, workers,
                 min_age_minutes)

        # Auto-delete corrupt files older than threshold
        if delete_after > 0:
            run_auto_delete(log_dir, log_file, delete_after, max_auto_delete)

        if run_interval <= 0 or shutdown_requested:
            break

        next_run = time.strftime(
            '%Y-%m-%d %H:%M:%S',
            time.localtime(time.time() + run_interval * 3600)
        )
        logger.info(
            "Next scan at %s (%sh interval). Waiting...",
            next_run, run_interval
        )
        logger.info("Container is idle. Stop the container to exit.")

        # Sleep in small increments to respond to shutdown signals
        sleep_until = time.time() + (run_interval * 3600)
        while time.time() < sleep_until and not shutdown_requested:
            time.sleep(10)

        if shutdown_requested:
            break

        logger.info("=" * 60)
        logger.info("Scheduled scan starting: %s", time.strftime('%Y-%m-%d %H:%M:%S'))
        logger.info("=" * 60)


if __name__ == "__main__":
    main()
