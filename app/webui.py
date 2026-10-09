"""BeatsCheck WebUI — optional web interface served via Python stdlib."""

import functools
import hashlib
import hmac
import http.cookies
import ipaddress
import json
import logging
import math
import mimetypes
import os
import re
import secrets
import threading
import time
import urllib.parse
from http.server import HTTPServer, SimpleHTTPRequestHandler
from socketserver import ThreadingMixIn

logger = logging.getLogger("beatscheck.webui")

# ---------------------------------------------------------------------------
# Shared application state (thread-safe)
# ---------------------------------------------------------------------------


class AppState:
    """Thread-safe container for state shared between scan loop and WebUI."""

    def __init__(self):
        self._lock = threading.Lock()
        self._data = {
            "status": "starting",
            "mode": "setup",
            "scan_progress": None,
            "last_scan_time": None,
            "last_scan_duration": None,
            "corrupt_count": 0,
            "total_scanned": 0,
            "version": "unknown",
            "uptime_start": time.time(),
        }

    def get(self, key=None):
        with self._lock:
            if key is not None:
                return self._data.get(key)
            return dict(self._data)

    def update(self, **kwargs):
        with self._lock:
            self._data.update(kwargs)

    def snapshot(self):
        """Return full state dict plus computed fields."""
        with self._lock:
            d = dict(self._data)
        d["uptime"] = int(time.time() - d["uptime_start"])
        return d


# Module-level singleton — set by main.main()
app_state = AppState()

# ---------------------------------------------------------------------------
# Authentication
# ---------------------------------------------------------------------------

_AUTH_FILE = "webui_auth.json"
_PBKDF2_ITERATIONS = 100_000
_SESSION_MAX_AGE = 86400  # 24 hours
_MIN_PASSWORD_LENGTH = 8
_MAX_USERNAME_LENGTH = 64
_MAX_PASSWORD_LENGTH = 1024
# Held across every load-check-write of webui_auth.json.
_auth_lock = threading.Lock()

# Sentinel returned by _load_auth() when the auth file EXISTS but cannot be
# read/parsed (corruption or PUID/PGID permission drift). Callers must fail
# closed on this — never treat it as "no credentials configured", which
# would re-open the first-run setup wizard to an attacker.
_AUTH_UNREADABLE = object()

# The user chose no login. Only this exact file content means "off"; anything
# else unexpected is _AUTH_UNREADABLE, so a damaged file never opens the API.
_AUTH_OFF_MARKER = {"login": "off"}
_AUTH_OFF = object()

_sessions = {}
_sessions_lock = threading.Lock()

# Failed-login throttle (per client IP). In-memory; resets on restart.
_LOGIN_MAX_ATTEMPTS = 5
_LOGIN_LOCKOUT_SECONDS = 300
_LOGIN_ATTEMPT_WINDOW = 900
_LOGIN_MAX_TRACKED = 10000
_LOGIN_OVERFLOW_KEY = "overflow"  # not an IP, so no client can own it
_MAX_FORWARDED_HOPS = 16
_login_attempts = {}
_login_attempts_lock = threading.Lock()


def _hash_password(password, salt=None):
    """Hash password with PBKDF2-SHA256. Returns 'pbkdf2:salt_hex:hash_hex'."""
    if salt is None:
        salt = secrets.token_bytes(16)
    else:
        salt = bytes.fromhex(salt)
    dk = hashlib.pbkdf2_hmac(
        'sha256', password.encode('utf-8'), salt, _PBKDF2_ITERATIONS)
    return f"pbkdf2:{salt.hex()}:{dk.hex()}"


def _verify_password(password, stored_hash):
    """Verify password against stored hash."""
    try:
        _, salt_hex, hash_hex = stored_hash.split(':')
    except (ValueError, AttributeError):
        return False
    expected = _hash_password(password, salt=salt_hex)
    return hmac.compare_digest(expected, stored_hash)


def _load_auth(config_dir):
    """Load auth credentials from webui_auth.json.

    Returns None when the file is ABSENT (first-run setup allowed), the
    credentials dict when present and valid, _AUTH_OFF when the user turned
    the login off, or the _AUTH_UNREADABLE sentinel when the file exists but
    cannot be parsed / is missing required fields. Callers must fail closed
    on the sentinel: mapping a read error to None would let anyone re-run
    /api/setup and seize the account (auth fail-open)."""
    path = os.path.join(config_dir, _AUTH_FILE)
    if not os.path.isfile(path):
        return None
    try:
        with open(path, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except (json.JSONDecodeError, OSError, ValueError):
        logger.error(
            "WebUI auth file %s exists but is unreadable/corrupt; "
            "refusing setup and login (fail closed)", path)
        return _AUTH_UNREADABLE
    if data == _AUTH_OFF_MARKER:
        return _AUTH_OFF
    if (isinstance(data, dict) and data.get('username')
            and data.get('password_hash')):
        return data
    logger.error(
        "WebUI auth file %s is present but missing username/"
        "password_hash; refusing setup and login (fail closed)", path)
    return _AUTH_UNREADABLE


def _write_auth_file(config_dir, data):
    """Replace webui_auth.json atomically."""
    path = os.path.join(config_dir, _AUTH_FILE)
    tmp = path + ".tmp"
    with open(tmp, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2)
    os.rename(tmp, path)


def _save_auth(config_dir, username, password):
    """Hash password and save auth credentials atomically. Returns the
    stored record."""
    record = {
        "username": username,
        "password_hash": _hash_password(password),
    }
    _write_auth_file(config_dir, record)
    logger.info("WebUI credentials created for user '%s'", username)
    return record


def _save_auth_off(config_dir):
    """Turn the login off: the stored credentials are replaced, not kept."""
    _write_auth_file(config_dir, _AUTH_OFF_MARKER)
    logger.warning("WebUI login turned off; anyone who can reach the "
                   "WebUI can use it")


def _text_field(body, name, max_len):
    """body[name] as a str ('' when absent), or None when it is not a string
    or is longer than *max_len*."""
    value = body.get(name)
    if value is None:
        return ''
    if not isinstance(value, str) or len(value) > max_len:
        return None
    return value


def _new_credentials(body):
    """(username, password, error) from a setup body; error is None when
    both are usable."""
    username = _text_field(body, 'username', _MAX_USERNAME_LENGTH)
    password = _text_field(body, 'password', _MAX_PASSWORD_LENGTH)
    if username is None or not username.isprintable():
        return None, None, (f"username must be at most "
                            f"{_MAX_USERNAME_LENGTH} printable characters")
    username = username.strip()
    if not username:
        return None, None, "username required"
    if password is None:
        return None, None, (f"password must be text of at most "
                            f"{_MAX_PASSWORD_LENGTH} characters")
    if len(password) < _MIN_PASSWORD_LENGTH:
        return None, None, (f"password must be at least "
                            f"{_MIN_PASSWORD_LENGTH} characters")
    if not password.strip():
        return None, None, "password cannot be all spaces"
    return username, password, None


def _create_session(auth):
    """Create a new session token with 24h expiry for the credentials
    record *auth*."""
    token = secrets.token_hex(32)
    with _sessions_lock:
        _sessions[token] = {
            "username": auth["username"],
            "password_hash": auth["password_hash"],
            "expires": time.time() + _SESSION_MAX_AGE,
        }
    return token


def _validate_session(token, auth):
    """Check the token is live and was issued under the credentials in
    *auth* (a _load_auth result); it is refused while they differ."""
    if not token or not isinstance(auth, dict):
        return False
    with _sessions_lock:
        session = _sessions.get(token)
        if not session:
            return False
        if time.time() > session["expires"]:
            del _sessions[token]
            return False
        return session["password_hash"] == auth["password_hash"]


def _invalidate_session(token):
    """Remove a session token."""
    with _sessions_lock:
        _sessions.pop(token, None)


def _clear_sessions():
    """End every session."""
    with _sessions_lock:
        _sessions.clear()


def _cleanup_sessions():
    """Remove all expired sessions."""
    now = time.time()
    with _sessions_lock:
        expired = [t for t, s in _sessions.items()
                   if now > s["expires"]]
        for t in expired:
            del _sessions[t]


def _prune_login_attempts(now):
    """Drop records whose window and lockout are both over. Caller holds the lock."""
    stale = [k for k, r in _login_attempts.items()
             if r["locked_until"] <= now
             and now - r["first"] > _LOGIN_ATTEMPT_WINDOW]
    for k in stale:
        del _login_attempts[k]


def _login_bucket(key, now):
    """*key*, or the shared overflow bucket while the table is full of live
    records, so no existing lockout is evicted. Caller holds the lock."""
    if key in _login_attempts or len(_login_attempts) < _LOGIN_MAX_TRACKED:
        return key
    _prune_login_attempts(now)
    if len(_login_attempts) < _LOGIN_MAX_TRACKED:
        return key
    return _LOGIN_OVERFLOW_KEY


def _login_begin_attempt(key):
    """Count a login attempt for *key* before the password is checked, so a
    parallel burst can't all pass the lockout check. Returns the remaining
    lockout seconds (>0) when the attempt must be refused, else 0."""
    now = time.time()
    with _login_attempts_lock:
        key = _login_bucket(key, now)
        rec = _login_attempts.get(key)
        if rec and rec["locked_until"] > now:
            return int(rec["locked_until"] - now) + 1
        if not rec or now - rec["first"] > _LOGIN_ATTEMPT_WINDOW:
            rec = {"count": 0, "first": now, "locked_until": 0}
            _login_attempts[key] = rec
        rec["count"] += 1
        if rec["count"] >= _LOGIN_MAX_ATTEMPTS:
            rec["locked_until"] = now + _LOGIN_LOCKOUT_SECONDS
            rec["count"] = 0
            rec["first"] = now
        return 0


@functools.lru_cache(maxsize=8)
def _trusted_proxy_networks(raw):
    """Parse WEBUI_TRUSTED_PROXIES (comma-separated IPs/CIDRs). A bad entry is
    skipped, which only trusts less."""
    networks = []
    for part in raw.split(','):
        part = part.strip()
        if not part:
            continue
        try:
            networks.append(ipaddress.ip_network(part, strict=False))
        except ValueError:
            # No entry text: CodeQL reads TRUSTED in the name as a secret.
            logger.warning("Ignoring a WEBUI_TRUSTED_PROXIES entry that is "
                           "not an IP or CIDR")
    return tuple(networks)


def _parse_ip(value):
    """IP address from a socket peer or X-Forwarded-For hop, or None."""
    try:
        addr = ipaddress.ip_address(value.strip())
    except ValueError:
        return None
    if addr.version == 6 and addr.ipv4_mapped:
        return addr.ipv4_mapped
    return addr


def _resolve_client_ip(peer, forwarded_for, trusted_raw):
    """The client IP to rate-limit. X-Forwarded-For counts only when the socket
    peer is a trusted proxy, and then only the right-most hop that isn't one,
    within the last _MAX_FORWARDED_HOPS: the left end is whatever the client
    sent, so it can be forged."""
    networks = _trusted_proxy_networks(trusted_raw)
    peer_ip = _parse_ip(peer)
    if (not networks or peer_ip is None
            or not any(peer_ip in n for n in networks)):
        return peer
    # Only the right-most hops can matter; rsplit keeps a long header cheap.
    tail = forwarded_for.rsplit(',', _MAX_FORWARDED_HOPS)[-_MAX_FORWARDED_HOPS:]
    hops = [h for h in tail if h.strip()]
    for hop in reversed(hops):
        ip = _parse_ip(hop)
        if ip is None:
            return peer
        if not any(ip in n for n in networks):
            return str(ip)
    return peer


def _login_record_success(key):
    """Clear the failure counter for *key* on a successful login."""
    with _login_attempts_lock:
        _login_attempts.pop(key, None)


# ---------------------------------------------------------------------------
# API helpers
# ---------------------------------------------------------------------------

_API_KEY_MASK = "********"

# The canonical config-key map lives in main.py; import it so the WebUI's
# allowed-keys list and parser stay in sync without duplication.
from main import (  # noqa: E402
    _CONFIG_KEY_MAP, _load_corrupt_details, _parse_config_lines,
    write_json_atomic, write_text_atomic,
)
_ALLOWED_CONFIG_KEYS = frozenset(_CONFIG_KEY_MAP)

_config_write_lock = threading.Lock()


def _str_list(body, name):
    """body[name] when it is a non-empty list of non-blank strings with no
    NUL byte, else None."""
    value = body.get(name)
    if (isinstance(value, list) and value
            and all(isinstance(v, str) and v.strip() and '\x00' not in v
                    for v in value)):
        return value
    return None


def _config_updates(body):
    """body["config"] if it is a non-empty map of finite numbers, booleans or
    single-line strings (a line break would add config lines), else None."""
    updates = body.get('config')
    if not isinstance(updates, dict) or not updates:
        return None
    for val in updates.values():
        if not isinstance(val, (str, int, float)):
            return None
        if isinstance(val, str) and not val.isprintable():
            return None
        if isinstance(val, float) and not math.isfinite(val):
            return None
    return updates


def _read_config_entries(config_dir):
    """Return [{key, value}] in file order for display in the WebUI."""
    return [
        {"key": k, "value": v}
        for k, v in _parse_config_lines(
            os.path.join(config_dir, "beatscheck.conf")).items()
    ]


def _format_config_value(val):
    """Quote a config value unless it's a plain number or boolean."""
    s = str(val)
    if re.match(r'^(\d+(?:\.\d+)?|true|false)$', s):
        return s
    return f'"{s}"'


def _write_config_file(config_dir, updates):
    """Update beatscheck.conf preserving comments and structure.
    Thread-safe via _config_write_lock; atomic via tmp+rename."""
    with _config_write_lock:
        path = os.path.join(config_dir, "beatscheck.conf")
        if not os.path.isfile(path):
            return False

        new_vals = {k.lower(): v for k, v in updates.items()}
        written_keys = set()

        with open(path, 'r') as f:
            lines = f.readlines()

        result = []
        for line in lines:
            stripped = line.strip()
            commented_match = re.match(r'^#\s*(\w+)\s*=', stripped)
            active_match = re.match(r'^(\w+)\s*=', stripped)
            match = active_match or commented_match
            if match:
                key = match.group(1).lower()
                if key in new_vals:
                    result.append(
                        f"{key} = {_format_config_value(new_vals[key])}\n")
                    written_keys.add(key)
                    continue
            result.append(line)

        for key, val in new_vals.items():
            if key not in written_keys:
                result.append(
                    f"{key} = {_format_config_value(val)}\n")

        write_text_atomic(path, ''.join(result))
        return True


_corrupt_cache = {"key": None, "value": None}
_corrupt_cache_lock = threading.Lock()


def _read_corrupt_list(config_dir):
    """Read corrupt.txt and corrupt_details.json, merge into list.
    Results are cached per (corrupt.txt, corrupt_details.json) mtime pair
    so repeated polls don't re-stat every corrupt file."""
    corrupt_path = os.path.join(config_dir, "corrupt.txt")
    details_path = os.path.join(config_dir, "corrupt_details.json")

    def _mtime(p):
        try:
            return os.path.getmtime(p)
        except OSError:
            return 0

    key = (_mtime(corrupt_path), _mtime(details_path))
    with _corrupt_cache_lock:
        if _corrupt_cache["key"] == key and _corrupt_cache["value"] is not None:
            return _corrupt_cache["value"]

    paths = []
    if os.path.isfile(corrupt_path):
        with open(corrupt_path, 'r', encoding='utf-8') as f:
            paths = [line.strip() for line in f if line.strip()]

    details = _load_corrupt_details(config_dir)
    dir_totals = {}

    result = []
    for p in paths:
        info = details.get(p, {})
        entry = {"path": p, "reason": info.get("reason", "")}
        if "trackfileId" in info:
            entry["has_lidarr_id"] = True
        try:
            entry["size"] = os.path.getsize(p)
        except OSError:
            entry["size"] = 0
            entry["missing"] = True
        d = os.path.dirname(p)
        if d not in dir_totals:
            try:
                dir_totals[d] = len([
                    f for f in os.listdir(d)
                    if os.path.isfile(os.path.join(d, f))])
            except OSError:
                dir_totals[d] = 0
        entry["album_total"] = dir_totals[d]
        result.append(entry)

    with _corrupt_cache_lock:
        _corrupt_cache["key"] = key
        _corrupt_cache["value"] = result
    return result


def _read_log_tail(config_dir, lines=200):
    """Read last N lines of beats_check.log. Returns (text, mtime)."""
    log_path = os.path.join(config_dir, "beats_check.log")
    try:
        st = os.stat(log_path)
    except OSError:
        return ("", 0)
    try:
        with open(log_path, 'rb') as f:
            size = st.st_size
            chunk = min(size, 256 * 1024)
            f.seek(max(0, size - chunk))
            data = f.read().decode('utf-8', errors='replace')
        all_lines = data.splitlines()
        return ('\n'.join(all_lines[-lines:]), int(st.st_mtime))
    except OSError:
        return ("", 0)


def _read_summary(config_dir):
    """Read summary.json from last scan."""
    path = os.path.join(config_dir, "summary.json")
    if not os.path.isfile(path):
        return {}
    try:
        with open(path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except (json.JSONDecodeError, ValueError):
        return {}


def _prune_json(path, keys):
    """Remove *keys* from a JSON-object file, atomic write on change."""
    if not os.path.isfile(path):
        return
    try:
        with open(path, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except (json.JSONDecodeError, OSError):
        return
    for k in keys:
        data.pop(k, None)
    try:
        write_json_atomic(path, data)
    except OSError:
        pass


def _ignore_corrupt_files(config_dir, files):
    """Remove files from corrupt.txt and the JSON state files without
    deleting the actual files. They'll reappear on the next scan if
    they're still corrupt."""
    corrupt_path = os.path.join(config_dir, "corrupt.txt")
    ignore_set = set(files)

    if os.path.isfile(corrupt_path):
        with open(corrupt_path, 'r', encoding='utf-8') as f:
            remaining = [line.strip() for line in f
                         if line.strip() and line.strip() not in ignore_set]
        write_text_atomic(corrupt_path,
                          ''.join(p + '\n' for p in remaining))

    _prune_json(os.path.join(config_dir, "corrupt_details.json"), files)
    _prune_json(os.path.join(config_dir, "corrupt_tracking.json"), files)


def _is_subpath(child, root):
    """True if *child* is *root* itself or a path beneath it.
    Both paths must already be resolved with os.path.realpath().
    Uses a separator-aware check so /data2 is NOT treated as a
    child of /data."""
    return child == root or child.startswith(root + os.sep)


def _list_dir(path):
    """List immediate subdirectories of a path. Used by folder picker."""
    real = os.path.realpath(path)
    if not _is_subpath(real, os.path.realpath('/data')):
        return []
    if not os.path.isdir(real):
        return []
    try:
        return sorted([
            os.path.join(path, n) for n in os.listdir(real)
            if os.path.isdir(os.path.join(real, n))
            and not n.startswith('.')])
    except OSError:
        return []


# ---------------------------------------------------------------------------
# Bulk-delete job tracker (in-memory; survives only while WebUI is running)
# ---------------------------------------------------------------------------

_delete_jobs = {}
_delete_jobs_lock = threading.Lock()


def _new_delete_job(total, mode):
    """Register a new delete job and return its id."""
    job_id = secrets.token_hex(8)
    with _delete_jobs_lock:
        _delete_jobs[job_id] = {
            "id": job_id,
            "mode": mode,
            "total": total,
            "done": 0,
            "current": "",
            "phase": "queued",
            "errors": [],
            "deleted": 0,
            "finished": False,
            "cancelled": False,
            "cancel_requested": False,
            "started_at": time.time(),
        }
    return job_id


def _update_delete_job(job_id, **kwargs):
    with _delete_jobs_lock:
        job = _delete_jobs.get(job_id)
        if job is not None:
            job.update(kwargs)


def _get_delete_job(job_id):
    with _delete_jobs_lock:
        job = _delete_jobs.get(job_id)
        if job is None:
            return None
        return dict(job)


def _cancel_delete_job(job_id):
    with _delete_jobs_lock:
        job = _delete_jobs.get(job_id)
        if job is None:
            return False
        job["cancel_requested"] = True
        return True


def _prune_delete_jobs(max_age=3600):
    """Drop finished jobs older than *max_age* to keep the store bounded."""
    cutoff = time.time() - max_age
    with _delete_jobs_lock:
        stale = [jid for jid, j in _delete_jobs.items()
                 if j.get("finished") and j.get("started_at", 0) < cutoff]
        for jid in stale:
            _delete_jobs.pop(jid, None)


def _run_delete_job(job_id, folders, config_dir, music_dir, mode):
    """Background worker that runs delete_album_folders under the scan
    lock with progress + cancel wired to the in-memory job store."""
    try:
        from main import (
            _acquire_scan_lock, delete_album_folders,
        )
    except ImportError:
        _update_delete_job(
            job_id, finished=True, phase="error",
            errors=[{"folder": "", "error": "delete not available"}])
        return

    def progress_cb(index, total, folder, phase):
        _update_delete_job(
            job_id, done=index, total=total,
            current=folder, phase=phase)

    def cancel_cb():
        job = _get_delete_job(job_id)
        return bool(job and job.get("cancel_requested"))

    lock_fd = None
    try:
        lock_fd = _acquire_scan_lock(config_dir)
    except OSError:
        _update_delete_job(
            job_id, finished=True, phase="error",
            errors=[{"folder": "",
                     "error": "could not acquire scan lock"}])
        return

    try:
        _update_delete_job(job_id, phase="running")
        result = delete_album_folders(
            folders, config_dir, music_dir=music_dir,
            mode=mode, progress_cb=progress_cb, cancel_cb=cancel_cb)
        _update_delete_job(
            job_id,
            deleted=result.get("count", 0),
            errors=result.get("errors", []),
            cancelled=result.get("cancelled", False),
            finished=True,
            phase="cancelled" if result.get("cancelled") else "done",
        )
    except Exception as e:  # noqa: BLE001
        logger.exception("Bulk delete job %s failed", job_id)
        _update_delete_job(
            job_id, finished=True, phase="error",
            errors=[{"folder": "", "error": str(e)}])
    finally:
        try:
            import fcntl as _fcntl
            if lock_fd is not None:
                _fcntl.flock(lock_fd.fileno(), _fcntl.LOCK_UN)
                lock_fd.close()
                try:
                    os.remove(os.path.join(config_dir, ".scanning"))
                except OSError:
                    pass
        except ImportError:
            pass
        _prune_delete_jobs()


def _run_delete_files_job(job_id, files, config_dir, music_dir):
    """Background worker that runs delete_corrupt_files under the scan
    lock with progress + cancel wired to the in-memory job store. Used
    by /api/delete-files so multi-album file deletes don't block the
    browser for the full 30s-per-album Lidarr search window."""
    try:
        from main import _acquire_scan_lock, delete_corrupt_files
    except ImportError:
        _update_delete_job(
            job_id, finished=True, phase="error",
            errors=[{"path": "", "error": "delete not available"}])
        return

    def progress_cb(index, total, label, phase):
        _update_delete_job(
            job_id, done=index, total=total,
            current=label, phase=phase)

    def cancel_cb():
        job = _get_delete_job(job_id)
        return bool(job and job.get("cancel_requested"))

    lock_fd = None
    try:
        lock_fd = _acquire_scan_lock(config_dir)
    except OSError:
        _update_delete_job(
            job_id, finished=True, phase="error",
            errors=[{"path": "",
                     "error": "could not acquire scan lock"}])
        return

    try:
        _update_delete_job(job_id, phase="running")
        result = delete_corrupt_files(
            files, config_dir, music_dir=music_dir,
            progress_cb=progress_cb, cancel_cb=cancel_cb)
        cancelled = bool(
            _get_delete_job(job_id) and
            _get_delete_job(job_id).get("cancel_requested"))
        _update_delete_job(
            job_id,
            deleted=result.get("count", 0),
            errors=result.get("errors", []),
            cancelled=cancelled,
            finished=True,
            phase="cancelled" if cancelled else "done",
        )
    except Exception as e:  # noqa: BLE001
        logger.exception("Bulk file-delete job %s failed", job_id)
        _update_delete_job(
            job_id, finished=True, phase="error",
            errors=[{"path": "", "error": str(e)}])
    finally:
        try:
            import fcntl as _fcntl
            if lock_fd is not None:
                _fcntl.flock(lock_fd.fileno(), _fcntl.LOCK_UN)
                lock_fd.close()
                try:
                    os.remove(os.path.join(config_dir, ".scanning"))
                except OSError:
                    pass
        except ImportError:
            pass
        _prune_delete_jobs()


def _trigger_rescan(config_dir, mode="report", fresh=False):
    """Trigger a rescan by writing the .rescan file.
    When *fresh* is True the trigger content is prefixed with ``fresh:``
    so the scanner clears the resume cache at scan start (not now),
    avoiding a race where a running scan rebuilds processed.txt
    between the delete and the next scan."""
    rescan_path = os.path.join(config_dir, ".rescan")
    try:
        content = ("fresh:" + mode) if fresh else mode
        with open(rescan_path, 'w') as f:
            f.write(content)
        return True
    except OSError:
        return False


# ---------------------------------------------------------------------------
# HTTP Request Handler
# ---------------------------------------------------------------------------

_MAX_BODY = 1_048_576  # 1 MB
_SESSION_COOKIE = "beatscheck_session"


class WebUIHandler(SimpleHTTPRequestHandler):
    """Handles both static files and /api/* JSON endpoints."""

    # Per socket read/write, so a client that stalls mid-request frees its thread.
    timeout = 30

    def log_message(self, format, *args):
        pass

    def _json_response(self, data, status=200, cookies=None):
        body = json.dumps(data).encode('utf-8')
        try:
            self.send_response(status)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Content-Length', len(body))
            self.send_header('Cache-Control', 'no-cache')
            if cookies:
                for cookie in cookies:
                    self.send_header('Set-Cookie', cookie)
            self.end_headers()
            self.wfile.write(body)
        except (ConnectionResetError, BrokenPipeError):
            # Client disconnected before we could reply (common for
            # long-running endpoints like /api/delete). Server-side
            # work already completed; nothing to do.
            pass

    def _read_body(self):
        """Parse JSON request body. Returns dict or None on error."""
        try:
            length = int(self.headers.get('Content-Length', 0))
        except (ValueError, TypeError):
            self._json_response(
                {"error": "invalid content-length"}, 400)
            return None
        if length <= 0 or length > _MAX_BODY:
            self._json_response(
                {"error": "body too large or empty"}, 400)
            return None
        try:
            body = json.loads(self.rfile.read(length))
            # json.loads keeps a lone \ud800 escape; this re-encode refuses it.
            json.dumps(body, ensure_ascii=False).encode('utf-8')
        except (ValueError, RecursionError):
            self._json_response({"error": "invalid JSON"}, 400)
            return None
        if not isinstance(body, dict):
            self._json_response({"error": "body must be a JSON object"}, 400)
            return None
        return body

    def _get_session_token(self):
        """Extract session token from cookies."""
        jar = http.cookies.SimpleCookie()
        jar.load(self.headers.get('Cookie', ''))
        morsel = jar.get(_SESSION_COOKIE)
        return morsel.value if morsel else None

    def _check_auth(self):
        """True when the login is off or the session is valid; otherwise
        sends 401 and returns False."""
        _cleanup_sessions()
        auth = _load_auth(self.server.config_dir)
        if (auth is _AUTH_OFF
                or _validate_session(self._get_session_token(), auth)):
            return True
        self._json_response({"error": "unauthorized"}, 401)
        return False

    def _throttled(self, key):
        """Count a password attempt for *key*. Sends 429 and returns True
        when *key* is locked out."""
        locked = _login_begin_attempt(key)
        if locked:
            self._json_response(
                {"error": f"too many failed attempts — try again in "
                 f"{locked}s"}, 429)
        return bool(locked)

    def _client_ip(self):
        """Client IP for rate-limiting; see _resolve_client_ip."""
        return _resolve_client_ip(
            self.client_address[0],
            self.headers.get('X-Forwarded-For', ''),
            os.environ.get('WEBUI_TRUSTED_PROXIES', ''))

    def _session_cookie(self, token, max_age=_SESSION_MAX_AGE):
        """Build a Set-Cookie header value. The Secure attribute is added
        when the request arrived over HTTPS (directly or via a reverse
        proxy that sets X-Forwarded-Proto), or when explicitly forced via
        WEBUI_FORCE_SECURE_COOKIE — so it hardens the documented HTTPS
        deployment without breaking a plain-HTTP LAN install."""
        xfp = self.headers.get(
            'X-Forwarded-Proto', '').split(',')[0].strip().lower()
        forced = os.environ.get(
            'WEBUI_FORCE_SECURE_COOKIE', '').strip().lower() \
            in ('1', 'true', 'yes', 'on')
        secure = "Secure; " if (xfp == 'https' or forced) else ""
        return (f"{_SESSION_COOKIE}={token}; "
                f"HttpOnly; SameSite=Strict; {secure}Path=/; "
                f"Max-Age={max_age}")

    def do_GET(self):
        config_dir = self.server.config_dir

        # Auth-exempt: status check and static assets
        if self.path == '/api/auth-status':
            auth = _load_auth(config_dir)
            token = self._get_session_token()
            if auth is _AUTH_UNREADABLE:
                # Fail closed: setup_required would reopen the setup wizard.
                self._json_response({
                    "setup_required": False,
                    "authenticated": False,
                    "error": "auth store unreadable",
                }, 503)
                return
            self._json_response({
                "setup_required": auth is None,
                "login_required": auth is not _AUTH_OFF,
                "authenticated": (auth is _AUTH_OFF
                                  or _validate_session(token, auth)),
            })
            return

        # Static assets are always served (login page needs them)
        if self.path == '/' or self.path == '/index.html':
            self._serve_static('index.html')
            return
        if not self.path.startswith('/api/'):
            self._serve_static(self.path.lstrip('/'))
            return

        # All other API endpoints require auth
        if not self._check_auth():
            return

        if self.path == '/api/status':
            state = app_state.snapshot()
            summary = _read_summary(config_dir)
            # Merge live library size from scan into summary so the
            # dashboard shows it immediately, not only after scan ends
            for key in ('library_size_human', 'library_files'):
                val = state.get(key)
                if val is not None:
                    summary[key] = val
            state["summary"] = summary
            # Lets an open page notice the login was turned off elsewhere.
            state["login_required"] = _load_auth(config_dir) is not _AUTH_OFF
            self._json_response(state)

        elif self.path == '/api/config':
            entries = _read_config_entries(config_dir)
            for entry in entries:
                if entry["key"] == "lidarr_api_key" \
                        and entry["value"]:
                    entry["value"] = _API_KEY_MASK
            self._json_response({"config": entries})

        elif self.path == '/api/corrupt':
            files = _read_corrupt_list(config_dir)
            self._json_response(
                {"files": files, "count": len(files)})

        elif self.path == '/api/log' or self.path.startswith('/api/log?'):
            params = urllib.parse.parse_qs(urllib.parse.urlparse(self.path).query)
            try:
                lines = max(1, min(
                    int(params.get('lines', ['200'])[0]), 10000))
            except (ValueError, TypeError):
                lines = 200
            since = params.get('since', ['0'])[0]
            try:
                since_mtime = int(since)
            except (ValueError, TypeError):
                since_mtime = 0
            text, mtime = _read_log_tail(config_dir, lines)
            if mtime and mtime == since_mtime:
                self._json_response({"log": None, "mtime": mtime, "unchanged": True})
            else:
                self._json_response({"log": text, "mtime": mtime})

        elif (self.path == '/api/delete-job-status'
                or self.path.startswith('/api/delete-job-status?')):
            params = urllib.parse.parse_qs(
                urllib.parse.urlparse(self.path).query)
            job_id = params.get('id', [''])[0]
            job = _get_delete_job(job_id)
            if job is None:
                self._json_response({"error": "job not found"}, 404)
                return
            self._json_response(job)

        elif self.path == '/api/paths' or self.path.startswith('/api/paths?'):
            params = urllib.parse.parse_qs(urllib.parse.urlparse(self.path).query)
            parent = params.get('dir', ['/data'])[0]
            real = os.path.realpath(parent)
            if not _is_subpath(real, os.path.realpath('/data')):
                self._json_response(
                    {"error": "path outside /data"}, 403)
                return
            self._json_response({
                "path": parent,
                "children": _list_dir(parent),
            })

        else:
            self._json_response({"error": "not found"}, 404)

    def _handle_setup(self, config_dir):
        """Handle POST /api/setup — first-run setup, or turning the login
        back on. {"login": false} chooses no login, on first run only."""
        body = self._read_body()
        if body is None:
            return
        login = body.get('login', True)
        if not isinstance(login, bool):
            self._json_response(
                {"error": "login must be true or false"}, 400)
            return
        with _auth_lock:
            auth = _load_auth(config_dir)
            if auth is _AUTH_UNREADABLE:
                self._json_response(
                    {"error": "auth store unreadable — refusing setup"},
                    503)
                return
            if auth is not None and auth is not _AUTH_OFF:
                self._json_response(
                    {"error": "already configured"}, 409)
                return
            if not login:
                if auth is _AUTH_OFF:
                    self._json_response(
                        {"error": "login is already off"}, 409)
                    return
                _save_auth_off(config_dir)
                self._json_response({"ok": True})
                return
            username, password, problem = _new_credentials(body)
            if problem:
                self._json_response({"error": problem}, 400)
                return
            token = _create_session(
                _save_auth(config_dir, username, password))
        self._json_response(
            {"ok": True},
            cookies=[self._session_cookie(token)])

    def _handle_login(self, config_dir):
        """Handle POST /api/login — credential validation."""
        key = self._client_ip()
        if self._throttled(key):
            return
        body = self._read_body()
        if body is None:
            return
        auth = _load_auth(config_dir)
        if auth is _AUTH_UNREADABLE:
            self._json_response(
                {"error": "auth store unreadable"}, 503)
            return
        if auth is None:
            self._json_response(
                {"error": "setup required"}, 409)
            return
        if auth is _AUTH_OFF:
            self._json_response({"error": "login is off"}, 409)
            return
        username = _text_field(body, 'username', _MAX_USERNAME_LENGTH)
        password = _text_field(body, 'password', _MAX_PASSWORD_LENGTH)
        if username is None or password is None:
            self._json_response(
                {"error": f"username and password must be text of at most "
                 f"{_MAX_USERNAME_LENGTH} and {_MAX_PASSWORD_LENGTH} "
                 f"characters"}, 400)
            return
        if (username.strip() == auth['username']
                and _verify_password(
                    password, auth['password_hash'])):
            _login_record_success(key)
            token = _create_session(auth)
            self._json_response(
                {"ok": True},
                cookies=[self._session_cookie(token)])
        else:
            self._json_response(
                {"error": "invalid credentials"}, 401)

    def _handle_logout(self):
        """Handle POST /api/logout — session invalidation."""
        token = self._get_session_token()
        if token:
            _invalidate_session(token)
        self._json_response(
            {"ok": True},
            cookies=[self._session_cookie("", max_age=0)])

    def _handle_auth_disable(self, config_dir):
        """Handle POST /api/auth/disable — turn the login off. Needs a
        session and the current password."""
        key = self._client_ip()
        if self._throttled(key):
            return
        body = self._read_body()
        if body is None:
            return
        password = _text_field(body, 'password', _MAX_PASSWORD_LENGTH)
        if password is None:
            self._json_response(
                {"error": f"password must be text of at most "
                 f"{_MAX_PASSWORD_LENGTH} characters"}, 400)
            return
        with _auth_lock:
            auth = _load_auth(config_dir)
            if auth is _AUTH_UNREADABLE:
                self._json_response(
                    {"error": "auth store unreadable"}, 503)
                return
            if auth is None or auth is _AUTH_OFF:
                self._json_response(
                    {"error": "login is already off"}, 409)
                return
            # 403, not 401: app.js treats a 401 as a dead session.
            if not _verify_password(password, auth['password_hash']):
                self._json_response({"error": "wrong password"}, 403)
                return
            _login_record_success(key)
            _save_auth_off(config_dir)
            _clear_sessions()
        self._json_response(
            {"ok": True},
            cookies=[self._session_cookie("", max_age=0)])

    def _handle_ignore(self, config_dir):
        """Handle POST /api/ignore — remove files from corrupt list."""
        body = self._read_body()
        if body is None:
            return
        files = _str_list(body, 'files')
        if files is None:
            self._json_response(
                {"error": "files must be a non-empty list of paths"}, 400)
            return
        _ignore_corrupt_files(config_dir, files)
        self._json_response({"ok": True})

    def _handle_rescan(self, config_dir):
        """Handle POST /api/rescan — start a scan now."""
        body = self._read_body()
        if body is None:
            return
        mode = body.get('mode', 'report')
        fresh = body.get('fresh', False)
        if mode not in ('report', 'move'):
            self._json_response(
                {"error": "mode must be report or move"}, 400)
            return
        if not isinstance(fresh, bool):
            self._json_response(
                {"error": "fresh must be true or false"}, 400)
            return
        ok = _trigger_rescan(config_dir, mode, fresh)
        self._json_response({"ok": ok})

    def do_POST(self):
        config_dir = self.server.config_dir

        # A cross-site page can POST text/plain without a CORS preflight, but
        # not JSON; while login is off, nothing else stops it.
        if self.headers.get_content_type() != 'application/json':
            self._json_response(
                {"error": "Content-Type must be application/json"}, 415)
            return

        if self._dispatch_auth(config_dir):
            return

        # Protected endpoints
        if not self._check_auth():
            return

        if self.path == '/api/config':
            body = self._read_body()
            if body is None:
                return
            updates = _config_updates(body)
            if updates is None:
                self._json_response(
                    {"error": "config must be a non-empty object of "
                     "numbers, booleans or single-line text"}, 400)
                return
            # Reject unknown config keys
            rejected = set(updates) - _ALLOWED_CONFIG_KEYS
            if rejected:
                self._json_response(
                    {"error": "unknown keys: "
                     + ", ".join(sorted(rejected))}, 400)
                return
            # Don't overwrite real key with the mask sentinel
            if updates.get('lidarr_api_key') == _API_KEY_MASK:
                del updates['lidarr_api_key']
            ok = _write_config_file(config_dir, updates)
            if ok:
                # Sync updated values to os.environ so other
                # operations (e.g. delete) see them immediately.
                for key, val in updates.items():
                    env_name = key.upper()
                    os.environ[env_name] = str(val)
                self._json_response({"ok": True})
            else:
                self._json_response(
                    {"error": "config file not found"}, 500)

        elif self.path == '/api/rescan':
            self._handle_rescan(config_dir)

        elif self.path == '/api/cancel':
            try:
                from main import cancel_scan
                cancel_scan()
                self._json_response({"ok": True})
            except ImportError:
                self._json_response(
                    {"error": "cancel not available"}, 500)

        elif self.path == '/api/delete':
            body = self._read_body()
            if body is None:
                return
            files = _str_list(body, 'files')
            if files is None:
                self._json_response(
                    {"error": "files must be a non-empty list of paths"},
                    400)
                return
            try:
                from main import delete_corrupt_files
            except ImportError:
                self._json_response(
                    {"error": "delete not available"}, 500)
                return
            music_dir = os.environ.get("MUSIC_DIR", "/data")
            result = delete_corrupt_files(
                files, config_dir, music_dir=music_dir)
            self._json_response(result)

        elif self._dispatch_bulk_delete(config_dir):
            return

        elif self.path == '/api/ignore':
            self._handle_ignore(config_dir)

        else:
            self._json_response({"error": "not found"}, 404)

    def _dispatch_auth(self, config_dir):
        """Handle the login POSTs. Returns True if the path matched
        (response already sent), False otherwise."""
        if self.path == '/api/setup':
            self._handle_setup(config_dir)
            return True
        if self.path == '/api/login':
            self._handle_login(config_dir)
            return True
        if self.path == '/api/logout':
            self._handle_logout()
            return True
        if self.path == '/api/auth/disable':
            if self._check_auth():
                self._handle_auth_disable(config_dir)
            return True
        return False

    def _dispatch_bulk_delete(self, config_dir):
        """Handle bulk-delete-related POSTs. Returns True if the path
        matched (response already sent), False otherwise."""
        if self.path == '/api/delete-albums':
            self._handle_delete_albums(config_dir)
            return True
        if self.path == '/api/delete-files':
            self._handle_delete_files(config_dir)
            return True
        if (self.path == '/api/delete-job-cancel'
                or self.path.startswith('/api/delete-job-cancel?')):
            params = urllib.parse.parse_qs(
                urllib.parse.urlparse(self.path).query)
            ok = _cancel_delete_job(params.get('id', [''])[0])
            self._json_response({"ok": ok})
            return True
        return False

    def _handle_delete_albums(self, config_dir):
        """POST /api/delete-albums — fire-and-forget bulk delete.
        Returns 202 + {job_id}; progress polled via
        /api/delete-job-status?id=..."""
        body = self._read_body()
        if body is None:
            return
        folders = _str_list(body, 'folders')
        mode = body.get('mode', 'whole')
        if folders is None:
            self._json_response(
                {"error": "folders must be a non-empty list of paths"}, 400)
            return
        if mode not in ('whole', 'corrupt'):
            self._json_response(
                {"error": "mode must be 'whole' or 'corrupt'"}, 400)
            return
        try:
            from main import _MAX_DELETE_ALBUMS
        except ImportError:
            _MAX_DELETE_ALBUMS = 50
        if len(folders) > _MAX_DELETE_ALBUMS:
            self._json_response(
                {"error": f"too many folders "
                 f"(max {_MAX_DELETE_ALBUMS})"}, 400)
            return
        # Refuse to start if a scan is running
        lock_path = os.path.join(config_dir, ".scanning")
        if os.path.exists(lock_path):
            self._json_response(
                {"error": "scan in progress — cannot delete"}, 409)
            return
        music_dir = os.environ.get("MUSIC_DIR", "/data")
        job_id = _new_delete_job(len(folders), mode)
        thread = threading.Thread(
            target=_run_delete_job,
            args=(job_id, folders, config_dir, music_dir, mode),
            daemon=True,
        )
        thread.start()
        self._json_response(
            {"job_id": job_id, "total": len(folders)}, 202)

    def _handle_delete_files(self, config_dir):
        """POST /api/delete-files — fire-and-forget file-level delete.
        Body: {"files": [...]}. Returns 202 + {job_id}; progress polled
        via /api/delete-job-status?id=..."""
        body = self._read_body()
        if body is None:
            return
        files = _str_list(body, 'files')
        if files is None:
            self._json_response(
                {"error": "files must be a non-empty list of paths"}, 400)
            return
        if len(files) > 5000:
            self._json_response(
                {"error": "too many files (max 5000)"}, 400)
            return
        lock_path = os.path.join(config_dir, ".scanning")
        if os.path.exists(lock_path):
            self._json_response(
                {"error": "scan in progress — cannot delete"}, 409)
            return
        music_dir = os.environ.get("MUSIC_DIR", "/data")
        # Progress is measured per-album; Lidarr lookup happens inside
        # the worker. Report total as file count for the initial display.
        job_id = _new_delete_job(len(files), "files")
        thread = threading.Thread(
            target=_run_delete_files_job,
            args=(job_id, files, config_dir, music_dir),
            daemon=True,
        )
        thread.start()
        self._json_response(
            {"job_id": job_id, "total": len(files)}, 202)

    def _serve_static(self, filename):
        """Serve a file from the static directory."""
        static_dir = self.server.static_dir
        filepath = os.path.join(static_dir, filename)
        real = os.path.realpath(filepath)
        if not _is_subpath(real, os.path.realpath(static_dir)):
            self.send_error(403)
            return
        if not os.path.isfile(real):
            self.send_error(404)
            return

        ext = os.path.splitext(filename)[1].lower()
        ct, _ = mimetypes.guess_type(filename)
        ct = ct or 'application/octet-stream'
        if ext in ('.html', '.css', '.js'):
            ct += '; charset=utf-8'

        with open(real, 'rb') as f:  # CodeQL[py/path-injection] false positive: real is validated by _is_subpath() above
            body = f.read()
        self.send_response(200)
        self.send_header('Content-Type', ct)
        self.send_header('Content-Length', len(body))
        if ext in ('.html', '.css', '.js'):
            self.send_header('Cache-Control', 'no-cache')
        self.end_headers()
        self.wfile.write(body)


# ---------------------------------------------------------------------------
# Threaded HTTP Server
# ---------------------------------------------------------------------------


class ThreadedHTTPServer(ThreadingMixIn, HTTPServer):
    daemon_threads = True

    def __init__(self, addr, handler, config_dir, static_dir):
        self.config_dir = config_dir
        self.static_dir = static_dir
        super().__init__(addr, handler)


def start_webui(config_dir, port=8484, static_dir=None):
    """Start the WebUI HTTP server in a daemon thread."""
    if static_dir is None:
        static_dir = os.path.join(
            os.path.dirname(__file__), 'static')

    server = ThreadedHTTPServer(
        ('0.0.0.0', port), WebUIHandler, config_dir, static_dir)

    thread = threading.Thread(
        target=server.serve_forever, daemon=True)
    thread.start()
    logger.info("WebUI started on port %d", port)
    return server
