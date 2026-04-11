"""BeatsCheck WebUI — optional web interface served via Python stdlib."""

import hashlib
import hmac
import json
import logging
import os
import re
import secrets
import threading
import time
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


# Module-level singleton — set by beats_check.main()
app_state = AppState()

# ---------------------------------------------------------------------------
# Authentication
# ---------------------------------------------------------------------------

_AUTH_FILE = "webui_auth.json"
_PBKDF2_ITERATIONS = 100_000
_SESSION_MAX_AGE = 86400  # 24 hours

_sessions = {}
_sessions_lock = threading.Lock()


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
    """Load auth credentials from webui_auth.json."""
    path = os.path.join(config_dir, _AUTH_FILE)
    if not os.path.isfile(path):
        return None
    try:
        with open(path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        if data.get('username') and data.get('password_hash'):
            return data
    except (json.JSONDecodeError, OSError, ValueError):
        pass
    return None


def _save_auth(config_dir, username, password):
    """Hash password and save auth credentials atomically."""
    path = os.path.join(config_dir, _AUTH_FILE)
    tmp = path + ".tmp"
    data = {
        "username": username,
        "password_hash": _hash_password(password),
    }
    with open(tmp, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2)
    os.rename(tmp, path)
    logger.info("WebUI credentials created for user '%s'", username)


def _create_session(username):
    """Create a new session token with 24h expiry."""
    token = secrets.token_hex(32)
    with _sessions_lock:
        _sessions[token] = {
            "username": username,
            "expires": time.time() + _SESSION_MAX_AGE,
        }
    return token


def _validate_session(token):
    """Check if session token is valid and not expired."""
    if not token:
        return False
    with _sessions_lock:
        session = _sessions.get(token)
        if not session:
            return False
        if time.time() > session["expires"]:
            del _sessions[token]
            return False
        return True


def _invalidate_session(token):
    """Remove a session token."""
    with _sessions_lock:
        _sessions.pop(token, None)


def _cleanup_sessions():
    """Remove all expired sessions."""
    now = time.time()
    with _sessions_lock:
        expired = [t for t, s in _sessions.items()
                   if now > s["expires"]]
        for t in expired:
            del _sessions[t]


# ---------------------------------------------------------------------------
# API helpers
# ---------------------------------------------------------------------------

_API_KEY_MASK = "********"

# Valid config keys (mirrors _CONFIG_KEY_MAP in beats_check.py).
# Hardcoded to avoid circular import.
_ALLOWED_CONFIG_KEYS = frozenset({
    'output_dir', 'mode', 'workers', 'run_interval', 'delete_after',
    'max_auto_delete', 'min_file_age', 'log_level', 'max_log_mb',
    'lidarr_url', 'lidarr_api_key', 'lidarr_search', 'lidarr_blocklist',
    'webui', 'webui_port',
})

_config_write_lock = threading.Lock()


def _strip_inline_comment(value):
    """Strip inline comments (space+hash) from unquoted values."""
    if ' #' in value and not (
            len(value) >= 2 and value[0] == value[-1]
            and value[0] in ('"', "'")):
        value = value[:value.index(' #')].rstrip()
    return value


def _read_config_file(config_dir):
    """Parse beatscheck.conf into an ordered list of {key, value}."""
    path = os.path.join(config_dir, "beatscheck.conf")
    entries = []
    if not os.path.isfile(path):
        return entries
    with open(path, 'r') as f:
        for line in f:
            raw = line.rstrip('\n')
            stripped = raw.strip()
            if not stripped or stripped.startswith('#'):
                continue
            if '=' not in stripped:
                continue
            key, _, value = stripped.partition('=')
            key = key.strip().lower()
            value = value.strip()
            value = _strip_inline_comment(value)
            if len(value) >= 2 and value[0] == value[-1] \
                    and value[0] in ('"', "'"):
                value = value[1:-1]
            entries.append({"key": key, "value": value})
    return entries


def _write_config_file(config_dir, updates):
    """Update beatscheck.conf preserving comments and structure.
    Uses atomic write (tmp + rename) to prevent corruption.
    Thread-safe via _config_write_lock."""
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

            if active_match:
                key = active_match.group(1).lower()
                if key in new_vals:
                    val = new_vals[key]
                    if not re.match(
                            r'^(\d+\.?\d*|true|false)$',
                            str(val)):
                        val = f'"{val}"'
                    result.append(f"{key} = {val}\n")
                    written_keys.add(key)
                    continue
            elif commented_match:
                key = commented_match.group(1).lower()
                if key in new_vals:
                    val = new_vals[key]
                    if not re.match(
                            r'^(\d+\.?\d*|true|false)$',
                            str(val)):
                        val = f'"{val}"'
                    result.append(f"{key} = {val}\n")
                    written_keys.add(key)
                    continue

            result.append(line)

        for key, val in new_vals.items():
            if key not in written_keys:
                if not re.match(
                        r'^(\d+\.?\d*|true|false)$', str(val)):
                    val = f'"{val}"'
                result.append(f"{key} = {val}\n")

        tmp_path = path + ".tmp"
        with open(tmp_path, 'w') as f:
            f.writelines(result)
        os.rename(tmp_path, path)
        return True


def _read_corrupt_list(config_dir):
    """Read corrupt.txt and corrupt_details.json, merge into list."""
    corrupt_path = os.path.join(config_dir, "corrupt.txt")
    details_path = os.path.join(config_dir, "corrupt_details.json")

    paths = []
    if os.path.isfile(corrupt_path):
        with open(corrupt_path, 'r', encoding='utf-8') as f:
            paths = [line.strip() for line in f if line.strip()]

    details = {}
    if os.path.isfile(details_path):
        try:
            with open(details_path, 'r', encoding='utf-8') as f:
                details = json.load(f)
        except (json.JSONDecodeError, ValueError):
            pass

    result = []
    for p in paths:
        info = details.get(p, {})
        if isinstance(info, str):
            info = {"reason": info}
        entry = {"path": p, "reason": info.get("reason", "")}
        try:
            entry["size"] = os.path.getsize(p)
        except OSError:
            entry["size"] = 0
            entry["missing"] = True
        result.append(entry)
    return result


def _read_log_tail(config_dir, lines=200):
    """Read last N lines of beats_check.log."""
    log_path = os.path.join(config_dir, "beats_check.log")
    if not os.path.isfile(log_path):
        return ""
    try:
        with open(log_path, 'rb') as f:
            try:
                f.seek(0, 2)
                size = f.tell()
                chunk = min(size, 256 * 1024)
                f.seek(max(0, size - chunk))
                data = f.read().decode('utf-8', errors='replace')
            except OSError:
                return ""
        all_lines = data.splitlines()
        return '\n'.join(all_lines[-lines:])
    except OSError:
        return ""


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


def _trigger_rescan(config_dir, mode="report", fresh=False):
    """Trigger a rescan by writing the .rescan file."""
    rescan_path = os.path.join(config_dir, ".rescan")
    try:
        with open(rescan_path, 'w') as f:
            f.write(mode)
        if fresh:
            processed = os.path.join(config_dir, "processed.txt")
            if os.path.exists(processed):
                try:
                    from beats_check import _wait_for_scan_lock
                    _wait_for_scan_lock(config_dir)
                except ImportError:
                    pass
                try:
                    os.remove(processed)
                except OSError:
                    pass
        return True
    except OSError:
        return False


# ---------------------------------------------------------------------------
# HTTP Request Handler
# ---------------------------------------------------------------------------

_MAX_BODY = 1_048_576  # 1 MB

# Paths that don't require authentication
_AUTH_EXEMPT_PATHS = frozenset({
    '/api/auth-status', '/api/login', '/api/setup',
})


class WebUIHandler(SimpleHTTPRequestHandler):
    """Handles both static files and /api/* JSON endpoints."""

    def log_message(self, format, *args):
        pass

    def _json_response(self, data, status=200, cookies=None):
        body = json.dumps(data).encode('utf-8')
        self.send_response(status)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Content-Length', len(body))
        self.send_header('Cache-Control', 'no-cache')
        if cookies:
            for cookie in cookies:
                self.send_header('Set-Cookie', cookie)
        self.end_headers()
        self.wfile.write(body)

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
            return json.loads(self.rfile.read(length))
        except (json.JSONDecodeError, ValueError):
            self._json_response({"error": "invalid JSON"}, 400)
            return None

    def _get_session_token(self):
        """Extract session token from cookies."""
        cookie_header = self.headers.get('Cookie', '')
        for part in cookie_header.split(';'):
            part = part.strip()
            if part.startswith('beatscheck_session='):
                return part[len('beatscheck_session='):]
        return None

    def _check_auth(self):
        """Check if request is authenticated. Returns True or
        sends 401 and returns False."""
        _cleanup_sessions()
        token = self._get_session_token()
        if _validate_session(token):
            return True
        self._json_response({"error": "unauthorized"}, 401)
        return False

    def _session_cookie(self, token, max_age=_SESSION_MAX_AGE):
        """Build a Set-Cookie header value."""
        return (f"beatscheck_session={token}; "
                f"HttpOnly; SameSite=Strict; Path=/; "
                f"Max-Age={max_age}")

    def do_GET(self):
        config_dir = self.server.config_dir

        # Auth-exempt: status check and static assets
        if self.path == '/api/auth-status':
            auth = _load_auth(config_dir)
            token = self._get_session_token()
            self._json_response({
                "setup_required": auth is None,
                "authenticated": _validate_session(token),
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
            state["summary"] = summary
            self._json_response(state)

        elif self.path == '/api/config':
            entries = _read_config_file(config_dir)
            for entry in entries:
                if entry["key"] == "lidarr_api_key" \
                        and entry["value"]:
                    entry["value"] = _API_KEY_MASK
            self._json_response({"config": entries})

        elif self.path == '/api/corrupt':
            files = _read_corrupt_list(config_dir)
            self._json_response(
                {"files": files, "count": len(files)})

        elif self.path == '/api/log' \
                or self.path.startswith('/api/log?'):
            lines = 200
            if '?' in self.path:
                try:
                    params = dict(
                        p.split('=', 1) for p in
                        self.path.split('?', 1)[1].split('&')
                        if '=' in p)
                    lines = max(1, min(
                        int(params.get('lines', 200)), 10000))
                except (ValueError, TypeError):
                    lines = 200
            text = _read_log_tail(config_dir, lines)
            self._json_response({"log": text})

        elif self.path == '/api/stats':
            summary = _read_summary(config_dir)
            state = app_state.snapshot()
            self._json_response({
                "summary": summary,
                "status": state["status"],
                "mode": state["mode"],
                "uptime": state["uptime"],
                "version": state["version"],
            })

        else:
            self._json_response({"error": "not found"}, 404)

    def _handle_setup(self, config_dir):
        """Handle POST /api/setup — first-run credential creation."""
        if _load_auth(config_dir) is not None:
            self._json_response(
                {"error": "already configured"}, 400)
            return
        body = self._read_body()
        if body is None:
            return
        username = (body.get('username') or '').strip()
        password = body.get('password') or ''
        if not username:
            self._json_response(
                {"error": "username required"}, 400)
            return
        if len(password) < 4:
            self._json_response(
                {"error": "password must be at least "
                 "4 characters"}, 400)
            return
        _save_auth(config_dir, username, password)
        token = _create_session(username)
        self._json_response(
            {"ok": True},
            cookies=[self._session_cookie(token)])

    def _handle_login(self, config_dir):
        """Handle POST /api/login — credential validation."""
        body = self._read_body()
        if body is None:
            return
        auth = _load_auth(config_dir)
        if auth is None:
            self._json_response(
                {"error": "setup required"}, 400)
            return
        username = (body.get('username') or '').strip()
        password = body.get('password') or ''
        if (username == auth['username']
                and _verify_password(
                    password, auth['password_hash'])):
            token = _create_session(username)
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

    def do_POST(self):
        config_dir = self.server.config_dir

        # Auth endpoints (no session required)
        if self.path == '/api/setup':
            self._handle_setup(config_dir)
            return
        if self.path == '/api/login':
            self._handle_login(config_dir)
            return
        if self.path == '/api/logout':
            self._handle_logout()
            return

        # Protected endpoints
        if not self._check_auth():
            return

        if self.path == '/api/config':
            body = self._read_body()
            if body is None:
                return
            updates = body.get('config', {})
            if not updates:
                self._json_response(
                    {"error": "no config provided"}, 400)
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
            body = self._read_body()
            if body is None:
                return
            mode = body.get('mode', 'report')
            fresh = body.get('fresh', False)
            if mode not in ('report', 'move'):
                self._json_response(
                    {"error": "mode must be report or move"},
                    400)
                return
            ok = _trigger_rescan(config_dir, mode, fresh)
            self._json_response({"ok": ok})

        elif self.path == '/api/cancel':
            try:
                from beats_check import cancel_scan
                cancel_scan()
                self._json_response({"ok": True})
            except ImportError:
                self._json_response(
                    {"error": "cancel not available"}, 500)

        elif self.path == '/api/delete':
            body = self._read_body()
            if body is None:
                return
            files = body.get('files', [])
            if not files:
                self._json_response(
                    {"error": "no files specified"}, 400)
                return
            try:
                from beats_check import delete_corrupt_files
            except ImportError:
                self._json_response(
                    {"error": "delete not available"}, 500)
                return
            music_dir = os.environ.get("MUSIC_DIR", "/music")
            result = delete_corrupt_files(
                files, config_dir, music_dir=music_dir)
            self._json_response(result)

        else:
            self._json_response({"error": "not found"}, 404)

    def _serve_static(self, filename):
        """Serve a file from the static directory."""
        static_dir = self.server.static_dir
        filepath = os.path.join(static_dir, filename)
        real = os.path.realpath(filepath)
        if not real.startswith(os.path.realpath(static_dir)):
            self.send_error(403)
            return
        if not os.path.isfile(real):
            self.send_error(404)
            return

        content_types = {
            '.html': 'text/html; charset=utf-8',
            '.css': 'text/css; charset=utf-8',
            '.js': 'application/javascript; charset=utf-8',
            '.json': 'application/json',
            '.png': 'image/png',
            '.svg': 'image/svg+xml',
            '.ico': 'image/x-icon',
        }
        ext = os.path.splitext(filename)[1].lower()
        ct = content_types.get(ext, 'application/octet-stream')

        with open(real, 'rb') as f:
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
