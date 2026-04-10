"""BeatsCheck WebUI — optional web interface served via Python stdlib."""

import json
import logging
import os
import re
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
            "version": "1.0.0",
            "uptime_start": time.time(),
        }

    def get(self, key=None):
        with self._lock:
            if key:
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
# API helpers
# ---------------------------------------------------------------------------

def _read_config_file(config_dir):
    """Parse beatscheck.conf into an ordered list of {key, value, comment}."""
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
            if len(value) >= 2 and value[0] == value[-1] and value[0] in ('"', "'"):
                value = value[1:-1]
            entries.append({"key": key, "value": value})
    return entries


def _write_config_file(config_dir, updates):
    """Update beatscheck.conf preserving comments and structure."""
    path = os.path.join(config_dir, "beatscheck.conf")
    if not os.path.isfile(path):
        return False

    # Build lookup of new values
    new_vals = {k.lower(): v for k, v in updates.items()}
    written_keys = set()

    with open(path, 'r') as f:
        lines = f.readlines()

    result = []
    for line in lines:
        stripped = line.strip()
        # Check if this is a commented-out config line: "# key = value"
        commented_match = re.match(r'^#\s*(\w+)\s*=', stripped)
        # Check if this is an active config line: "key = value"
        active_match = re.match(r'^(\w+)\s*=', stripped)

        if active_match:
            key = active_match.group(1).lower()
            if key in new_vals:
                val = new_vals[key]
                # Quote strings that aren't pure numbers or booleans
                if not re.match(r'^(\d+\.?\d*|true|false)$', str(val)):
                    val = f'"{val}"'
                result.append(f"{key} = {val}\n")
                written_keys.add(key)
                continue
        elif commented_match:
            key = commented_match.group(1).lower()
            if key in new_vals:
                val = new_vals[key]
                if not re.match(r'^(\d+\.?\d*|true|false)$', str(val)):
                    val = f'"{val}"'
                result.append(f"{key} = {val}\n")
                written_keys.add(key)
                continue

        result.append(line)

    # Append any keys not already in file
    for key, val in new_vals.items():
        if key not in written_keys:
            if not re.match(r'^(\d+\.?\d*|true|false)$', str(val)):
                val = f'"{val}"'
            result.append(f"{key} = {val}\n")

    with open(path, 'w') as f:
        f.writelines(result)
    return True


def _read_corrupt_list(config_dir):
    """Read corrupt.txt and corrupt_details.json, merge into list of dicts."""
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
        # Include size if file exists
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
            # Seek from end for efficiency
            try:
                f.seek(0, 2)
                size = f.tell()
                # Read last 256KB max
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
                os.remove(processed)
        return True
    except OSError:
        return False


# ---------------------------------------------------------------------------
# HTTP Request Handler
# ---------------------------------------------------------------------------

class WebUIHandler(SimpleHTTPRequestHandler):
    """Handles both static files and /api/* JSON endpoints."""

    # Suppress per-request log spam
    def log_message(self, format, *args):
        pass

    def _json_response(self, data, status=200):
        body = json.dumps(data).encode('utf-8')
        self.send_response(status)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Content-Length', len(body))
        self.send_header('Cache-Control', 'no-cache')
        self.end_headers()
        self.wfile.write(body)

    def _read_body(self):
        length = int(self.headers.get('Content-Length', 0))
        if length == 0:
            return {}
        try:
            return json.loads(self.rfile.read(length))
        except (json.JSONDecodeError, ValueError):
            return {}

    def do_GET(self):
        config_dir = self.server.config_dir

        if self.path == '/api/status':
            state = app_state.snapshot()
            summary = _read_summary(config_dir)
            state["summary"] = summary
            self._json_response(state)

        elif self.path == '/api/config':
            entries = _read_config_file(config_dir)
            self._json_response({"config": entries})

        elif self.path == '/api/corrupt':
            files = _read_corrupt_list(config_dir)
            self._json_response({"files": files, "count": len(files)})

        elif self.path.startswith('/api/log'):
            # Parse ?lines=N
            lines = 200
            if '?' in self.path:
                params = dict(p.split('=', 1) for p in
                              self.path.split('?', 1)[1].split('&')
                              if '=' in p)
                lines = int(params.get('lines', 200))
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

        elif self.path == '/' or self.path == '/index.html':
            self._serve_static('index.html')

        elif self.path.startswith('/api/'):
            self._json_response({"error": "not found"}, 404)

        else:
            # Serve static files
            self._serve_static(self.path.lstrip('/'))

    def do_POST(self):
        config_dir = self.server.config_dir

        if self.path == '/api/config':
            body = self._read_body()
            updates = body.get('config', {})
            if not updates:
                self._json_response({"error": "no config provided"}, 400)
                return
            ok = _write_config_file(config_dir, updates)
            self._json_response({"ok": ok})

        elif self.path == '/api/rescan':
            body = self._read_body()
            mode = body.get('mode', 'report')
            fresh = body.get('fresh', False)
            if mode not in ('report', 'move'):
                self._json_response({"error": "mode must be report or move"}, 400)
                return
            ok = _trigger_rescan(config_dir, mode, fresh)
            self._json_response({"ok": ok})

        elif self.path == '/api/delete':
            body = self._read_body()
            files = body.get('files', [])
            if not files:
                self._json_response({"error": "no files specified"}, 400)
                return
            # Security: only allow deleting files listed in corrupt.txt
            corrupt_path = os.path.join(config_dir, "corrupt.txt")
            allowed = set()
            if os.path.isfile(corrupt_path):
                try:
                    with open(corrupt_path, 'r') as f:
                        allowed = {l.strip() for l in f if l.strip()}
                except OSError:
                    pass
            deleted = []
            errors = []
            for fp in files:
                if fp not in allowed:
                    errors.append({"path": fp, "error": "not in corrupt list"})
                    continue
                try:
                    real = os.path.realpath(fp)
                    if os.path.isfile(real):
                        os.remove(real)
                        deleted.append(fp)
                    else:
                        errors.append({"path": fp, "error": "not found"})
                except OSError as e:
                    errors.append({"path": fp, "error": str(e)})
            # Update corrupt.txt
            corrupt_path = os.path.join(config_dir, "corrupt.txt")
            if os.path.isfile(corrupt_path):
                try:
                    with open(corrupt_path, 'r') as f:
                        remaining = [l.strip() for l in f
                                     if l.strip() and l.strip() not in deleted]
                    with open(corrupt_path, 'w') as f:
                        for p in remaining:
                            f.write(p + '\n')
                except OSError:
                    pass
            self._json_response({
                "deleted": deleted,
                "errors": errors,
                "count": len(deleted),
            })

        else:
            self._json_response({"error": "not found"}, 404)

    def _serve_static(self, filename):
        """Serve a file from the static directory."""
        static_dir = self.server.static_dir
        filepath = os.path.join(static_dir, filename)
        # Security: prevent path traversal
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


def start_webui(config_dir, port=8080, static_dir=None):
    """Start the WebUI HTTP server in a daemon thread."""
    if static_dir is None:
        static_dir = os.path.join(os.path.dirname(__file__), 'static')

    server = ThreadedHTTPServer(
        ('0.0.0.0', port), WebUIHandler, config_dir, static_dir)

    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    logger.info("WebUI started on port %d", port)
    return server
