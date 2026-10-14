# Web UI

BeatsCheck includes an optional web interface for monitoring and control. Disabled by default.

## Enabling the WebUI

1. Edit `/config/beatscheck.conf`:
   ```conf
   webui = true
   webui_port = 8484
   ```

2. Publish the port in your Docker setup:
   ```yaml
   # compose.yaml
   ports:
     - "8484:8484"
   ```

3. Access at `http://your-server:8484`
4. On first visit, the setup wizard asks whether to require a login. Pick **Require a login** and create your credentials, or **No login** for an open WebUI. You can change this later in the **Access** card on the Configuration page

## Features

- **Authentication** — first-run setup wizard, session-based login with PBKDF2-hashed passwords; the login can be turned off and on from Configuration
- **Dashboard** — live scan status, progress bar with ETA, library stats
- **Corrupt Files** — sortable/searchable table with individual and bulk delete
- **Configuration** — edit all settings from the browser (config key allowlist enforced)
- **Logs** — real-time log viewer with syntax highlighting, level filter, search, copy/download
- **Dark/Light mode** — toggle with one click, preference saved
- **Mobile responsive** — full functionality on phones and tablets
- **Accessible** — keyboard navigation, ARIA labels, skip-to-content, reduced motion support

## Resetting credentials

If you forget your WebUI password:

```bash
docker exec beatscheck reset-webui-password
```

This removes the credential file, and existing sessions stop working. The next visit to the WebUI will show the setup wizard to create new credentials.

## Turning the login off

On the Configuration page, **Access → Turn off login** asks for your current password, then removes the stored credentials and ends every session. While the login is off, a **Login off** pill shows in the header and anyone who can reach the WebUI can use it, including deleting files. **Turn on login** in the same card creates new credentials.

## Security

- **Authentication required** — every API endpoint except setup, login, logout and the login status needs a valid session (PBKDF2-SHA256 hashed passwords, HttpOnly session cookies), unless you chose **No login** or turned the login off. A damaged `webui_auth.json` keeps the WebUI locked, never open
- **Login lockout** — 5 attempts per client within 15 minutes, then a 5-minute lockout. Behind a reverse proxy, set [`WEBUI_TRUSTED_PROXIES`](configuration.md#container-environment-variables) so each client gets its own lockout
- **Setup wizard** — on first access, create credentials (stored hashed in `/config/webui_auth.json`) or choose **No login**
- **Cross-site POSTs refused** — the API accepts only JSON POSTs, which a browser sends from another site only after a CORS check the WebUI never passes. This holds with the login off too
- **Config allowlist** — only known configuration keys are accepted (arbitrary key injection blocked)
- **Thread-safe config writes** — concurrent requests cannot corrupt the config file
- **Delete validation** — a file delete accepts only files in `corrupt.txt`; an album delete needs at least one of them in that folder, then removes the whole folder. Both must be inside the music directory; symlinks rejected
- **Path traversal protection** — static file serving validates all paths against the static directory
- **API key masking** — Lidarr API key shown as `********` in the UI
- **No third-party Python packages** — the WebUI uses only the standard library

**Important:** The WebUI is designed for trusted LAN / Docker bridge networks. For remote access, use a reverse proxy with HTTPS and authentication (Nginx, Caddy, Traefik). Do not expose the WebUI port directly to the internet.
