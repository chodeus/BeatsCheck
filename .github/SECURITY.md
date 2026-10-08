# Security Policy

## Supported Versions

| Version | Supported |
|---------|-----------|
| latest  | Yes       |
| < latest | No       |

## Reporting a Vulnerability

If you discover a security vulnerability, please report it responsibly:

1. **GitHub Security Advisory** (preferred): [Create a private advisory](https://github.com/chodeus/BeatsCheck/security/advisories/new)
2. **Email**: Open a GitHub issue marked `[SECURITY]` if it is not sensitive

Please do **not** open a public issue for security vulnerabilities that could be exploited.

## Security Measures

### Container Security
- Runs as a non-root user via `su-exec` (configurable PUID/PGID)
- Minimal Alpine base image with only required packages
- Music directory mounted read-only by default (`:ro`)
- Core scanning needs no network access (`--network none` is supported
  when the optional Lidarr integration is not configured)
- Tini init process for proper signal handling
- HEALTHCHECK for container orchestrator monitoring
- Supports two hardened deployment modes (both documented in `compose.yaml`):
  - **Capability-dropped PUID/PGID:** `cap_drop: ALL` + `cap_add` of the five
    capabilities the entrypoint actually needs (CHOWN, SETUID, SETGID, FOWNER,
    DAC_OVERRIDE) plus `no-new-privileges:true`. Removes NET_RAW, SYS_ADMIN,
    mount, and module-loading powers even during the brief root window.
  - **Fully rootless:** `user: "99:100"` instead of PUID/PGID env vars, with
    the host config dir pre-chowned to match. Container never runs as root.

### CI/CD Security
- Trivy vulnerability scanning on every image build
- SARIF results uploaded to GitHub Security tab
- Dependabot monitoring for GitHub Actions and Docker base image updates
- CodeQL static analysis for Python code
- All GitHub Actions pinned to specific commit SHAs (not floating tags)
- Multi-arch builds with SBOM and build attestation

### Application Security
- Outbound HTTP is limited to the configured Lidarr API (opt-in); the
  Lidarr API key is sent via the `X-Api-Key` header (never in URLs),
  redirects are blocked, and the key is never logged
- ffmpeg arguments are list-based (no shell injection)
- Symlink boundary checking prevents directory traversal
- Handles credentials: the Lidarr API key (stored in `beatscheck.conf`
  or a Docker secret, masked in WebUI responses) and WebUI logins
  (PBKDF2-SHA256 hashed passwords + in-memory session tokens)
- All file operations use safe patterns (atomic JSON writes)

## Best Practices for Users

- Keep your container image updated (`docker pull ghcr.io/chodeus/beatscheck:latest`)
- Use read-only music mounts when running in report mode
- Set appropriate PUID/PGID for your environment
- Restrict the `/config` directory permissions to the container user
