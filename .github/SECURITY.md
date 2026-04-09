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
- No network access required (`--network none` supported)
- Tini init process for proper signal handling
- HEALTHCHECK for container orchestrator monitoring

### CI/CD Security
- Trivy vulnerability scanning on every image build
- SARIF results uploaded to GitHub Security tab
- Dependabot monitoring for GitHub Actions and Docker base image updates
- CodeQL static analysis for Python code
- All GitHub Actions pinned to specific commit SHAs (not floating tags)
- Multi-arch builds with SBOM and build attestation

### Application Security
- No external network calls — only local filesystem access
- ffmpeg arguments are list-based (no shell injection)
- Symlink boundary checking prevents directory traversal
- No secrets, tokens, or credentials handled by the application
- All file operations use safe patterns (atomic JSON writes)

## Best Practices for Users

- Keep your container image updated (`docker pull ghcr.io/chodeus/beatscheck:latest`)
- Use read-only music mounts when running in report mode
- Set appropriate PUID/PGID for your environment
- Restrict the `/config` directory permissions to the container user
