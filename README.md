# BeatsCheck

> **This project is in active development.** Features may change between releases. Please report issues on [GitHub](https://github.com/chodeus/BeatsCheck/issues).

BeatsCheck finds damaged music files. It decodes every file in your library with ffmpeg, flags the ones that fail or stop short, and explains in plain English what is wrong. You can then review them, delete them, or move them to a quarantine folder.

**It starts in setup mode: nothing is scanned, moved or deleted until you choose to.**

Supported formats: FLAC, MP3, M4A, OGG, Opus, WAV, WMA, AAC, AIFF, APE, WavPack, ALAC, M4B, M4P, MP2, MPC, DSF, DFF

## Quick start

1. Copy [`compose.yaml`](compose.yaml) into a folder, set your music and config paths, and run `docker compose up -d` in that folder. On Unraid, use the [template](docs/installation.md#unraid).
2. Start a scan: `docker exec beatscheck rescan report`
3. Read the results in `corrupt.txt` in your config folder, or turn on the [WebUI](docs/webui.md).

## Modes

| Mode | What it does | Music mount | Safe? |
|------|-------------|-------------|-------|
| `setup` | Container starts idle, no scanning (default) | `ro` | Yes |
| `report` | Scan and log only, writes `corrupt.txt` | `ro` | Yes |
| `delete` | Interactive — prompts per album folder to delete | **`rw`** | You choose |
| `move` | Auto-move corrupt files to quarantine folder | **`rw`** | Destructive |

## Commands

```bash
docker exec beatscheck rescan report          # scan now, in report mode
docker exec beatscheck rescan --fresh report  # re-check every file
docker exec -it beatscheck delete             # review and delete corrupt files
docker exec beatscheck reset-webui-password   # forgot the WebUI password
```

## Documentation

- [Installation](docs/installation.md) — Docker Compose, docker run, Unraid, without Docker, updating
- [Scanning and deleting](docs/usage.md) — scan, review, delete and rescan
- [Configuration](docs/configuration.md) — every setting, environment variables, output files
- [How it works](docs/how-it-works.md) — the decode test, cut-short files, safety features, performance
- [Web UI](docs/webui.md) — setup, login and security
- [Lidarr integration](docs/lidarr.md) — blocklist, delete and re-download through Lidarr

## Security

See [SECURITY.md](.github/SECURITY.md) for the security policy, vulnerability reporting, and container security measures.

## License

[MIT](LICENSE)
