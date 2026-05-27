#!/bin/sh
set -e

PUID=${PUID:-99}
PGID=${PGID:-100}
UMASK=${UMASK:-002}
TZ=${TZ:-UTC}

# Timezone: if /etc/localtime is bind-mounted from host, use it as-is.
# Otherwise set it from the TZ env var.
if [ -f "/etc/localtime" ] && [ ! -L "/etc/localtime" ]; then
    : # bind-mounted regular file from host
elif [ -n "$TZ" ] && [ -f "/usr/share/zoneinfo/$TZ" ]; then
    ln -sf "/usr/share/zoneinfo/$TZ" /etc/localtime
    export TZ
fi

# Detect rootless mode (`docker run --user uid:gid`). PUID/PGID env vars
# are ignored — we can't usermod/chown without root, and the supplied uid
# is already what the operator wants.
if [ "$(id -u)" != "0" ]; then
    PUID=$(id -u)
    PGID=$(id -g)
    USER_NAME=$(id -un 2>/dev/null || echo "uid-${PUID}")

    # Operator must pre-chown /config on the host. Fail fast with a clear
    # message if they haven't.
    if [ ! -w /config ]; then
        echo "Rootless mode but /config is not writable by uid:gid ${PUID}:${PGID}"
        echo "Pre-chown the host config dir: sudo chown -R ${PUID}:${PGID} /path/to/config"
        exit 1
    fi

    for cmd in ffmpeg python3; do
        command -v "$cmd" >/dev/null 2>&1 || { echo "Missing required tool: $cmd"; exit 1; }
    done

    umask "${UMASK}"
    exec env HOME=/app PYTHONUNBUFFERED=1 python3 -u /app/main.py "$@"
fi

# Validate PUID/PGID are numeric
case "$PUID" in
    ''|*[!0-9]*) echo "Warning: PUID must be numeric, got '$PUID'. Using 99."; PUID=99 ;;
esac
case "$PGID" in
    ''|*[!0-9]*) echo "Warning: PGID must be numeric, got '$PGID'. Using 100."; PGID=100 ;;
esac

# Create group if it doesn't exist
if ! getent group "${PGID}" > /dev/null 2>&1; then
    addgroup -g "${PGID}" checker
fi
GROUP_NAME=$(getent group "${PGID}" | cut -d: -f1)

# Create user if it doesn't exist
if ! getent passwd "${PUID}" > /dev/null 2>&1; then
    adduser -D -u "${PUID}" -G "${GROUP_NAME}" -h /app -s /sbin/nologin checker
fi
USER_NAME=$(getent passwd "${PUID}" | cut -d: -f1)

# Validate dependencies
for cmd in ffmpeg python3; do
    if ! command -v "$cmd" >/dev/null 2>&1; then
        echo "Missing required tool: $cmd"
        exit 1
    fi
done

# Ensure writable dirs exist and are owned correctly
mkdir -p /config
chown -R "${PUID}:${PGID}" /config

umask "${UMASK}"

exec su-exec "${PUID}:${PGID}" env \
    HOME=/app \
    PYTHONUNBUFFERED=1 \
    python3 -u /app/main.py "$@"
