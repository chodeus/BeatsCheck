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
