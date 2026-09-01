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
    # Rootless cannot write /etc/localtime; TZ in the environment still applies.
    ln -sf "/usr/share/zoneinfo/$TZ" /etc/localtime 2>/dev/null || true
    export TZ
fi

# `test -w` passes on a mode-600 dir and never sees a read-only mount.
# RUN_AS is empty when we are already the target uid, so leave it unquoted.
require_writable_config() {
    probe="/config/.beatscheck-write-probe.$$"
    if ! ${RUN_AS} touch "${probe}" 2>/dev/null; then
        echo "FATAL: /config is not writable by ${PUID}:${PGID}."
        echo "Pre-chown it on the host: sudo chown -R ${PUID}:${PGID} /path/to/config"
        exit 1
    fi
    ${RUN_AS} rm -f "${probe}"
}

# Rootless (`docker run --user uid:gid`): PUID/PGID are ignored because we
# cannot usermod without root.
if [ "$(id -u)" != "0" ]; then
    PUID=$(id -u)
    PGID=$(id -g)

    # No chown is possible here, so the operator must pre-chown /config.
    RUN_AS=""
    require_writable_config

    for cmd in ffmpeg python3; do
        command -v "$cmd" >/dev/null 2>&1 || { echo "Missing required tool: $cmd"; exit 1; }
    done

    umask "${UMASK}"
    exec env HOME=/config PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1 python3 -u /app/main.py "$@"
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
    # -H so adduser never creates or chmods the home dir.
    adduser -D -H -h /config -u "${PUID}" -G "${GROUP_NAME}" -s /sbin/nologin checker
fi

# Validate dependencies
for cmd in ffmpeg python3; do
    if ! command -v "$cmd" >/dev/null 2>&1; then
        echo "Missing required tool: $cmd"
        exit 1
    fi
done

# Ensure writable dirs exist and are owned correctly
mkdir -p /config
# Chowns only what is wrong, through NOFOLLOW directory fds: a path-based
# chown follows an intermediate dir swapped for a symlink mid-sweep.
python3 /app/fix_ownership.py /config "${PUID}" "${PGID}" || true

# Fail closed on the thing that matters. A per-file chown error can be benign
# (foreign uids on a network mount); an unwritable /config is not.
RUN_AS="su-exec ${PUID}:${PGID}"
require_writable_config

umask "${UMASK}"

exec su-exec "${PUID}:${PGID}" env \
    HOME=/config \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    python3 -u /app/main.py "$@"
