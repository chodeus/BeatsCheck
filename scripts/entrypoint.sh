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

# Creating a file is the only reliable test: `test -w` passes on a directory the
# process cannot use — mode 600 satisfies it while the missing search bit blocks
# everything below — and it never sees a read-only mount. RUN_AS is empty when we
# are already the target uid, so it must stay unquoted.
require_writable_config() {
    probe="/config/.beatscheck-write-probe.$$"
    if ! ${RUN_AS} touch "${probe}" 2>/dev/null; then
        echo "FATAL: /config is not writable by ${PUID}:${PGID}."
        echo "Pre-chown it on the host: sudo chown -R ${PUID}:${PGID} /path/to/config"
        exit 1
    fi
    ${RUN_AS} rm -f "${probe}"
}

# Detect rootless mode (`docker run --user uid:gid`). PUID/PGID env vars
# are ignored — we can't usermod/chown without root, and the supplied uid
# is already what the operator wants.
if [ "$(id -u)" != "0" ]; then
    PUID=$(id -u)
    PGID=$(id -g)
    USER_NAME=$(id -un 2>/dev/null || echo "uid-${PUID}")

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
    # -H: never create or chmod the home dir. -h /app made adduser chown /app;
    # -h /config made it chmod the operator's config dir to 2755.
    adduser -D -H -h /config -u "${PUID}" -G "${GROUP_NAME}" -s /sbin/nologin checker
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
# Chown only what is wrong; an already-correct tree costs a stat pass.
# Walks through NOFOLLOW directory fds: a path-based chown follows an
# intermediate directory swapped for a symlink mid-sweep.
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
