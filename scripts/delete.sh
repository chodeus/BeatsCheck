#!/bin/sh
# Convenience wrapper for interactive delete inside a running container.
# Usage: docker exec -it beatscheck delete
MUSIC_DIR=${MUSIC_DIR:-/data}

if [ ! -w "$MUSIC_DIR" ]; then
    echo "ERROR: Music directory ($MUSIC_DIR) is read-only."
    echo "       Change the mount from :ro to :rw in your container config, then restart."
    exit 1
fi

# Mirrors entrypoint.sh: su-exec cannot switch identity under `--user uid:gid`.
# RUN_AS is empty when we are already the target uid, so leave it unquoted.
RUN_AS=""
[ "$(id -u)" = "0" ] && RUN_AS="su-exec ${PUID:-99}:${PGID:-100}"

umask "${UMASK:-002}"

exec ${RUN_AS} env \
    MODE=delete \
    HOME=/config \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    python3 /app/main.py
