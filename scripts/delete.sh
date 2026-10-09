#!/bin/sh
# Convenience wrapper for interactive delete inside a running container.
# Usage: docker exec -it beatscheck delete
# Delete mode itself refuses an unwritable music dir: only it knows the one
# beatscheck.conf configures.

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
