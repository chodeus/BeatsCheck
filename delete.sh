#!/bin/sh
# Convenience wrapper for interactive delete inside a running container.
# Usage: docker exec -it BeatsCheck /app/delete.sh
exec su-exec "${PUID:-99}:${PGID:-100}" env \
    MODE=delete \
    PYTHONUNBUFFERED=1 \
    python3 /app/beats_check.py
