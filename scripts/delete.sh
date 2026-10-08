#!/bin/sh
# Convenience wrapper for interactive delete inside a running container.
# Usage: docker exec -it beatscheck delete
MUSIC_DIR=${MUSIC_DIR:-/data}

if [ ! -w "$MUSIC_DIR" ]; then
    echo "ERROR: Music directory ($MUSIC_DIR) is read-only."
    echo "       Change the mount from :ro to :rw in your container config, then restart."
    exit 1
fi

exec su-exec "${PUID:-99}:${PGID:-100}" env \
    MODE=delete \
    PYTHONUNBUFFERED=1 \
    python3 /app/main.py
