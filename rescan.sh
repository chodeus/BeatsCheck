#!/bin/sh
# Trigger a rescan from inside or outside the container.
# Usage: docker exec beatscheck /app/rescan.sh [--fresh]
CONFIG_DIR=${CONFIG_DIR:-/config}

if [ "$1" = "--fresh" ]; then
    rm -f "$CONFIG_DIR/processed.txt"
    echo "Cleared resume cache. Full rescan will run."
fi

touch "$CONFIG_DIR/.rescan"
echo "Rescan triggered. Check container logs for progress."
