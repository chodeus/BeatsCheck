#!/bin/sh
# Trigger a rescan from inside or outside the container.
# Usage: rescan [--fresh] [--mode report|move]
CONFIG_DIR=${CONFIG_DIR:-/config}

MODE_OVERRIDE=""
FRESH=false

for arg in "$@"; do
    case "$arg" in
        --fresh) FRESH=true ;;
        --mode) shift; MODE_OVERRIDE="$1" ;;
        report|move) MODE_OVERRIDE="$arg" ;;
    esac
    shift 2>/dev/null || true
done

if [ "$FRESH" = true ]; then
    rm -f "$CONFIG_DIR/processed.txt"
    echo "Cleared resume cache. Full rescan will run."
fi

if [ -n "$MODE_OVERRIDE" ]; then
    echo "$MODE_OVERRIDE" > "$CONFIG_DIR/.rescan"
    echo "Rescan triggered (mode: $MODE_OVERRIDE). Check container logs."
else
    touch "$CONFIG_DIR/.rescan"
    echo "Rescan triggered. Check container logs for progress."
fi
