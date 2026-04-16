#!/bin/sh
# Trigger a rescan from inside or outside the container.
# Usage: rescan [--fresh] [--mode report|move]
#        rescan [--fresh] [report|move]
CONFIG_DIR=${CONFIG_DIR:-/config}

MODE_OVERRIDE=""
FRESH=false

while [ $# -gt 0 ]; do
    case "$1" in
        --fresh) FRESH=true; shift ;;
        --mode)
            if [ -n "$2" ]; then
                MODE_OVERRIDE="$2"
                shift 2
            else
                echo "Error: --mode requires a value (report or move)"
                exit 1
            fi
            ;;
        report|move) MODE_OVERRIDE="$1"; shift ;;
        *) echo "Unknown option: $1"; exit 1 ;;
    esac
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
