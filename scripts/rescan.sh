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
            case "$2" in
                report|move) MODE_OVERRIDE="$2"; shift 2 ;;
                *) echo "Error: --mode requires report or move"; exit 1 ;;
            esac
            ;;
        report|move) MODE_OVERRIDE="$1"; shift ;;
        *) echo "Unknown option: $1"; exit 1 ;;
    esac
done

# Same format the WebUI writes: an optional "fresh:", then an optional mode.
TRIGGER="$MODE_OVERRIDE"
[ "$FRESH" = true ] && TRIGGER="fresh:$MODE_OVERRIDE"

# Mirrors delete.sh: su-exec cannot switch identity under `--user uid:gid`.
# RUN_AS is empty when we are already the target uid, so leave it unquoted.
RUN_AS=""
[ "$(id -u)" = "0" ] && RUN_AS="su-exec ${PUID:-99}:${PGID:-100}"

# os.replace swaps the directory entry, so a symlink at .rescan is replaced,
# never written through.
if ! ${RUN_AS} python3 -c '
import os, sys, tempfile
config_dir, trigger = sys.argv[1], sys.argv[2]
fd, tmp = tempfile.mkstemp(prefix=".rescan.", dir=config_dir)
with os.fdopen(fd, "w") as f:
    f.write(trigger)
os.replace(tmp, os.path.join(config_dir, ".rescan"))
' "$CONFIG_DIR" "$TRIGGER"; then
    echo "Error: could not write $CONFIG_DIR/.rescan"
    exit 1
fi

[ "$FRESH" = true ] && echo "The resume cache is cleared when the scan starts."
if [ -n "$MODE_OVERRIDE" ]; then
    echo "Rescan triggered (mode: $MODE_OVERRIDE). Check container logs."
else
    echo "Rescan triggered. Check container logs for progress."
fi
