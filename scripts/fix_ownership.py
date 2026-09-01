#!/usr/bin/env python3
"""Chown a directory tree to uid:gid without descending through symlinks."""

import os
import sys


def _chown(name: str, uid: int, gid: int, dir_fd: int) -> None:
    try:
        info = os.stat(name, dir_fd=dir_fd, follow_symlinks=False)
    except OSError:
        return  # vanished mid-walk
    if info.st_uid == uid and info.st_gid == gid:
        return
    try:
        os.chown(name, uid, gid, dir_fd=dir_fd, follow_symlinks=False)
    except OSError:
        # Benign on a mount with foreign uids; the write probe fails closed.
        pass


def main(root: str, uid: int, gid: int) -> int:
    # fwalk owns the directory fds and defaults to follow_symlinks=False, so a
    # subdirectory swapped for a symlink is never descended.
    try:
        root_fd = os.open(root, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    except OSError:
        return 1
    try:
        for _, dirnames, filenames, dir_fd in os.fwalk(
            ".", dir_fd=root_fd, follow_symlinks=False
        ):
            for name in dirnames + filenames:
                _chown(name, uid, gid, dir_fd)
    finally:
        os.close(root_fd)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1], int(sys.argv[2]), int(sys.argv[3])))
