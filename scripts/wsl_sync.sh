#!/usr/bin/env bash
# Copy the project from the Windows checkout into the WSL home directory.
#
# Everything FUSE-related must run on ext4, never under /mnt/c. /mnt/c is a
# 9p/drvfs mount, and measuring a FUSE overlay on top of 9p would report the
# cost of 9p rather than the cost of this design. It also cannot host a FUSE
# mount point at all.
set -euo pipefail

SRC="${1:-/mnt/c/Users/Mohamed Zayaan/Downloads/Distributed_File_Sync}"
DST="${2:-$HOME/dfs}"

mkdir -p "$DST"
rm -rf "$DST/app" "$DST/tests" "$DST/benchmarks"
cp -r "$SRC/app" "$DST/"
cp -r "$SRC/tests" "$DST/"
[ -d "$SRC/benchmarks" ] && cp -r "$SRC/benchmarks" "$DST/"
cp "$SRC/pyproject.toml" "$DST/"
find "$DST" -name __pycache__ -type d -exec rm -rf {} + 2>/dev/null || true

echo "synced $SRC -> $DST"
