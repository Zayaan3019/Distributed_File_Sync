#!/usr/bin/env bash
# Run the pjdfstest POSIX conformance suite against a real FUSE mount.
#
# pjdfstest must run as root (it tests setuid/chown semantics that an
# unprivileged user cannot exercise) and it must run *inside* the mount, so
# this script mounts a local-only filesystem -- no sync engine, no network --
# and points `prove` at it.
#
# The suite is deliberately harsh. It checks corners of POSIX that a sync
# filesystem has no reason to implement (hardlinks, device nodes, chflags).
# The goal is an honest pass rate with every failure named, not a green run.
#
# Usage: pjdfstest.sh [pjdfstest-dir] [output-file]
set -uo pipefail

PJD="${1:-$HOME/pjdfstest}"
OUT="${2:-$HOME/pjdfstest_results.txt}"
ROOT="${PJD_ROOT:-$HOME/pjd_run}"
VENV="${VENV:-$HOME/dfsvenv/bin/python}"
PROJECT="${PROJECT:-$HOME/dfs}"

MNT="$ROOT/mnt"
BACKING="$ROOT/backing"
CATALOG="$ROOT/catalog.db"

if [ ! -x "$PJD/pjdfstest" ]; then
    echo "pjdfstest binary not built at $PJD/pjdfstest" >&2
    exit 1
fi

cleanup() {
    cd /
    fusermount3 -uz "$MNT" 2>/dev/null || true
    if [ -n "${MOUNT_PID:-}" ]; then
        kill "$MOUNT_PID" 2>/dev/null || true
        wait "$MOUNT_PID" 2>/dev/null || true
    fi
}
trap cleanup EXIT

fusermount3 -uz "$MNT" 2>/dev/null || true
rm -rf "$ROOT"
mkdir -p "$MNT" "$BACKING"

# allow_other is mandatory here, not a tuning knob. pjdfstest performs most of
# its checks as unprivileged uids (`-u 65534 ...`), and a FUSE mount without
# allow_other is visible only to the mounting user -- every one of those checks
# fails with EACCES before it reaches our code. A run without this option
# reports a pass rate for the mount options, not for the filesystem.
if ! grep -qs '^user_allow_other' /etc/fuse.conf; then
    echo "user_allow_other" >> /etc/fuse.conf
fi

cd "$PROJECT"
"$VENV" -c "
import sys
sys.path.insert(0, '.')
from app.fuse.mount import mount_blocking
mount_blocking('$MNT', '$BACKING', '$CATALOG', allow_other=True)
" &
MOUNT_PID=$!

# pjdfstest needs to create files under the mount as other users.
chmod 0777 "$BACKING"

# Wait for the mount to appear rather than sleeping a fixed amount.
for _ in $(seq 1 60); do
    if mountpoint -q "$MNT"; then break; fi
    sleep 0.5
done

if ! mountpoint -q "$MNT"; then
    echo "mount never came up at $MNT" >&2
    exit 1
fi

echo "mounted $MNT over $BACKING; running pjdfstest" >&2

cd "$MNT"
prove -rv "$PJD/tests" > "$OUT" 2>&1
STATUS=$?

echo "pjdfstest finished (prove exit $STATUS); results in $OUT" >&2
exit 0
