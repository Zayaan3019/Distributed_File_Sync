#!/usr/bin/env bash
# Definition-of-Done #1: mount works under WSL2 and ls/cat/vim/cp/git behave
# correctly inside it.
#
# Deliberately uses the real tools rather than Python's os module. The point
# is that ordinary userland programs work, and those programs issue syscall
# patterns (readdir plus stat per entry, rename-based atomic save, O_APPEND,
# fsync) that a hand-written test would not think to reproduce.
set -uo pipefail

ROOT="${1:-$HOME/dfs_smoke}"
BACK="$ROOT/backing"
MNT="$ROOT/mnt"
DB="$ROOT/catalog.db"
PY="${PY:-$HOME/dfsvenv/bin/python}"
PROJ="${PROJ:-$HOME/dfs}"

pass=0; fail=0
check() {  # check <label> <expected> <actual>
  if [ "$2" == "$3" ]; then
    echo "  PASS  $1"
    pass=$((pass+1))
  else
    echo "  FAIL  $1"
    echo "          expected: $2"
    echo "          actual:   $3"
    fail=$((fail+1))
  fi
}

cleanup() {
  fusermount3 -uz "$MNT" 2>/dev/null || true
  [ -n "${FPID:-}" ] && kill "$FPID" 2>/dev/null
  wait "${FPID:-}" 2>/dev/null
}
trap cleanup EXIT

fusermount3 -uz "$MNT" 2>/dev/null || true
rm -rf "$ROOT"; mkdir -p "$BACK" "$MNT"

cd "$PROJ"
"$PY" -c "
import sys; sys.path.insert(0, '$PROJ')
from app.fuse.mount import mount_blocking
mount_blocking('$MNT', '$BACK', '$DB')
" &
FPID=$!

for _ in $(seq 1 50); do
  mountpoint -q "$MNT" && break
  sleep 0.2
done
mountpoint -q "$MNT" || { echo "MOUNT FAILED"; exit 1; }

echo "=== mount ==="
mount | grep "$MNT" | head -1
echo

echo "=== ls / cat / basic writes ==="
echo "hello world" > "$MNT/a.txt"
check "cat after write"        "hello world"  "$(cat "$MNT/a.txt")"
check "ls shows the file"      "a.txt"        "$(ls "$MNT")"
check "size via stat"          "12"           "$(stat -c %s "$MNT/a.txt")"
check "wc -c"                  "12"           "$(wc -c < "$MNT/a.txt")"

echo
echo "=== mkdir / nested / rmdir ==="
mkdir -p "$MNT/d/e"
echo nested > "$MNT/d/e/f.txt"
check "nested read"            "nested"       "$(cat "$MNT/d/e/f.txt")"
check "find depth"             "$MNT/d/e/f.txt" "$(find "$MNT" -name f.txt)"
rmdir "$MNT/d/e" 2>/dev/null
check "rmdir non-empty refused" "1"           "$([ -d "$MNT/d/e" ] && echo 1 || echo 0)"

echo
echo "=== cp (large, exercises sequential read+write) ==="
dd if=/dev/urandom of="$ROOT/src.bin" bs=1M count=8 status=none
cp "$ROOT/src.bin" "$MNT/big.bin"
check "cp round-trip checksum" \
  "$(md5sum < "$ROOT/src.bin" | cut -d' ' -f1)" \
  "$(md5sum < "$MNT/big.bin" | cut -d' ' -f1)"
cp -a "$MNT/big.bin" "$ROOT/back.bin"
check "cp -a out checksum" \
  "$(md5sum < "$ROOT/src.bin" | cut -d' ' -f1)" \
  "$(md5sum < "$ROOT/back.bin" | cut -d' ' -f1)"

echo
echo "=== O_APPEND / truncate / sparse ==="
printf 'one\n' >  "$MNT/app.txt"
printf 'two\n' >> "$MNT/app.txt"
printf 'three\n' >> "$MNT/app.txt"
check "append order" "one two three" "$(tr '\n' ' ' < "$MNT/app.txt" | sed 's/ $//')"
truncate -s 4 "$MNT/app.txt"
check "truncate size" "4" "$(stat -c %s "$MNT/app.txt")"
truncate -s 1048576 "$MNT/sparse.bin"
check "sparse size" "1048576" "$(stat -c %s "$MNT/sparse.bin")"
check "sparse reads zero" "1048576" "$(tr -d '\0' < "$MNT/sparse.bin" | wc -c | awk '{print 1048576-$1}')"

echo
echo "=== rename (inode must be preserved) ==="
echo renameme > "$MNT/r1.txt"
INO_BEFORE=$(stat -c %i "$MNT/r1.txt")
mv "$MNT/r1.txt" "$MNT/r2.txt"
INO_AFTER=$(stat -c %i "$MNT/r2.txt")
check "rename preserves inode" "$INO_BEFORE" "$INO_AFTER"
check "rename content"         "renameme"    "$(cat "$MNT/r2.txt")"
check "old name gone"          "0"           "$([ -e "$MNT/r1.txt" ] && echo 1 || echo 0)"

echo
echo "=== vim (rename-based atomic save) ==="
if command -v vim >/dev/null; then
  printf 'line1\nline2\n' > "$MNT/v.txt"
  vim -e -s -c '%s/line2/CHANGED/' -c 'wq' "$MNT/v.txt" </dev/null >/dev/null 2>&1
  check "vim edit persisted" "line1 CHANGED" "$(tr '\n' ' ' < "$MNT/v.txt" | sed 's/ $//')"
else
  echo "  SKIP  vim not installed"
fi

echo
echo "=== git status inside the mount ==="
if command -v git >/dev/null; then
  mkdir -p "$MNT/repo"
  (cd "$MNT/repo" && git init -q . && git config user.email t@t && git config user.name t \
    && echo tracked > tracked.txt && git add -A && git commit -qm init) >/dev/null 2>&1
  echo modified >> "$MNT/repo/tracked.txt"
  echo untracked > "$MNT/repo/new.txt"
  OUT=$(cd "$MNT/repo" && git status --porcelain 2>&1 | sort | tr '\n' ';')
  check "git status" " M tracked.txt;?? new.txt;" "$OUT"
  check "git fsck clean" "0" "$(cd "$MNT/repo" && git fsck --no-progress >/dev/null 2>&1; echo $?)"
else
  echo "  SKIP  git not installed"
fi

echo
echo "=== unlink ==="
rm "$MNT/a.txt"
check "unlink removes entry" "0" "$([ -e "$MNT/a.txt" ] && echo 1 || echo 0)"
check "backing store agrees" "0" "$([ -e "$BACK/a.txt" ] && echo 1 || echo 0)"

echo
echo "=== statfs (df must not lie) ==="
DF_MNT=$(df -k --output=size "$MNT" | tail -1 | tr -d ' ')
DF_BACK=$(df -k --output=size "$BACK" | tail -1 | tr -d ' ')
check "df size matches backing" "$DF_BACK" "$DF_MNT"

echo
echo "=== stable inodes across remount ==="
echo persist > "$MNT/stable.txt"
INO1=$(stat -c %i "$MNT/stable.txt")
fusermount3 -u "$MNT"; wait "$FPID" 2>/dev/null; FPID=""
"$PY" -c "
import sys; sys.path.insert(0, '$PROJ')
from app.fuse.mount import mount_blocking
mount_blocking('$MNT', '$BACK', '$DB')
" &
FPID=$!
for _ in $(seq 1 50); do mountpoint -q "$MNT" && break; sleep 0.2; done
INO2=$(stat -c %i "$MNT/stable.txt")
check "inode stable across remount" "$INO1" "$INO2"

echo
echo "================================================"
echo "  PASSED: $pass    FAILED: $fail"
echo "================================================"
[ "$fail" -eq 0 ]
