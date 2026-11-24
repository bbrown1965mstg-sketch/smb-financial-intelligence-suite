#!/bin/zsh
# =============================================================================
# backup_db.sh -- PostgreSQL warehouse backup (cron/launchd friendly).
#
# Creates a compressed custom-format pg_dump with a timestamped filename in
# backups/, then VERIFIES the archive parses (pg_restore --list) before
# reporting success.
#
# Connection (first match wins):
#   1. $DATABASE_URL   postgresql://user:pass@host:port/dbname
#   2. built-in default postgresql://postgres:postgres@localhost:5432/smb_warehouse
#      (matches the suite's documented default; override in env for real use)
#
# Optional env:
#   BACKUP_DIR  output directory (default: <suite root>/backups)
#   KEEP_N      retain newest N dumps, prune older (0 = keep everything)
#   PGBIN       PostgreSQL bin dir (else /Users/Owner/pg/bin, else $PATH)
#
# Usage: ./backup_db.sh [output_file]
# Exit:  0 = dump written + archive verified; 1 = failure (stderr explains)
# =============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
BACKUP_DIR="${BACKUP_DIR:-$SCRIPT_DIR/backups}"
DEFAULT_DSN="postgresql://postgres:postgres@localhost:5432/smb_warehouse"
DSN="${DATABASE_URL:-$DEFAULT_DSN}"
STAMP="$(date +%Y%m%d_%H%M%S)"
OUT="${1:-$BACKUP_DIR/smb_warehouse_backup_$STAMP.dump}"

# ---- locate pg tools ---------------------------------------------------------
if [[ -n "${PGBIN:-}" && -x "$PGBIN/pg_dump" ]]; then
    PG_DUMP="$PGBIN/pg_dump"; PG_RESTORE="$PGBIN/pg_restore"
elif [[ -x /Users/Owner/pg/bin/pg_dump ]]; then
    PG_DUMP=/Users/Owner/pg/bin/pg_dump; PG_RESTORE=/Users/Owner/pg/bin/pg_restore
else
    PG_DUMP="$(command -v pg_dump || true)"
    PG_RESTORE="$(command -v pg_restore || true)"
fi
[[ -n "$PG_DUMP" && -n "$PG_RESTORE" ]] || {
    echo "backup_db.sh: pg_dump/pg_restore not found -- set PGBIN to your PostgreSQL bin dir" >&2
    exit 1
}

mkdir -p "$BACKUP_DIR"

# ---- dump --------------------------------------------------------------------
echo "[backup_db] dumping -> $OUT"
"$PG_DUMP" --format=custom --compress=6 --dbname="$DSN" --file="$OUT"

# ---- verify the archive is parseable -----------------------------------------
ERRLOG="$(mktemp)"
if ! ENTRIES="$("$PG_RESTORE" --list "$OUT" 2>"$ERRLOG" | wc -l | tr -d ' ')"; then
    echo "[backup_db] ERROR: archive unreadable: $(head -2 "$ERRLOG")" >&2
    rm -f "$ERRLOG"; exit 1
fi
rm -f "$ERRLOG"
if [[ "$ENTRIES" -lt 10 ]]; then
    echo "[backup_db] ERROR: archive suspiciously small ($ENTRIES entries) -- inspect $OUT" >&2
    exit 1
fi

echo "[backup_db] OK  $OUT  ($ENTRIES archive entries, $(du -h "$OUT" | cut -f1))"
echo "$OUT" > "$BACKUP_DIR/last_backup_path.txt"

# ---- retention ---------------------------------------------------------------
if [[ "${KEEP_N:-0}" -gt 0 ]]; then
    ls -t "$BACKUP_DIR"/smb_warehouse_backup_*.dump 2>/dev/null | tail -n +$((KEEP_N + 1)) | while read -r old; do
        echo "[backup_db] pruning $(basename "$old")"
        rm -f "$old"
    done
fi
