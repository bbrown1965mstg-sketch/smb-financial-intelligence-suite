#!/bin/zsh
# =============================================================================
# restore_db.sh -- restore a backup_db.sh dump into a target database, verified.
#
#   ./restore_db.sh <backup.dump> [--into <dbname>] [--fresh]
#
#   <backup.dump>   custom-format pg_dump archive
#   --into <db>     target database name (default: smb_warehouse_restore_test)
#   --fresh         drop the target database first if it exists. Refuses to
#                   drop the database named in the live DATABASE_URL.
#
# The admin connection (create/drop target) resolves like backup_db.sh:
# $DATABASE_URL > built-in suite default. The restore lands in the target
# database on the same host/port/user.
#
# Verification: after restore, every public table must be present (>= 15 base
# tables) and queryable; per-table row counts are printed for round-trip diff.
#
# Exit: 0 = restored + verified; 1 = failure.
# =============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
DEFAULT_DSN="postgresql://postgres:postgres@localhost:5432/smb_warehouse"
ADMIN_DSN="${DATABASE_URL:-$DEFAULT_DSN}"

DUMP=""; TARGET="smb_warehouse_restore_test"; FRESH=0
while [[ $# -gt 0 ]]; do
    case "$1" in
        --into)  TARGET="$2"; shift 2 ;;
        --fresh) FRESH=1; shift ;;
        *)       DUMP="$1"; shift ;;
    esac
done
[[ -n "$DUMP" && -f "$DUMP" ]] || {
    echo "restore_db.sh: usage: ./restore_db.sh <backup.dump> [--into <db>] [--fresh]" >&2
    exit 1
}

# ---- locate tools -------------------------------------------------------------
if [[ -n "${PGBIN:-}" && -x "$PGBIN/pg_restore" ]]; then
    PG_RESTORE="$PGBIN/pg_restore"; PSQL="$PGBIN/psql"
elif [[ -x /Users/Owner/pg/bin/pg_restore ]]; then
    PG_RESTORE=/Users/Owner/pg/bin/pg_restore; PSQL=/Users/Owner/pg/bin/psql
else
    PG_RESTORE="$(command -v pg_restore || true)"; PSQL="$(command -v psql || true)"
fi
[[ -n "$PG_RESTORE" && -n "$PSQL" ]] || {
    echo "restore_db.sh: pg_restore/psql not found -- set PGBIN" >&2; exit 1
}

# ---- derive the target DSN in pure zsh (same host/port/user, target db) -------
# Strip any ?query, keep everything up to the last "/", then append the target.
ADMIN_BASE="${ADMIN_DSN%%\?*}"          # drop ?options if present
ADMIN_BASE="${ADMIN_BASE%/*}"           # drop /dbname -> scheme://user:pass@host:port
LIVE_DB="${ADMIN_DSN%%\?*}"
LIVE_DB="${LIVE_DB##*/}"                # basename after the last "/"
TARGET_DSN="$ADMIN_BASE/$TARGET"

# ---- create (and optionally drop) the target database -------------------------
if [[ "$FRESH" -eq 1 ]]; then
    if [[ "$TARGET" == "$LIVE_DB" ]]; then
        echo "restore_db.sh: refusing --fresh drop of live database '$LIVE_DB'" >&2
        exit 1
    fi
    "$PSQL" --dbname="$ADMIN_DSN" -v ON_ERROR_STOP=1 -c "DROP DATABASE IF EXISTS \"$TARGET\";" >/dev/null
fi

EXISTS="$("$PSQL" --dbname="$ADMIN_DSN" -tAc "SELECT 1 FROM pg_database WHERE datname='$TARGET'")"
if [[ "$EXISTS" != "1" ]]; then
    "$PSQL" --dbname="$ADMIN_DSN" -v ON_ERROR_STOP=1 -c "CREATE DATABASE \"$TARGET\";" >/dev/null
fi

# ---- restore -------------------------------------------------------------------
echo "[restore_db] restoring $DUMP -> database '$TARGET'"
ERRLOG="$(mktemp)"
if ! "$PG_RESTORE" --dbname="$TARGET_DSN" --exit-on-error "$DUMP" 2>"$ERRLOG"; then
    echo "[restore_db] ERROR: pg_restore failed:" >&2
    tail -15 "$ERRLOG" >&2; rm -f "$ERRLOG"
    exit 1
fi
rm -f "$ERRLOG"

# ---- verify: every public table present and countable ---------------------------
N_TABLES="$("$PSQL" --dbname="$TARGET_DSN" -tAc \
    "SELECT count(*) FROM information_schema.tables WHERE table_schema='public' AND table_type='BASE TABLE'")"
if [[ "$N_TABLES" -lt 15 ]]; then
    echo "[restore_db] ERROR: only $N_TABLES base tables restored -- expected >= 15" >&2
    exit 1
fi

echo "[restore_db] OK  database '$TARGET' ($N_TABLES tables). Row counts:"
"$PSQL" --dbname="$TARGET_DSN" -tAc \
    "SELECT '    ' || tablename || ': ' || (xpath('/row/c/text()', query_to_xml(format('SELECT count(*) AS c FROM %I', tablename), false, true, '')))[1]::text
     FROM pg_tables WHERE schemaname='public' ORDER BY tablename"
